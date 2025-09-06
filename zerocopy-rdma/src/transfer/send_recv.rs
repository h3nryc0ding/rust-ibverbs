use crate::memory::jit::JustInTimeProvider;
use crate::memory::pool::PoolProvider;
use crate::memory::{Handle, Provider};
use crate::protocol::{QueryMeta, QueryRequest, QueryResponse, QueryResponseBuffer};
use crate::rdma::wr_dispatcher::WRDispatcher;
use crate::record::MockRecord;
use crate::transfer::{
    CLIENT_RECORDS, Client, Protocol, SERVER_RECORDS, SendRecvProtocol, Server, synchronize,
};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair};
use std::cmp::min;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::{io, task};
use tracing::{debug, instrument};

const CONCURRENCY: usize = 2;

#[derive(Clone)]
pub struct SendRecvClient {
    dispatcher: WRDispatcher,

    send: JustInTimeProvider<QueryRequest>,
    recv: PoolProvider<QueryResponseBuffer<CLIENT_RECORDS>>,
}

impl Client for SendRecvClient {
    async fn new(ctx: Context, stream: &mut TcpStream) -> io::Result<Self> {
        let (pd, cq, qp) = perform_rdma_handshake(&ctx, stream).await?;
        let dispatcher = WRDispatcher::new(cq, qp);

        let recv = PoolProvider::new(&pd, CONCURRENCY)?;
        let send = JustInTimeProvider::new(pd);

        synchronize(stream).await?;
        Ok(Self {
            dispatcher,
            recv,
            send,
        })
    }

    #[instrument(skip_all, name = "Client::request")]
    async fn request(&mut self, req: QueryRequest) -> io::Result<QueryResponse<CLIENT_RECORDS>> {
        let mut send = self.send.acquire_mr().await?;
        let recv = self.recv.acquire_mr().await?;

        let res = {
            *send = req;
            let recv = self.dispatcher.post_receive(&[recv.slice()]).await?;
            self.dispatcher.post_send(&[send.slice()], None).await?;
            recv
        };
        let wc = res
            .await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        let meta = QueryMeta::from(wc.imm_data);
        Ok(QueryResponse::new(recv, meta))
    }
}

pub struct SendRecvServer {
    dispatcher: WRDispatcher,

    send: Arc<MemoryRegion<QueryResponseBuffer<SERVER_RECORDS>>>,
    recv: PoolProvider<QueryRequest>,
}

impl Server for SendRecvServer {
    #[instrument(skip_all, fields(peer = %stream.peer_addr().unwrap()))]
    async fn new(ctx: Context, mut stream: TcpStream) -> io::Result<Self> {
        let (pd, cq, qp) = perform_rdma_handshake(&ctx, &mut stream).await?;
        let dispatcher = WRDispatcher::new(cq, qp);

        let send = pd.allocate()?;
        let recv = PoolProvider::new(&pd, CONCURRENCY)?;

        synchronize(&mut stream).await?;
        Ok(Self {
            dispatcher,
            recv,
            send: Arc::new(send),
        })
    }

    #[instrument(skip(self))]
    async fn serve(&mut self) -> io::Result<()> {
        loop {
            let recv = self.recv.acquire_mr().await?;

            let recv_waiter = self.dispatcher.post_receive(&[recv.slice()]).await?;

            let send = self.send.clone();
            let mut dispatcher = self.dispatcher.clone();
            tokio::spawn(async move {
                let _wc = recv_waiter.await.unwrap();
                let req = recv;
                debug!(req = ?*req, "received request");

                let start = req.offset;
                let end = min(SERVER_RECORDS, start + req.count);
                let s = size_of::<MockRecord>();
                let slice = send.slice(start * s..end * s);
                debug!(resp = ?slice, "prepared response");
                let meta = QueryMeta::new(0, (end - start) as u16);

                dispatcher
                    .post_send(&[slice], Some(u32::from(meta)))
                    .await
                    .unwrap()
                    .await
                    .unwrap();
            });
        }
    }
}

impl Protocol for SendRecvProtocol {
    type Client = SendRecvClient;
    type Server = SendRecvServer;
}

async fn perform_rdma_handshake(
    ctx: &Context,
    stream: &mut TcpStream,
) -> io::Result<(ProtectionDomain, CompletionQueue, QueuePair)> {
    let pd = ctx.alloc_pd()?;
    let cq = ctx.create_cq(4096, 0)?;
    let qp = {
        let qpb = pd.create_qp(&cq, &cq, IBV_QPT_RC)?.build()?;

        let local = qpb.endpoint()?;
        let data = serde_json::to_vec(&local)?;
        stream.write_all(&data).await?;

        let mut buf = [0u8; 1024];
        let n = stream.read(&mut buf).await?;
        let remote = serde_json::from_slice(&buf[..n])?;

        qpb.handshake(remote)?
    };
    Ok((pd, cq, qp))
}
