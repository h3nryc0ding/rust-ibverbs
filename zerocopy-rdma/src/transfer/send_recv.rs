use crate::memory::jit::JustInTimeProvider;
use crate::memory::pool::PoolProvider;
use crate::memory::{Handle, Provider};
use crate::protocol::{QueryRequest, QueryResponse};
use crate::rdma::wr_dispatcher::WRDispatcher;
use crate::record::MockRecord;
use crate::transfer::{CLIENT_RECORDS, SERVER_RECORDS , Client, Protocol, SendRecvProtocol, Server, synchronize};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair};
use std::sync::Arc;
use tokio::{io, task};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, instrument};

const CONCURRENCY: usize = 2;

#[derive(Clone)]
pub struct SendRecvClient {
    dispatcher: WRDispatcher,

    send: JustInTimeProvider<QueryRequest>,
    recv: PoolProvider<QueryResponse<CLIENT_RECORDS>>,
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
    async fn request(&mut self, req: QueryRequest) -> io::Result<Handle<QueryResponse<CLIENT_RECORDS>>> {
        let mut send = self.send.acquire_mr().await?;
        let recv = self.recv.acquire_mr().await?;

        let res = {
            *send = req;
            let recv = self.dispatcher.post_receive(&[recv.slice()]).await?;
            self.dispatcher.post_send(&[send.slice()]).await?;
            recv
        };
        res.await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(recv)
    }
}

pub struct SendRecvServer {
    dispatcher: WRDispatcher,

    send: Arc<MemoryRegion<QueryResponse<SERVER_RECORDS>>>,
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

                let s = size_of::<MockRecord>();
                let slice = send.slice(req.offset * s..(req.offset + req.count) * s);
                debug!(resp = ?slice, "prepared response");

                dispatcher.post_send(&[slice]).await.unwrap().await.unwrap();
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
