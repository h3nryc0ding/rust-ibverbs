use crate::async_cq::AsyncCompletionQueue;
use crate::memory::jit::JustInTimeProvider;
use crate::memory::pool::PoolProvider;
use crate::memory::{Handle, Provider};
use crate::protocol::{QueryRequest, QueryResponse};
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol, SendRecvProtocol, Server, synchronize};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::io;
use tracing::instrument;

const CONCURRENCY: usize = 10;

#[derive(Clone)]
pub struct SendRecvClient {
    qp: Arc<Mutex<QueuePair>>,
    cq: AsyncCompletionQueue,

    send: JustInTimeProvider<QueryRequest>,
    recv: PoolProvider<QueryResponse>,
}

impl Client for SendRecvClient {
    async fn new(ctx: Context, stream: &mut TcpStream) -> io::Result<Self> {
        let (pd, cq, qp) = perform_rdma_handshake(&ctx, stream).await?;
        let cq = AsyncCompletionQueue::new(cq);

        let recv = PoolProvider::new(&pd, CONCURRENCY)?;
        let send = JustInTimeProvider::new(pd);

        synchronize(stream).await?;
        Ok(Self {
            qp: Arc::new(Mutex::new(qp)),
            cq,
            recv,
            send,
        })
    }

    #[instrument(skip_all, name = "Client::request")]
    async fn request(&mut self, req: QueryRequest) -> io::Result<Handle<QueryResponse>> {
        let mut send = self.send.acquire_mr().await?;
        let recv = self.recv.acquire_mr().await?;

        let res = {
            *send = req;

            let recv = {
                let mut qp = self.qp.lock().await;
                self.cq.post_receive(&mut qp, &[recv.slice()]).await?
            };
            {
                let mut qp = self.qp.lock().await;
                self.cq.post_send(&mut qp, &[send.slice()]).await?;
            }
            recv
        };
        res.await
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))?;
        Ok(recv)
    }
}

pub struct SendRecvServer {
    qp: Arc<Mutex<QueuePair>>,
    cq: AsyncCompletionQueue,

    send: Arc<MemoryRegion<QueryResponse>>,
    recv: PoolProvider<QueryRequest>,
}

impl Server for SendRecvServer {
    #[instrument(skip_all, fields(peer = %stream.peer_addr().unwrap()))]
    async fn new(ctx: Context, mut stream: TcpStream) -> io::Result<Self> {
        let (pd, cq, qp) = perform_rdma_handshake(&ctx, &mut stream).await?;
        let cq = AsyncCompletionQueue::new(cq);
        let send = pd.allocate()?;
        let recv = PoolProvider::new(&pd, CONCURRENCY)?;
        synchronize(&mut stream).await?;
        Ok(Self {
            qp: Arc::new(Mutex::new(qp)),
            cq,
            recv,
            send: Arc::new(send),
        })
    }

    #[instrument(skip(self))]
    async fn serve(&mut self) -> io::Result<()> {
        loop {
            let recv = self.recv.acquire_mr().await?;

            let recv_waiter = {
                let mut qp = self.qp.lock().await;
                self.cq.post_receive(&mut qp, &[recv.slice()]).await?
            };

            let send = self.send.clone();
            let qp = self.qp.clone();
            let mut cq = self.cq.clone();
            tokio::spawn(async move {
                recv_waiter.await.unwrap();
                let req = recv;

                let slice = send.slice(
                    req.offset * size_of::<MockRecord>()
                        ..(req.offset + req.count) * size_of::<MockRecord>(),
                );
                let send_waiter = {
                    let mut qp = qp.lock().await;
                    cq.post_send(&mut qp, &[slice]).await.unwrap()
                };
                send_waiter.await.unwrap();
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
