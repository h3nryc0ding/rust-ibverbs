use crate::memory::{BufferGuard, PoolManager};
use crate::protocol::QueryRequest;
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol, RECORDS, SendRecvProtocol, Server};
use crate::utils::await_completions;
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::{io, task};

#[derive(Clone)]
pub struct SendRecvClient {
    qp: Arc<Mutex<QueuePair>>,
    cq: Arc<Mutex<CompletionQueue>>,

    pool: PoolManager<MockRecord, 2, RECORDS>,
    send: Arc<Mutex<MemoryRegion<Vec<QueryRequest>>>>,
}

impl Client for SendRecvClient {
    async fn new(ctx: Context, stream: &mut TcpStream) -> io::Result<Self> {
        let (pd, cq, qp) = perform_rdma_handshake(&ctx, stream).await?;
        let pool = PoolManager::new(&pd)?;
        let send_data = vec![QueryRequest::default(); 1];
        let send = pd.register(send_data)?;
        synchronize(stream).await?;
        Ok(Self {
            qp: Arc::new(Mutex::new(qp)),
            cq: Arc::new(Mutex::new(cq)),
            pool,
            send: Arc::new(Mutex::new(send)),
        })
    }
    async fn request(&mut self, req: QueryRequest) -> io::Result<BufferGuard<MockRecord>> {
        let buf = self.pool.acquire().await?;

        let local_recv = buf.mr().slice(&(0..RECORDS * size_of::<MockRecord>()));
        unsafe { self.qp.lock().await.post_receive(&[local_recv], 0)? }
        {
            let mut send = self.send.lock().await;
            send.inner_mut()[0] = req;
            let local_send = send.slice(&(0..1 * size_of::<QueryRequest>()));
            unsafe { self.qp.lock().await.post_send(&[local_send], 0)? }
        }

        let cq = &mut self.cq.lock().await;
        await_completions::<2>(cq).await?;

        Ok(buf)
    }
}

pub struct SendRecvServer {
    qp: QueuePair,
    cq: CompletionQueue,

    recv: MemoryRegion<Vec<QueryRequest>>,
    send: MemoryRegion<Vec<MockRecord>>,
}

impl Server for SendRecvServer {
    async fn new(ctx: Context, mut stream: TcpStream) -> io::Result<Self> {
        let (pd, cq, qp) = perform_rdma_handshake(&ctx, &mut stream).await?;
        let (recv, send) = create_server_mrs(&pd).await?;
        synchronize(&mut stream).await?;
        Ok(Self { qp, cq, recv, send })
    }
    async fn serve(&mut self) -> io::Result<()> {
        loop {
            let local_recv = self.recv.slice(&(0..1 * size_of::<QueryRequest>()));
            unsafe { self.qp.post_receive(&[local_recv], 0)? }
            await_completions::<1>(&mut self.cq).await?;

            let offset = self.recv.inner()[0].offset;
            let limit = self.recv.inner()[0].count;

            let local_send = self.send.slice(
                &(offset * size_of::<MockRecord>()..(offset + limit) * size_of::<MockRecord>()),
            );
            unsafe { self.qp.post_send(&[local_send], 0)? }
            await_completions::<1>(&mut self.cq).await?;
            println!("Served request: offset {}, count {}", offset, limit);
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
    let cq = ctx.create_cq(1024, 0)?;
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
    println!("QP created and connected");
    Ok((pd, cq, qp))
}

async fn create_server_mrs(
    pd: &ProtectionDomain,
) -> io::Result<(
    MemoryRegion<Vec<QueryRequest>>,
    MemoryRegion<Vec<MockRecord>>,
)> {
    let send_data = task::spawn_blocking(|| (0..RECORDS).map(MockRecord::new).collect()).await?;
    let send = pd.register(send_data)?;
    let recv_data = vec![QueryRequest::default(); 1];
    let recv = pd.register(recv_data)?;
    println!("MRs created");
    Ok((recv, send))
}
async fn synchronize(stream: &mut TcpStream) -> io::Result<()> {
    stream.write_all(b"READY\n").await?;
    let mut buf = [0u8; 16];
    let n = stream.read(&mut buf).await?;
    if &buf[..n] == b"READY\n" {
        println!("Synchronized");
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to synchronize",
        ))
    }
}
