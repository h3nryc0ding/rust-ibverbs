mod copy;
mod copy_threaded;
mod ideal;
mod naive;
mod pipeline;
mod pipeline_async;
mod pipeline_threaded;

use crate::BINCODE_CONFIG;
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, RemoteMemorySlice};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{hint, io};
use tokio::task;
use tracing::trace;

pub use crate::client::copy::CopyClient;
pub use crate::client::copy_threaded::CopyThreadedClient;
pub use crate::client::ideal::IdealClient;
pub use crate::client::naive::NaiveClient;
pub use crate::client::pipeline::PipelineClient;
pub use crate::client::pipeline_async::PipelineAsyncClient;
pub use crate::client::pipeline_threaded::PipelineThreadedClient;

pub struct ClientConfig {
    pub server_addr: SocketAddr,
    pub mr_size: usize,
    pub qp_count: usize,
}

pub struct BaseClient {
    #[allow(dead_code)]
    ctx: Context,
    pub(crate) cfg: ClientConfig,
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qps: Vec<QueuePair>,

    pub(crate) remote: RemoteMemorySlice,
}

impl BaseClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(1024, 0)?;

        let mut remote_mr: RemoteMemorySlice = Default::default();
        let mut qps = Vec::with_capacity(cfg.qp_count);
        for _ in 0..cfg.qp_count {
            let mut stream = TcpStream::connect(&cfg.server_addr).unwrap();
            let pqp = pd
                .create_qp(&cq, &cq, IBV_QPT_RC)?
                .allow_remote_rw()
                .build()?;
            let remote = decode_from_std_read(&mut stream, BINCODE_CONFIG).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to receive remote endpoint: {:?}", e),
                )
            })?;
            trace!("Server remote endpoint: {:?}", remote);
            let local = pqp.endpoint()?;
            trace!("Client local endpoint: {:?}", local);
            encode_into_std_write(&local, &mut stream, BINCODE_CONFIG).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to send local endpoint: {:?}", e),
                )
            })?;
            let qp = pqp.handshake(remote)?;
            remote_mr = decode_from_std_read(&mut stream, BINCODE_CONFIG).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to receive remote memory region: {:?}", e),
                )
            })?;
            qps.push(qp);
        }
        let remote = remote_mr;

        trace!("Server remote slice: {:?}", remote);
        Ok(Self {
            ctx,
            cfg,
            pd,
            cq,
            qps,
            remote,
        })
    }
}

#[derive(Default)]
struct RequestState {
    registered_acquired: AtomicUsize,
    posted: AtomicUsize,
    received: AtomicUsize,
    deregistered_copied: AtomicUsize,
}

pub struct RequestHandle {
    id: usize,
    chunks: usize,
    state: Arc<RequestState>,
}

impl RequestHandle {
    pub fn wait(&self) {
        while self.state.deregistered_copied.load(Ordering::Relaxed) < self.chunks {
            hint::spin_loop();
        }
    }

    pub async fn wait_async(&self) {
        while self.state.deregistered_copied.load(Ordering::Relaxed) < self.chunks {
            task::yield_now().await;
        }
    }
}

pub trait BlockingClient: Sized {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self>;
    fn request(&mut self, dst: &mut [u8]) -> io::Result<()>;
}

pub trait NonBlockingClient: Sized {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self>;
    fn request(&mut self, dst: &mut [u8]) -> io::Result<RequestHandle>;
}

pub trait AsyncClient: Sized {
    fn new(ctx: Context, cfg: ClientConfig) -> impl Future<Output = io::Result<Self>>;
    fn request(&mut self, dst: &mut [u8]) -> impl Future<Output = io::Result<()>>;
}

fn encode_wr_id(req_id: usize, chunk_id: usize) -> u64 {
    ((req_id as u64) << 32) | (chunk_id as u64)
}

fn decode_wr_id(wr_id: u64) -> (usize, usize) {
    let req_id = (wr_id >> 32) as usize;
    let chunk_id = (wr_id & u32::MAX as u64) as usize;
    (req_id, chunk_id)
}
