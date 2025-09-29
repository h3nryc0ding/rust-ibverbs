mod copy;
mod copy_threaded;
mod ideal;
mod ideal_threaded;
mod naive;
mod pipeline;
mod pipeline_async;
mod pipeline_threaded;

use crate::BINCODE_CONFIG;
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, RemoteMemorySlice};
use std::net::{TcpStream, ToSocketAddrs};
use std::{array, io};
use tracing::trace;

pub use crate::client::copy::CopyClient;
pub use crate::client::copy_threaded::CopyThreadedClient;
pub use crate::client::ideal::IdealClient;
pub use crate::client::ideal_threaded::IdealThreadedClient;
pub use crate::client::naive::NaiveClient;
pub use crate::client::pipeline::PipelineClient;
pub use crate::client::pipeline_async::PipelineAsyncClient;
pub use crate::client::pipeline_threaded::PipelineThreadedClient;

pub struct BaseClient<const QPS: usize> {
    #[allow(dead_code)]
    ctx: Context,
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qps: [QueuePair; QPS],

    pub(crate) remote: RemoteMemorySlice,
}

impl<const QPS: usize> BaseClient<QPS> {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(1024, 0)?;

        let mut remote_mr: RemoteMemorySlice = Default::default();
        let qps = array::from_fn(|_| {
            let mut stream = TcpStream::connect(&addr).unwrap();
            let pqp = pd
                .create_qp(&cq, &cq, IBV_QPT_RC)
                .unwrap()
                .allow_remote_rw()
                .build()
                .unwrap();
            let remote = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
            trace!("Server remote endpoint: {:?}", remote);
            let local = pqp.endpoint().unwrap();
            trace!("Client local endpoint: {:?}", local);
            encode_into_std_write(&local, &mut stream, BINCODE_CONFIG).unwrap();
            let qp = pqp.handshake(remote).unwrap();
            remote_mr = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
            qp
        });
        let remote = remote_mr;

        trace!("Server remote slice: {:?}", remote);
        Ok(Self {
            ctx,
            pd,
            cq,
            qps,
            remote,
        })
    }
}

pub trait Client: Sized {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self>;
    fn request(&mut self, dst: &mut [u8]) -> io::Result<()>;
}

pub trait AsyncClient: Sized {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> impl Future<Output = io::Result<Self>>;
    fn request(&mut self, dst: &mut [u8]) -> impl Future<Output = io::Result<()>>;
}
