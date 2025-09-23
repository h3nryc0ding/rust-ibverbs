mod copy;
mod ideal;
mod ideal_threaded;
mod naive;
mod split;

use crate::BINCODE_CONFIG;
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, RemoteMemorySlice};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::{Arc, Mutex};
use tracing::trace;

pub use crate::client::copy::CopyClient;
pub use crate::client::ideal::IdealClient;
pub use crate::client::ideal_threaded::IdealThreadedClient;
pub use crate::client::naive::NaiveClient;
pub use crate::client::split::SplitClient;

pub struct BaseSingleQPClient {
    #[allow(dead_code)]
    ctx: Context,
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qp: QueuePair,

    pub(crate) remote: RemoteMemorySlice,
}

pub struct BaseMultiQPClient {
    #[allow(dead_code)]
    ctx: Context,
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qps: Vec<QueuePair>,

    pub(crate) remote: RemoteMemorySlice,
}

#[derive(Clone)]
pub struct BaseThreadedSingleQPClient {
    #[allow(dead_code)]
    ctx: Arc<Context>,
    pub(crate) pd: Arc<Mutex<ProtectionDomain>>,
    pub(crate) cq: Arc<Mutex<CompletionQueue>>,
    pub(crate) qp: Arc<Mutex<QueuePair>>,

    pub(crate) remote: RemoteMemorySlice,
}

#[derive(Clone)]
pub struct BaseThreadedMultiQPClient {
    #[allow(dead_code)]
    ctx: Arc<Context>,
    pub(crate) pd: Arc<Mutex<ProtectionDomain>>,
    pub(crate) cq: Arc<Mutex<CompletionQueue>>,
    pub(crate) qps: Vec<Arc<Mutex<QueuePair>>>,

    pub(crate) remote: RemoteMemorySlice,
}

impl BaseSingleQPClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let mut stream = TcpStream::connect(addr)?;
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(1024, 0)?;

        let qp = {
            let pqp = pd
                .create_qp(&cq, &cq, IBV_QPT_RC)?
                .allow_remote_rw()
                .build()?;
            let remote = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
            trace!("Server remote endpoint: {:?}", remote);
            let local = pqp.endpoint()?;
            trace!("Client local endpoint: {:?}", local);
            encode_into_std_write(&local, &mut stream, BINCODE_CONFIG).unwrap();
            pqp.handshake(remote)?
        };

        let remote = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
        trace!("Server remote slice: {:?}", remote);

        Ok(Self {
            ctx,
            pd,
            cq,
            qp,
            remote,
        })
    }
}

impl BaseMultiQPClient {
    pub fn new<const QP: usize>(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(1024, 0)?;

        let mut remote_mr: RemoteMemorySlice = Default::default();
        let mut qps = Vec::with_capacity(QP);
        for _ in 0..QP {
            let mut stream = TcpStream::connect(&addr)?;
            let pqp = pd
                .create_qp(&cq, &cq, IBV_QPT_RC)?
                .allow_remote_rw()
                .build()?;
            let remote = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
            trace!("Server remote endpoint: {:?}", remote);
            let local = pqp.endpoint()?;
            trace!("Client local endpoint: {:?}", local);
            encode_into_std_write(&local, &mut stream, BINCODE_CONFIG).unwrap();
            let qp = pqp.handshake(remote)?;

            qps.push(qp);
            remote_mr = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
        }
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

impl BaseThreadedSingleQPClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let client = BaseSingleQPClient::new(ctx, addr)?;
        Ok(Self {
            ctx: Arc::new(client.ctx),
            pd: Arc::new(Mutex::new(client.pd)),
            cq: Arc::new(Mutex::new(client.cq)),
            qp: Arc::new(Mutex::new(client.qp)),
            remote: client.remote,
        })
    }
}

impl BaseThreadedMultiQPClient {
    fn new<const QP: usize>(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let client = BaseMultiQPClient::new::<QP>(ctx, addr)?;
        Ok(Self {
            ctx: Arc::new(client.ctx),
            pd: Arc::new(Mutex::new(client.pd)),
            cq: Arc::new(Mutex::new(client.cq)),
            qps: client
                .qps
                .into_iter()
                .map(|qp| Arc::new(Mutex::new(qp)))
                .collect(),
            remote: client.remote,
        })
    }
}

pub trait Client {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self>
    where
        Self: Sized;
    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>>;
}
