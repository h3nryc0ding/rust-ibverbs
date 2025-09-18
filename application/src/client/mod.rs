mod copy;
mod pipeline;
mod simple;
mod split;
mod preallocated;

use crate::BINCODE_CONFIG;
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, RemoteMemorySlice};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use tracing::trace;

pub use crate::client::copy::CopyClient;
pub use crate::client::simple::SimpleClient;

pub struct BaseClient {
    #[allow(dead_code)]
    ctx: Context,
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qp: QueuePair,

    pub(crate) remote: RemoteMemorySlice,
}

impl BaseClient {
    pub fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
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

pub trait Client {
    fn new(base: BaseClient) -> io::Result<Self>
    where
        Self: Sized;
    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>>;
}
