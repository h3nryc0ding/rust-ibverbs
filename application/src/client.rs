use crate::BINCODE_CONFIG;
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, RemoteMemorySlice};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use tracing::trace;

pub struct Client {
    ctx: Context,
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qp: QueuePair,

    pub(crate) remote: RemoteMemorySlice,
}

impl Client {
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

pub struct ConcurrentClient {
    ctx: Arc<Context>,
    pd: Arc<ProtectionDomain>,
    cq: Arc<CompletionQueue>,
    qp: Arc<QueuePair>,

    pub(crate) remote: RemoteMemorySlice,
}

impl ConcurrentClient {
    pub fn new(client: Client) -> Self {
        Self {
            ctx: Arc::new(client.ctx),
            pd: Arc::new(client.pd),
            cq: Arc::new(client.cq),
            qp: Arc::new(client.qp),
            remote: client.remote,
        }
    }
}
