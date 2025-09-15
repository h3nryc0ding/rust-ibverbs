use crate::{BINCODE_CONFIG, MB};
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{
    CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair, RemoteMemorySlice, ibv_wc,
};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use tracing::{instrument, trace};

pub struct Client {
    ctx: Context,
    pd: ProtectionDomain,
    cq: CompletionQueue,
    qp: QueuePair,

    remote: RemoteMemorySlice,
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

    #[instrument(skip(self), err)]
    pub fn simple_request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        let local = self.pd.allocate(size)?;
        unsafe { self.qp.post_read(&[local.slice(..)], self.remote, 0)? };

        let mut completed = false;
        let mut completions = [ibv_wc::default(); 1];
        while !completed {
            for completion in self.cq.poll(&mut completions)? {
                if let Some((e, _)) = completion.error() {
                    panic!("wc error: {:?}", e)
                }
                completed = true;
            }
        }

        local.deregister()
    }

    #[instrument(skip(self), err)]
    pub fn spit_request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        static MR_SIZE: usize = 4 * MB;
        let response = vec![0u8; size];
        
        let local = self.pd.register()?;
    }
}
