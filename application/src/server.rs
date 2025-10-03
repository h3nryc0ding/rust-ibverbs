use crate::{BINCODE_CONFIG, SERVER_DATA_SIZE};
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair};
use std::io;
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use tracing::trace;

pub struct Server {
    #[allow(dead_code)]
    ctx: Context,
    pd: ProtectionDomain,
    cq: CompletionQueue,
    listener: TcpListener,
    data: MemoryRegion,
}

impl Server {
    pub fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let listener = TcpListener::bind(addr)?;
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(1024, 0)?;
        let mut data = pd.allocate_zeroed(SERVER_DATA_SIZE)?;
        for i in 0..SERVER_DATA_SIZE {
            data[i] = i as u8;
        }
        Ok(Self {
            ctx,
            pd,
            cq,
            listener,
            data,
        })
    }

    pub fn serve(&mut self) -> io::Result<()> {
        let mut qps = Vec::new();
        for stream in self.listener.incoming() {
            let qp = self.handshake(stream?)?;
            qps.push(qp);
        }

        unreachable!()
    }

    fn handshake(&self, mut stream: TcpStream) -> io::Result<QueuePair> {
        let pqp = self
            .pd
            .create_qp(&self.cq, &self.cq, IBV_QPT_RC)?
            .allow_remote_rw()
            .build()?;
        let local = pqp.endpoint()?;

        trace!("Server local endpoint: {:?}", local);
        encode_into_std_write(local, &mut stream, BINCODE_CONFIG).unwrap();
        let remote = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
        trace!("Client remote endpoint: {:?}", remote);
        let qp = pqp.handshake(remote)?;

        let remote = self.data.slice_remote(..);
        encode_into_std_write(&remote, &mut stream, BINCODE_CONFIG).unwrap();
        trace!("Server remote slice: {:?}", remote);

        Ok(qp)
    }
}
