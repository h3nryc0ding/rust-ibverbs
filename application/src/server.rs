#[cfg(feature = "hwlocality")]
use crate::hwlocality::pin_thread_to_node;
use crate::{BINCODE_CONFIG, PORT};
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{CompletionQueue, MemoryRegion, ProtectionDomain, QueuePair};
use std::io;
use std::net::{Ipv6Addr, TcpListener, TcpStream};
use tracing::trace;

#[cfg(feature = "hwlocality")]
pub const NUMA_NODE: usize = 0;

pub struct Config {
    pub size: usize,
}

pub struct Server {
    pd: ProtectionDomain,
    cq: CompletionQueue,
    listener: TcpListener,
    data: MemoryRegion,
}

impl Server {
    pub fn new(config: Config) -> io::Result<Self> {
        #[cfg(feature = "hwlocality")]
        pin_thread_to_node::<NUMA_NODE>()?;

        let ctx = ibverbs::devices()?
            .iter()
            .next()
            .ok_or(io::ErrorKind::NotFound)?
            .open()?;

        let listener = TcpListener::bind((Ipv6Addr::UNSPECIFIED, PORT))?;
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(1024, 0)?;
        let mut data = pd.allocate_zeroed(config.size)?;
        for i in 0..config.size {
            data[i] = i as u8;
        }
        Ok(Self {
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

        encode_into_std_write(
            &self.data.slice_remote(..).collect::<Vec<_>>(),
            &mut stream,
            BINCODE_CONFIG,
        )
        .unwrap();

        Ok(qp)
    }
}
