use crate::client::BaseClient;
use crate::{BINCODE_CONFIG, PORT};
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::RemoteMemorySlice;
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use std::io;
use std::net::{IpAddr, TcpStream};
use tracing::trace;

const QP_COUNT: usize = 3;

impl BaseClient {
    pub fn new(addr: IpAddr) -> io::Result<Self> {
        let ctx = ibverbs::devices()?
            .iter()
            .next()
            .ok_or(io::ErrorKind::NotFound)?
            .open()?;

        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(1024, 0)?;

        let mut qps = Vec::with_capacity(QP_COUNT);
        let mut remotes = Vec::new();
        for _ in 0..QP_COUNT {
            let mut stream = TcpStream::connect((addr, PORT))?;
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
            remotes = decode_from_std_read(&mut stream, BINCODE_CONFIG).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to receive remote memory regions: {:?}", e),
                )
            })?;
            qps.push(qp);
        }

        Ok(Self {
            pd,
            cq,
            qps,
            remotes,
        })
    }

    pub fn remotes(&self) -> Vec<RemoteMemorySlice> {
        self.remotes.clone()
    }
}

pub(crate) fn encode_wr_id(req_id: usize, chunk_id: usize) -> u64 {
    ((req_id as u64) << 32) | (chunk_id as u64)
}

pub(crate) fn decode_wr_id(wr_id: u64) -> (usize, usize) {
    let req_id = (wr_id >> 32) as usize;
    let chunk_id = (wr_id & u32::MAX as u64) as usize;
    (req_id, chunk_id)
}
