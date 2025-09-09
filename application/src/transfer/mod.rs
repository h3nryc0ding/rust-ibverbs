use crate::memory::{MemoryHandle, Provider};
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, ibv_qp_type, ibv_wc};
use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use tracing::info;

mod send_recv;
mod send_recv_read;
mod send_recv_write;

pub use send_recv::SendRecvProtocol;
pub use send_recv_read::SendRecvReadProtocol;

pub trait Protocol {
    type Client<M: Provider>: Client<M>;
    type Server<M: Provider>: Server<M>;
}

pub trait Client<M: Provider> {
    fn new(addr: impl ToSocketAddrs) -> io::Result<Self>
    where
        Self: Sized;

    fn request(&mut self, req: u8) -> io::Result<MemoryHandle>;
}

pub trait Server<M: Provider> {
    fn new(addr: impl ToSocketAddrs) -> io::Result<Self>
    where
        Self: Sized;

    fn serve(&mut self) -> io::Result<()>;
}

fn await_completions<const N: usize>(cq: &mut CompletionQueue) -> io::Result<()> {
    let mut outstanding = N;
    let mut completions = [ibv_wc::default(); N];
    while outstanding > 0 {
        for completion in cq.poll(&mut completions)? {
            if let Some(err) = completion.error() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!(
                        "Work completion error: {:?} for {}",
                        err,
                        completion.wr_id()
                    ),
                ));
            }
            info!("{:?}", completion);
            outstanding -= 1;
        }
    }

    Ok(())
}

pub fn handshake(
    ctx: &Context,
    stream: &mut TcpStream,
) -> io::Result<(ProtectionDomain, CompletionQueue, QueuePair)> {
    let pd = ctx.alloc_pd()?;
    let cq = ctx.create_cq(4096, 0)?;
    let qp = {
        let qpb = pd
            .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)?
            .allow_remote_rw()
            // .set_timeout(0)
            // .set_retry_count(7)
            .set_rnr_retry(7)
            //.set_min_rnr_timer(0)
            .build()?;
        let local = qpb.endpoint()?;
        let data = serde_json::to_vec(&local)?;
        let mut buf = [0u8; 1024];
        let n = {
            stream.write_all(&data)?;
            stream.read(&mut buf)?
        };
        let remote = serde_json::from_slice(&buf[..n])?;
        qpb.handshake(remote)?
    };
    Ok((pd, cq, qp))
}

fn synchronize(stream: &mut TcpStream) -> io::Result<()> {
    const READY: &[u8] = b"READY\n";
    stream.write_all(READY)?;
    let mut buf = [0u8; size_of_val(READY)];
    let n = stream.read(&mut buf)?;
    if &buf[..n] == READY {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to synchronize",
        ))
    }
}
