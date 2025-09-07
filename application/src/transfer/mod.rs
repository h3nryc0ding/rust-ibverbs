use crate::memory::MemoryHandle;
use ibverbs::{CompletionQueue, ibv_wc};
use std::io;
use std::net::ToSocketAddrs;

mod send_recv;
mod send_recv_read;
mod send_recv_write;

pub use send_recv::SendRecvProtocol;
pub use send_recv_read::SendRecvReadProtocol;

const SERVER_SIZE: usize = 1 * 2usize.pow(30); // 1 GiB

pub trait Protocol {
    type Client: Client;
    type Server: Server;
}

pub trait Client {
    fn new<A: ToSocketAddrs>(addr: A) -> io::Result<Self>
    where
        Self: Sized;

    fn request(&mut self, req: u8) -> io::Result<MemoryHandle>;
}

pub trait Server {
    fn new<A: ToSocketAddrs>(addr: A) -> io::Result<Self>
    where
        Self: Sized;

    fn serve(&mut self) -> io::Result<()>;
}

fn await_completions<const N: usize>(cq: &mut CompletionQueue) -> io::Result<()> {
    let mut c = N;
    let mut completions = [ibv_wc::default(); N];
    while c > 0 {
        for completion in cq.poll(&mut completions)? {
            if let Some(err) = completion.error() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Work completion error: {:?}", err),
                ));
            }
            c -= 1;
        }
    }

    Ok(())
}
