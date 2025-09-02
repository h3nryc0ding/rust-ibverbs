mod read_write;
mod send_recv;

use crate::memory::BufferGuard;
use crate::record::MockRecord;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::{io, net};
use tracing::instrument;
use crate::protocol::QueryRequest;

pub const RECORDS: usize = 512 * 1024; // 0.5M records ~ 0.5GB

pub trait Client: Clone {
    fn new(
        ctx: ibverbs::Context,
        stream: &mut net::TcpStream,
    ) -> impl Future<Output = io::Result<Self>>
    where
        Self: Sized;
    fn request(
        &mut self,
        r: QueryRequest,
    ) -> impl Future<Output = io::Result<BufferGuard<MockRecord>>>;
}

pub trait Server: Send + Sync {
    fn new(
        ctx: ibverbs::Context,
        stream: net::TcpStream,
    ) -> impl Future<Output = io::Result<Self>> + Send
    where
        Self: Sized;
    fn serve(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

pub trait Protocol {
    type Client: Client;
    type Server: Server;
}
pub struct SendRecvProtocol;
pub struct ReadWriteProtocol;

#[instrument(skip_all, fields(peer = %stream.peer_addr().unwrap()))]
async fn synchronize(stream: &mut net::TcpStream) -> io::Result<()> {
    const READY: &[u8] = b"READY\n";
    stream.write_all(READY).await?;
    let mut buf = [0u8; size_of_val(READY)];
    let n = stream.read(&mut buf).await?;
    if &buf[..n] == READY {
        Ok(())
    } else {
        Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "Failed to synchronize",
        ))
    }
}
