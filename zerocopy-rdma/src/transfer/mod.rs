mod read_write;
mod send_recv;

use crate::protocol::QueryRequest;
use crate::record::MockRecord;
use tokio::{io, net};
use crate::memory::BufferGuard;

pub const RECORDS: usize = 512 * 1024; // 0.5M records ~ 0.5GB

pub trait Client: Clone {
    fn new(ctx: ibverbs::Context, stream: &mut net::TcpStream) -> impl Future<Output=io::Result<Self>>
    where
        Self: Sized;
    fn request(&mut self, r: QueryRequest) -> impl Future<Output=io::Result<BufferGuard<MockRecord>>>;
}

pub trait Server: Send + Sync {
    fn new(ctx: ibverbs::Context, stream: net::TcpStream) -> impl Future<Output=io::Result<Self>> + Send
    where
        Self: Sized;
    fn serve(&mut self) -> impl Future<Output=io::Result<()>> + Send;
}

pub trait Protocol {
    type Client: Client;
    type Server: Server;
}
pub struct SendRecvProtocol;
pub struct ReadWriteProtocol;
