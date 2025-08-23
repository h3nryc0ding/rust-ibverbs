mod read_write;
mod send_recv;

use crate::protocol::QueryRequest;
use crate::record::MockRecord;
use tokio::{io, net};

pub const RECORDS: usize = 1024 * 1024; // 1M records ~ 1GB

pub trait Client {
    fn new(ctx: ibverbs::Context, stream: &mut net::TcpStream) -> impl Future<Output = io::Result<Self>>
    where
        Self: Sized;
    fn request(&mut self, r: QueryRequest) -> impl Future<Output = io::Result<Vec<MockRecord>>>;
}

pub trait Server {
    fn new(ctx: ibverbs::Context, stream: &mut net::TcpStream) -> impl Future<Output = io::Result<Self>>
    where
        Self: Sized;
    fn serve(&mut self) -> impl Future<Output = io::Result<()>>;
}

pub trait Protocol {
    type Client: Client;
    type Server: Server;
}
pub struct SendRecvProtocol;
pub struct ReadWriteProtocol;
