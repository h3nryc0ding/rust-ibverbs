use crate::memory::BufferGuard;
use crate::protocol::QueryRequest;
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol, ReadWriteProtocol, Server};
use ibverbs::Context;
use tokio::io;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct ReadWriteClient;

impl Client for ReadWriteClient {
    async fn new(_ctx: Context, _stream: &mut TcpStream) -> io::Result<Self> {
        todo!()
    }

    async fn request(&mut self, _r: QueryRequest) -> io::Result<BufferGuard<MockRecord>> {
        todo!()
    }
}

pub struct ReadWriteServer;

impl Server for ReadWriteServer {
    async fn new(_ctx: Context, _stream: TcpStream) -> io::Result<Self> {
        todo!()
    }

    async fn serve(&mut self) -> io::Result<()> {
        todo!()
    }
}

impl Protocol for ReadWriteProtocol {
    type Client = ReadWriteClient;
    type Server = ReadWriteServer;
}
