use crate::memory::Handle;
use crate::protocol::{QueryRequest, QueryResponse};
use crate::transfer::{CLIENT_RECORDS, Client, Protocol, Server, WriteProtocol};
use ibverbs::Context;
use tokio::io;
use tokio::net::TcpStream;

#[derive(Clone)]
pub struct WriteClient;

impl Client for WriteClient {
    async fn new(_ctx: Context, _stream: &mut TcpStream) -> io::Result<Self> {
        todo!()
    }

    async fn request(
        &mut self,
        _r: QueryRequest,
    ) -> io::Result<Handle<QueryResponse<CLIENT_RECORDS>>> {
        todo!()
    }
}

pub struct WriteServer;

impl Server for WriteServer {
    async fn new(_ctx: Context, _stream: TcpStream) -> io::Result<Self> {
        todo!()
    }

    async fn serve(&mut self) -> io::Result<()> {
        todo!()
    }
}

impl Protocol for WriteProtocol {
    type Client = WriteClient;
    type Server = WriteServer;
}
