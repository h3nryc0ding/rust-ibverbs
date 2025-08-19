mod send_recv;

use crate::protocol::QueryRequest;
use crate::rdma::Connection;
use crate::record::MockRecord;
use std::io;

pub const RECORDS: usize = 10 * 1024 * 1024; // 10M records ~ 10GB

pub trait Client {
    fn new(c: Connection) -> io::Result<Self>
    where
        Self: Sized;
    fn request(&mut self, r: QueryRequest) -> io::Result<Vec<MockRecord>>;
}

pub trait Server {
    fn new(c: Connection) -> io::Result<Self>
    where
        Self: Sized;
    fn serve(&mut self) -> io::Result<()>;
}

pub trait Protocol {
    type Client: Client;
    type Server: Server;
}
pub struct SendRecvProtocol;
