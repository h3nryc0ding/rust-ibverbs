use crate::rdma::Connection;
use crate::transfer::{Protocol, Server};
use std::io;

pub fn run<P: Protocol>(c: Connection) -> io::Result<()> {
    let mut server = P::Server::new(c)?;
    server.serve()
}
