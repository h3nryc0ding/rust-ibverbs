use crate::transfer::Server;
use std::net::ToSocketAddrs;

pub struct SendRecvReadServer;

impl Server for SendRecvReadServer {
    fn new<A: ToSocketAddrs>(addr: A) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn serve(&mut self) -> std::io::Result<()> {
        todo!()
    }
}
