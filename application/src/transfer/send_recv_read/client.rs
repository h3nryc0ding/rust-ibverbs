use crate::memory::MemoryHandle;
use crate::transfer::Client;
use std::net::ToSocketAddrs;

pub struct SendRecvReadClient;

impl Client for SendRecvReadClient {
    fn new<A: ToSocketAddrs>(addr: A) -> std::io::Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    fn request(&mut self, req: u8) -> std::io::Result<MemoryHandle> {
        todo!()
    }
}
