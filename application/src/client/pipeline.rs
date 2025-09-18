use std::io;
use crate::client::{BaseClient, Client};

pub struct PipelineClient;

impl Client for PipelineClient {
    fn new(base: BaseClient) -> io::Result<Self> {
        todo!()
    }


    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        todo!()
    }
}
