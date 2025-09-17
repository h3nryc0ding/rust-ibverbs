mod pipeline;
mod simple;
mod split;
mod copy;

use crate::client::{Client, ConcurrentClient};
use std::io;

pub trait TransferStrategy {
    fn request(client: &mut Client, size: usize) -> io::Result<Box<[u8]>>;
}

pub trait ConcurrentTransferStrategy {
    fn request(client: &mut ConcurrentClient, size: usize) -> io::Result<Box<[u8]>>;
}

pub struct CopyStrategy;
pub struct SimpleStrategy;
pub struct SplitStrategy;
pub struct PipelineStrategy;
