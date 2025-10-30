pub mod copy;
pub mod ideal;
mod lib;
pub mod naive;
pub mod pipeline;

use bytes::BytesMut;
use ibverbs::{CompletionQueue, ProtectionDomain, QueuePair, RemoteMemorySlice};
use std::fmt::Debug;
use std::{hint, io};

#[cfg(feature = "hwlocality")]
pub(crate) const NUMA_NODE: usize = 1;

pub struct BaseClient {
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qps: Vec<QueuePair>,

    pub(crate) remotes: Vec<RemoteMemorySlice>,
}

pub trait Client {
    type Config: Eq + Debug + Clone;
    fn config(&self) -> &Self::Config;
}

pub trait BlockingClient: Client + Sized {
    fn new(c: BaseClient, config: Self::Config) -> io::Result<Self>;
    fn fetch(&mut self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<BytesMut>;
}

pub trait RequestHandle {
    fn is_available(&self) -> bool;
    fn wait_available(&self) {
        while !self.is_available() {
            hint::spin_loop();
        }
    }
    fn is_acquirable(&self) -> bool;
    fn wait_acquirable(&self) {
        while !self.is_acquirable() {
            hint::spin_loop();
        }
    }
    fn acquire(self) -> io::Result<BytesMut>;
}

pub trait NonBlockingClient: Client + Sized {
    type Handle: RequestHandle;
    fn new(c: BaseClient, config: Self::Config) -> io::Result<Self>;
    fn prefetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<Self::Handle>;
}
