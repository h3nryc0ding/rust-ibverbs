pub mod copy;
pub mod ideal;
mod lib;
pub mod naive;
pub mod pipeline;

use bytes::BytesMut;
use ibverbs::{CompletionQueue, ProtectionDomain, QueuePair, RemoteMemorySlice};
use std::{hint, io};

pub(crate) const NUMA_NODE: usize = 1;

pub struct BaseClient {
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qps: Vec<QueuePair>,

    pub(crate) remotes: Vec<RemoteMemorySlice>,
}

pub trait BlockingClient: Sized {
    type Config;
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

pub trait NonBlockingClient: Sized {
    type Config;
    type Handle: RequestHandle;
    fn new(c: BaseClient, config: Self::Config) -> io::Result<Self>;
    fn prefetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<Self::Handle>;
}

pub trait AsyncClient: Sized {
    type Config;
    fn new(c: BaseClient, config: Self::Config) -> impl Future<Output = io::Result<Self>>;
    fn prefetch(
        &self,
        bytes: BytesMut,
        remote: &RemoteMemorySlice,
    ) -> impl Future<Output = io::Result<BytesMut>>;
}
