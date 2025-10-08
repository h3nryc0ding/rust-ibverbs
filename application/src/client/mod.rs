pub mod copy;
pub mod ideal;
mod lib;
pub mod naive;
pub mod pipeline;

use bytes::BytesMut;
use ibverbs::{CompletionQueue, ProtectionDomain, QueuePair, RemoteMemorySlice};
use lib::RequestHandle;
use std::io;

pub(crate) const NUMA_NODE: usize = 1;

pub struct BaseClient {
    pub(crate) pd: ProtectionDomain,
    pub(crate) cq: CompletionQueue,
    pub(crate) qps: Vec<QueuePair>,

    pub(crate) remotes: Vec<RemoteMemorySlice>,
}

pub trait BlockingClient {
    fn fetch(&mut self, bytes: BytesMut, remote: RemoteMemorySlice) -> io::Result<BytesMut>;
}

pub trait NonBlockingClient {
    fn prefetch(&mut self, bytes: BytesMut, remote: RemoteMemorySlice)
    -> io::Result<RequestHandle>;
}

pub trait AsyncClient {
    fn prefetch(
        &self,
        bytes: BytesMut,
        remote: RemoteMemorySlice,
    ) -> impl Future<Output = io::Result<BytesMut>>;
}
