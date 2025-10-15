use crate::chunks_unsplit;
use crate::client::RequestHandle;
use bytes::BytesMut;
use dashmap::DashMap;
use ibverbs::{MemoryRegion, RemoteMemorySlice};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Handle {
    pub(crate) chunks: usize,
    pub(crate) state: Arc<State>,
}

pub(crate) struct State {
    pub(crate) bytes: DashMap<usize, BytesMut>,

    pub(crate) posted: AtomicUsize,
    pub(crate) received: AtomicUsize,
    pub(crate) copied: AtomicUsize,
}

impl Handle {
    pub fn new(chunks: usize) -> Self {
        Self {
            chunks,
            state: Arc::new(State {
                bytes: DashMap::with_capacity(chunks),
                posted: AtomicUsize::new(0),
                received: AtomicUsize::new(0),
                copied: AtomicUsize::new(0),
            }),
        }
    }
}

impl RequestHandle for Handle {
    #[inline]
    fn is_available(&self) -> bool {
        self.state.copied.load(Ordering::Relaxed) >= self.chunks
    }

    #[inline]
    fn is_acquirable(&self) -> bool {
        self.is_available()
    }

    fn acquire(self) -> std::io::Result<BytesMut> {
        self.wait_acquirable();
        assert_eq!(self.chunks, self.state.bytes.len());

        chunks_unsplit(
            (0..self.chunks)
                .filter_map(move |i| self.state.bytes.remove(&i))
                .map(|(_, v)| v),
        )
    }
}

pub(crate) struct Pending {
    pub(crate) state: Arc<State>,
    pub(crate) mr: MemoryRegion,
    pub(crate) bytes: BytesMut,
}

pub(crate) struct MRMessage(pub(crate) MemoryRegion);

pub(crate) struct PostMessage {
    pub(crate) id: usize,
    pub(crate) chunk: usize,
    pub(crate) state: Arc<State>,
    pub(crate) remote: RemoteMemorySlice,
    pub(crate) bytes: BytesMut,
}

pub(crate) struct CopyMessage {
    pub(crate) id: usize,
    pub(crate) chunk: usize,
    pub(crate) state: Arc<State>,
    pub(crate) mr: MemoryRegion,
    pub(crate) bytes: BytesMut,
}

impl Debug for MRMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MRMessage").field("mr", &self.0).finish()
    }
}

impl Debug for PostMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostMessage")
            .field("id", &self.id)
            .field("chunk", &self.chunk)
            .field(
                "bytes",
                &format_args!(
                    "BytesMut {{ ptr: {:?}, len: {:?} }}",
                    self.bytes.as_ptr(),
                    self.bytes.len()
                ),
            )
            .finish()
    }
}

impl Debug for CopyMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("CopyMessage")
            .field("id", &self.id)
            .field("chunk", &self.chunk)
            .field(
                "bytes",
                &format_args!(
                    "BytesMut {{ ptr: {:?}, len: {:?} }}",
                    self.bytes.as_ptr(),
                    self.bytes.len()
                ),
            )
            .field("mr", &self.mr)
            .finish()
    }
}
