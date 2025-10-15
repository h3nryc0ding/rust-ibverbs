use crate::chunks_unsplit;
use crate::client::RequestHandle;
use bytes::BytesMut;
use dashmap::DashMap;
use ibverbs::{MemoryRegion, RemoteMemorySlice};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{fmt, io};

pub struct Handle {
    pub(crate) chunks: usize,
    pub(crate) state: Arc<State>,
}

pub(crate) struct State {
    pub(crate) bytes: DashMap<usize, BytesMut>,

    pub(crate) registered: AtomicUsize,
    pub(crate) posted: AtomicUsize,
    pub(crate) received: AtomicUsize,
    pub(crate) deregistered: AtomicUsize,
}

impl Handle {
    pub fn new(chunks: usize) -> Self {
        Self {
            chunks,
            state: Arc::new(State {
                bytes: DashMap::with_capacity(chunks),
                registered: AtomicUsize::new(0),
                posted: AtomicUsize::new(0),
                received: AtomicUsize::new(0),
                deregistered: AtomicUsize::new(0),
            }),
        }
    }
}

impl RequestHandle for Handle {
    fn is_available(&self) -> bool {
        self.state.received.load(Ordering::Acquire) >= self.chunks
    }

    fn is_acquirable(&self) -> bool {
        self.state.deregistered.load(Ordering::Acquire) >= self.chunks
    }

    fn acquire(self) -> io::Result<BytesMut> {
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
}

pub(crate) struct RegistrationMessage {
    pub(crate) id: usize,
    pub(crate) chunk: usize,
    pub(crate) state: Arc<State>,
    pub(crate) bytes: BytesMut,
    pub(crate) remote: RemoteMemorySlice,
}

pub(crate) struct PostMessage {
    pub(crate) id: usize,
    pub(crate) chunk: usize,
    pub(crate) state: Arc<State>,
    pub(crate) mr: MemoryRegion,
    pub(crate) remote: RemoteMemorySlice,
}

pub(crate) struct DeregistrationMessage {
    pub(crate) id: usize,
    pub(crate) chunk: usize,
    pub(crate) state: Arc<State>,
    pub(crate) mr: MemoryRegion,
}

impl Debug for RegistrationMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistrationMessage")
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

impl Debug for PostMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostMessage")
            .field("id", &self.id)
            .field("chunk", &self.chunk)
            .field("mr", &self.mr)
            .finish()
    }
}

impl Debug for DeregistrationMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeregistrationMessage")
            .field("id", &self.id)
            .field("chunk", &self.chunk)
            .field("mr", &self.mr)
            .finish()
    }
}
