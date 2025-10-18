use crate::client::RequestHandle;
use bytes::BytesMut;
use ibverbs::{MemoryRegion, RemoteMemorySlice};
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::{fmt, io};

#[derive(Default)]
pub struct Handle {
    pub(crate) state: Arc<State>,
}

#[derive(Default)]
pub(crate) struct State {
    pub(crate) posted: AtomicBool,
    pub(crate) received: AtomicBool,
}

impl RequestHandle for Handle {
    fn is_available(&self) -> bool {
        self.state.received.load(Ordering::Acquire)
    }

    fn is_acquirable(&self) -> bool {
        self.is_available()
    }

    fn acquire(self) -> io::Result<BytesMut> {
        unimplemented!("Can't acquire bytes from ideal")
    }
}

pub(crate) struct Pending {
    pub(crate) state: Arc<State>,
    pub(crate) mr: MemoryRegion,
}

pub(crate) struct PostMessage {
    pub(crate) id: usize,
    pub(crate) state: Arc<State>,
    pub(crate) remote: RemoteMemorySlice,
}

pub(crate) struct MRMessage(pub(crate) MemoryRegion);

impl Debug for PostMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostMessage")
            .field("id", &self.id)
            .field("remote", &self.remote)
            .finish()
    }
}

impl Debug for MRMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("MRMessage").field("mr", &self.0).finish()
    }
}
