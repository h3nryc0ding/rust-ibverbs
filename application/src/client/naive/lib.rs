use crate::client::RequestHandle;
use bytes::BytesMut;
use ibverbs::{MemoryRegion, RemoteMemorySlice};
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::{fmt, io};

#[derive(Default)]
pub struct Handle {
    pub(crate) state: Arc<State>,
}

#[derive(Default)]
pub(crate) struct State {
    pub(crate) bytes: Mutex<Option<BytesMut>>,

    pub(crate) registered: AtomicBool,
    pub(crate) posted: AtomicBool,
    pub(crate) received: AtomicBool,
    pub(crate) deregistered: AtomicBool,
}

impl RequestHandle for Handle {
    fn is_available(&self) -> bool {
        self.state.received.load(Ordering::Acquire)
    }

    fn is_acquirable(&self) -> bool {
        self.state.deregistered.load(Ordering::Acquire)
    }

    fn acquire(self) -> io::Result<BytesMut> {
        self.wait_acquirable();
        let mut bytes = self.state.bytes.lock().unwrap();

        Ok(bytes.take().unwrap())
    }
}

pub(crate) struct Pending {
    pub(crate) state: Arc<State>,
    pub(crate) mr: MemoryRegion,
}

pub(crate) struct RegistrationMessage {
    pub(crate) id: usize,
    pub(crate) state: Arc<State>,
    pub(crate) bytes: BytesMut,
    pub(crate) remote: RemoteMemorySlice,
}

pub(crate) struct PostMessage {
    pub(crate) id: usize,
    pub(crate) state: Arc<State>,
    pub(crate) mr: MemoryRegion,
    pub(crate) remote: RemoteMemorySlice,
}

pub(crate) struct DeregistrationMessage {
    pub(crate) id: usize,
    pub(crate) state: Arc<State>,
    pub(crate) mr: MemoryRegion,
}

impl Debug for RegistrationMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("RegistrationMessage")
            .field("id", &self.id)
            .field(
                "bytes",
                &format_args!(
                    "BytesMut {{ ptr: {:?}, len: {:?} }}",
                    self.bytes.as_ptr(),
                    self.bytes.len()
                ),
            )
            .field("remote", &self.remote)
            .finish()
    }
}

impl Debug for PostMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostMessage")
            .field("id", &self.id)
            .field("mr", &self.mr)
            .field("remote", &self.remote)
            .finish()
    }
}

impl Debug for DeregistrationMessage {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("DeregistrationMessage")
            .field("id", &self.id)
            .field("mr", &self.mr)
            .finish()
    }
}
