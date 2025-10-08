use crate::client::lib::RequestCore;
use bytes::BytesMut;
use ibverbs::{MemoryRegion, RemoteMemorySlice};
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub(crate) struct Pending {
    pub(crate) state: Arc<RequestCore>,
    pub(crate) mr: MemoryRegion,
}

pub(crate) struct RegistrationMessage {
    pub(crate) id: usize,
    pub(crate) state: Arc<RequestCore>,
    pub(crate) bytes: BytesMut,
    pub(crate) remote: RemoteMemorySlice,
}

pub(crate) struct PostMessage {
    pub(crate) id: usize,
    pub(crate) state: Arc<RequestCore>,
    pub(crate) mr: MemoryRegion,
    pub(crate) remote: RemoteMemorySlice,
}

pub(crate) struct DeregistrationMessage {
    pub(crate) id: usize,
    pub(crate) state: Arc<RequestCore>,
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
