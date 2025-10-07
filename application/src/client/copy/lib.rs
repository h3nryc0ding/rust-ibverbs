use crate::client::RequestCore;
use bytes::BytesMut;
use ibverbs::MemoryRegion;
use std::fmt;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub(crate) struct Pending {
    pub(crate) state: Arc<RequestCore>,
    pub(crate) mr: MemoryRegion,
    pub(crate) bytes: BytesMut,
}

pub(crate) struct MRMessage(pub(crate) MemoryRegion);

pub(crate) struct PostMessage {
    pub(crate) id: usize,
    pub(crate) chunk: usize,
    pub(crate) state: Arc<RequestCore>,
    pub(crate) bytes: BytesMut,
}

pub(crate) struct CopyMessage {
    pub(crate) id: usize,
    pub(crate) chunk: usize,
    pub(crate) state: Arc<RequestCore>,
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
