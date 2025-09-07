use crate::memory::{MemoryHandle, MemoryProvider};
use ibverbs::ProtectionDomain;
use std::io;
use std::sync::Arc;
use tracing::{debug, instrument};

#[derive(Clone)]
pub struct JitProvider {
    pd: Arc<ProtectionDomain>,
}

impl JitProvider {
    pub fn new(pd: Arc<ProtectionDomain>) -> Self {
        JitProvider { pd }
    }
}

impl MemoryProvider for JitProvider {
    #[instrument(skip(self), name = "JitProvider::allocate", ret, err)]
    fn allocate<T: 'static>(&self, count: usize) -> io::Result<MemoryHandle<T>> {
        let mr = self.pd.allocate(count)?;

        let cleanup = |mr| {
            debug!("Deregistering memory region");
            drop(mr);
        };

        Ok(MemoryHandle::new(mr, cleanup))
    }
}
