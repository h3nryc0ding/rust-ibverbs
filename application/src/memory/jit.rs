use crate::memory::{MemoryHandle, Provider};
use ibverbs::ProtectionDomain;
use std::io;
use std::sync::Arc;
use tracing::{debug, instrument};

#[derive(Clone)]
pub struct JitProvider {
    pd: Arc<ProtectionDomain>,
}

impl Provider for JitProvider {
    fn new(pd: Arc<ProtectionDomain>) -> io::Result<Self> {
        Ok(JitProvider { pd })
    }

    #[instrument(skip(self), name = "JitProvider::allocate", ret, err)]
    fn allocate<T: 'static + Default>(&self, count: usize) -> io::Result<MemoryHandle<T>> {
        let mr = self.pd.allocate(count)?;

        let cleanup = |mr| {
            debug!("Deregistering memory region");
            drop(mr);
        };

        Ok(MemoryHandle::new(mr, cleanup))
    }
}
