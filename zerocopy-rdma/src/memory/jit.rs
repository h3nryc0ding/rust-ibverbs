use crate::memory::{ManagedMemoryRegion, MemoryProvider};
use ibverbs::{MemoryRegion, ProtectionDomain};
use std::io;
use std::sync::Arc;

pub struct JitProvider {
    pd: Arc<ProtectionDomain>,
}

impl JitProvider {
    pub fn new(pd: Arc<ProtectionDomain>) -> Self {
        Self { pd }
    }
}

impl MemoryProvider for JitProvider {
    fn allocate(&self, size: usize) -> io::Result<ManagedMemoryRegion> {
        let mr = self.pd.allocate(size)?;
        Ok(ManagedMemoryRegion {
            mr: Some(mr),
            provider: self,
        })
    }
    fn deallocate(&self, mr: MemoryRegion) -> io::Result<()> {
        drop(mr);
        Ok(())
    }
}
