pub mod jit;
pub mod pool;

use ibverbs::MemoryRegion;
use std::io;
use std::sync::Arc;

trait MemoryProvider: Send + Sync + Clone {
    fn allocate(&self, size: usize) -> io::Result<ManagedMemoryRegion>;
    fn deallocate(&self, mr: MemoryRegion) -> io::Result<()>;
}

pub struct ManagedMemoryRegion<'p> {
    mr: Option<MemoryRegion>,
    provider: Arc<&'p dyn MemoryProvider>,
}

impl<'p> Drop for ManagedMemoryRegion<'p> {
    fn drop(&mut self) {
        if let Some(mr) = self.mr.take() {
            let _ = self.provider.deallocate(mr);
        }
    }
}
