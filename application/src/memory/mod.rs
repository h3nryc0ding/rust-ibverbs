pub mod jit;
pub mod pooled;

use ibverbs::{MemoryRegion, ProtectionDomain};
use std::fmt::{Debug, Formatter};
use std::mem::ManuallyDrop;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::{any, io, mem};
use tracing::instrument;

pub trait Provider {
    fn new(pd: Arc<ProtectionDomain>) -> io::Result<Self>
    where
        Self: Sized;
    fn allocate<T: 'static + Default>(&self, count: usize) -> io::Result<MemoryHandle<T>>;
}

pub struct MemoryHandle<T = u8> {
    mr: ManuallyDrop<MemoryRegion<T>>,
    cleanup: Box<dyn FnOnce(MemoryRegion<T>) + Send + 'static>,
}

impl<T: 'static> MemoryHandle<T> {
    #[instrument(skip_all, name = "MemoryHandle::new", ret)]
    pub fn new(
        mr: MemoryRegion<T>,
        cleanup: impl FnOnce(MemoryRegion<T>) + Send + 'static,
    ) -> Self {
        Self {
            mr: ManuallyDrop::new(mr),
            cleanup: Box::new(cleanup),
        }
    }

    #[instrument(name = "MemoryHandle::cast", ret)]
    pub fn cast<U>(mut self) -> MemoryHandle<U> {
        let mr = unsafe { ManuallyDrop::take(&mut self.mr).cast::<U>() };
        let cleanup = mem::replace(&mut self.cleanup, Box::new(|_| ()));

        mem::forget(self);

        MemoryHandle {
            mr: ManuallyDrop::new(mr),
            cleanup: Box::new(|mr| {
                let mr = unsafe { mr.cast::<T>() };
                cleanup(mr);
            }),
        }
    }
}

impl<T> Deref for MemoryHandle<T> {
    type Target = MemoryRegion<T>;

    fn deref(&self) -> &Self::Target {
        &self.mr
    }
}

impl<T> DerefMut for MemoryHandle<T> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.mr
    }
}

impl<T> Drop for MemoryHandle<T> {
    #[instrument(name = "MemoryHandle::drop")]
    fn drop(&mut self) {
        let mr = unsafe { ManuallyDrop::take(&mut self.mr) };
        let cleanup = mem::replace(&mut self.cleanup, Box::new(|_| ()));
        cleanup(mr);
    }
}

impl<T> Debug for MemoryHandle<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        unsafe {
            f.debug_struct("MemoryHandle")
                .field("addr", &self.mr.addr())
                .field("length", &self.mr.length())
                .field("type", &any::type_name::<T>())
                .finish()
        }
    }
}
