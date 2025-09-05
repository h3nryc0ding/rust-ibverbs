pub mod jit;
pub mod pool;

use ibverbs::{LocalMemorySlice, MemoryRegion};
use std::fmt::{Debug, Formatter};
use std::io;
use std::ops::{Deref, DerefMut};
use tracing::instrument;

pub struct Handle<D> {
    mr: Option<MemoryRegion<D>>,
    release: Box<dyn Releaser<D>>,
}

impl<D> Handle<D> {
    #[instrument(skip_all, name = "Handle::new")]
    fn new(mr: MemoryRegion<D>, release: impl Releaser<D> + 'static) -> Self {
        Self {
            mr: Some(mr),
            release: Box::new(release),
        }
    }
    pub fn slice(&self) -> LocalMemorySlice {
        self.mr().slice(0..size_of::<D>())
    }

    fn mr(&self) -> &MemoryRegion<D> {
        self.mr.as_ref().unwrap()
    }

    fn mr_mut(&mut self) -> &mut MemoryRegion<D> {
        self.mr.as_mut().unwrap()
    }
}

impl<D> Drop for Handle<D> {
    #[instrument(skip_all, name = "Handle::drop")]
    fn drop(&mut self) {
        if let Some(mr) = self.mr.take() {
            self.release.release(mr);
        }
    }
}

impl<D> Deref for Handle<D> {
    type Target = D;

    fn deref(&self) -> &Self::Target {
        self.mr()
    }
}

impl<D> DerefMut for Handle<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mr_mut()
    }
}

impl<D: Debug> Debug for Handle<D> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Handle").field("data", &**self).finish()
    }
}

trait Releaser<D>: Send {
    fn release(&self, mr: MemoryRegion<D>);
}

pub trait Provider<D>: Clone + Send {
    async fn acquire_mr(&mut self) -> io::Result<Handle<D>>;
}
