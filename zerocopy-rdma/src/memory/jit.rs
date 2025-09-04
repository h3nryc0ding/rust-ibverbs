use crate::memory::{Handle, Provider, Releaser};
use ibverbs::{MemoryRegion, ProtectionDomain};
use std::io;
use std::marker::PhantomData;
use std::sync::Arc;
use tokio::task;

struct JustInTimeReleaser;

impl<D> Releaser<D> for JustInTimeReleaser {
    fn release(&self, mr: MemoryRegion<D>) {
        drop(mr);
    }
}

pub struct JustInTimeProvider<D> {
    pd: Arc<ProtectionDomain>,

    _p: PhantomData<D>,
}

impl<D> JustInTimeProvider<D> {
    pub fn new(pd: ProtectionDomain) -> Self {
        Self {
            pd: Arc::new(pd),
            _p: PhantomData,
        }
    }
}

impl<D> Clone for JustInTimeProvider<D> {
    fn clone(&self) -> Self {
        JustInTimeProvider {
            pd: self.pd.clone(),
            _p: PhantomData,
        }
    }
}

impl<D: Default + Send + 'static> Provider<D> for JustInTimeProvider<D> {
    async fn acquire_mr(&mut self) -> io::Result<Handle<D>> {
        let pd = self.pd.clone();
        let mr = task::spawn_blocking(move || pd.allocate()).await??;
        Ok(Handle {
            mr: Some(mr),
            release: Box::new(JustInTimeReleaser),
        })
    }
}
