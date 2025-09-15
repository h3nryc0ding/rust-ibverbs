use crate::memory::{MemoryHandle, Provider};
use ibverbs::{MemoryRegion, ProtectionDomain};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use tracing::instrument;

#[derive(Clone)]
pub struct PooledProvider {
    pd: Arc<ProtectionDomain>,
    pools: Arc<Mutex<HashMap<usize, VecDeque<MemoryRegion>>>>,
}

impl PooledProvider {
    #[instrument(skip(self), name = "PooledProvider::preallocate", ret, err)]
    pub fn preallocate<T: 'static + Default>(&self, count: usize, num: usize) -> io::Result<()> {
        todo!()
    }

    #[instrument(skip(self), name = "PooledProvider::_find")]
    fn _find(&self, size: usize) -> Option<MemoryRegion> {
        todo!()
    }

    #[instrument(skip(self), name = "PooledProvider::_allocate", err)]
    fn _allocate(&self, size: usize) -> io::Result<MemoryRegion> {
        todo!()
    }
}

impl Provider for PooledProvider {
    fn new(pd: Arc<ProtectionDomain>) -> io::Result<Self> {
        Ok(Self {
            pd,
            pools: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    #[instrument(skip(self), name = "PooledProvider::allocate", ret, err)]
    fn allocate<T: 'static>(&self, count: usize) -> io::Result<MemoryHandle<T>> {
        todo!()
    }
}
