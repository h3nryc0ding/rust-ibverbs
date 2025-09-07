use crate::memory::{MemoryHandle, MemoryProvider};
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
    pub fn new(pd: Arc<ProtectionDomain>) -> Self {
        Self {
            pd,
            pools: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn preallocate<T: 'static>(&self, count: usize, num: usize) -> io::Result<()> {
        let mut pools = self.pools.lock().unwrap();
        let pool = pools
            .entry(count * size_of::<T>())
            .or_insert_with(VecDeque::new);
        for _ in 0..num {
            let mr = self.pd.allocate::<T>(count)?.cast();
            pool.push_back(mr);
        }
        Ok(())
    }
}

impl MemoryProvider for PooledProvider {
    #[instrument(skip(self), name = "PooledProvider::allocate", ret, err)]
    fn allocate<T: 'static>(&self, count: usize) -> io::Result<MemoryHandle<T>> {
        let size = count * size_of::<T>();
        let mr = {
            let mut pools = self.pools.lock().unwrap();
            pools.get_mut(&size).and_then(|p| p.pop_front())
        };
        let mr = match mr {
            Some(mr) => mr,
            None => self.pd.allocate(size)?,
        }
        .cast();

        let pools = Arc::clone(&self.pools);
        let cleanup = move |mr: MemoryRegion<T>| {
            let mr = mr.cast::<u8>();
            let mut pools = pools.lock().unwrap();
            let pool = pools.entry(size).or_insert_with(VecDeque::new);
            pool.push_back(mr);
        };

        Ok(MemoryHandle::new(mr, cleanup))
    }
}
