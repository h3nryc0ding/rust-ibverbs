use crate::memory::{MemoryHandle, Provider};
use ibverbs::{MemoryRegion, ProtectionDomain};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use tracing::{debug, instrument};

#[derive(Clone)]
pub struct PooledProvider {
    pd: Arc<ProtectionDomain>,
    pools: Arc<Mutex<HashMap<usize, VecDeque<MemoryRegion>>>>,
}

impl PooledProvider {
    #[instrument(skip(self), name = "PooledProvider::preallocate", ret, err)]
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

impl Provider for PooledProvider {
    fn new(pd: Arc<ProtectionDomain>) -> io::Result<Self> {
        Ok(Self {
            pd,
            pools: Arc::new(Mutex::new(HashMap::new())),
        })
    }

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
            debug!("Returning memory region to pool");
            let mr = mr.cast::<u8>();
            let mut pools = pools.lock().unwrap();
            let pool = pools.entry(size).or_insert_with(VecDeque::new);
            pool.push_back(mr);
        };

        Ok(MemoryHandle::new(mr, cleanup))
    }
}
