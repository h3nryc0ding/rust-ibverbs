use crate::memory::{MemoryHandle, Provider};
use ibverbs::{MemoryRegion, ProtectionDomain};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::{Arc, Mutex};
use tracing::{debug, info, instrument};

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

    #[instrument(skip(self), name = "PooledProvider::_find")]
    fn _find(&self, size: usize) -> Option<MemoryRegion> {
        let mut pools = self.pools.lock().unwrap();

        if let Some(pool) = pools.get_mut(&size) {
            if let Some(mr) = pool.pop_front() {
                info!("Found exact match in pool for size {}", size);
                return Some(mr);
            }
        }

        let mut candidates: Vec<_> = pools.iter_mut().filter(|&(&s, _)| s >= size).collect();

        candidates.sort_by_key(|&(&size, _)| size);

        for (_, pool) in candidates {
            if let Some(mut mr) = pool.pop_front() {
                mr.set_count(size);
                info!("Found larger match in pool for size {}", size);
                return Some(mr);
            }
        }

        None
    }

    #[instrument(skip(self), name = "PooledProvider::_allocate", err)]
    fn _allocate(&self, size: usize) -> io::Result<MemoryRegion> {
        info!("Allocating new memory region of size {}", size);
        self.pd.allocate::<u8>(size)
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
        let mr = match self._find(size) {
            Some(mr) => mr,
            None => self._allocate(size)?,
        }
        .cast();

        let pools = Arc::clone(&self.pools);
        let cleanup = move |mr: MemoryRegion<T>| {
            debug!("Returning memory region to pool");
            let mut mr = mr.cast::<u8>();
            let size = unsafe { mr.length() };
            mr.set_count(size);
            let mut pools = pools.lock().unwrap();
            let pool = pools.entry(size).or_insert_with(VecDeque::new);
            pool.push_back(mr);
        };

        Ok(MemoryHandle::new(mr, cleanup))
    }
}
