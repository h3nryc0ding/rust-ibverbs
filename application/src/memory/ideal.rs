use std::sync::Arc;
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use ibverbs::ProtectionDomain;
use crate::memory::pooled::PooledProvider;
use crate::memory::{MemoryHandle, Provider};
use crate::{REQUEST_COUNT, REQUEST_SIZE_MAX, REQUEST_SIZE_SEED};

#[derive(Clone)]
pub struct IdealProvider(PooledProvider);

impl Provider for IdealProvider {
    fn new(pd: Arc<ProtectionDomain>) -> std::io::Result<Self> {
        let pooled = PooledProvider::new(pd)?;

        let mut rng = ChaCha8Rng::seed_from_u64(REQUEST_SIZE_SEED);
        for _ in 0..REQUEST_COUNT {
            let size = rng.random_range(0..REQUEST_SIZE_MAX);
            pooled.preallocate::<u8>(size, 1)?;
        }

        // Request
        pooled.preallocate::<u8>(1, 1)?;
        // send_recv::ServerMeta
        pooled.preallocate::<u64>(1, 1)?;
        // send_recv_read::ServerMeta
        pooled.preallocate::<u64>(2, 1)?;


        Ok(Self(pooled))
    }

    fn allocate<T: 'static>(&self, count: usize) -> std::io::Result<MemoryHandle<T>> {
        self.0.allocate(count)
    }
}