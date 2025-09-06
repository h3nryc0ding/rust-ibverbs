use crate::memory::{ManagedMemoryRegion, MemoryProvider};
use ibverbs::MemoryRegion;
use std::io;
use std::sync::{Arc, Mutex, mpsc};

pub struct PoolProvider {
    tx: mpsc::Sender<MemoryRegion>,
    rx: Arc<Mutex<mpsc::Receiver<MemoryRegion>>>,
}

impl PoolProvider {
    pub fn new(pool: Vec<MemoryRegion>) -> Self {
        let (tx, rx) = mpsc::channel();
        for mr in pool {
            tx.send(mr).unwrap();
        }
        PoolProvider {
            tx,
            rx: Arc::new(Mutex::new(rx)),
        }
    }
}

impl MemoryProvider for PoolProvider {
    fn allocate(&self, _size: usize) -> io::Result<ManagedMemoryRegion> {
        match self.rx.lock().unwrap().recv() {
            Ok(mr) => Ok(ManagedMemoryRegion {
                mr: Some(mr),
                provider: self,
            }),
            Err(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    fn deallocate(&self, mr: MemoryRegion) -> io::Result<()> {
        self.tx
            .send(mr)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
    }
}
