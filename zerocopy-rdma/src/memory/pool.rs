use crate::memory::{Handle, Provider, Releaser};
use ibverbs::{MemoryRegion, ProtectionDomain};
use std::io;
use std::sync::Arc;
use tokio::sync::{Mutex, mpsc};

struct PoolReleaser<D> {
    tx: mpsc::Sender<MemoryRegion<D>>,
}

impl<D> Releaser<D> for PoolReleaser<D> {
    fn release(&self, mr: MemoryRegion<D>) {
        let _ = self.tx.try_send(mr).unwrap();
    }
}

pub struct PoolProvider<D> {
    tx: mpsc::Sender<MemoryRegion<D>>,
    rx: Arc<Mutex<mpsc::Receiver<MemoryRegion<D>>>>,
}

impl<D: Default> PoolProvider<D> {
    pub fn new(pd: &ProtectionDomain, size: usize) -> io::Result<Self> {
        let (tx, rx) = mpsc::channel(size);
        for _ in 0..size {
            let mr = pd.allocate()?;
            let _ = tx.try_send(mr).unwrap();
        }
        Ok(Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
        })
    }
}

impl<D> Clone for PoolProvider<D> {
    fn clone(&self) -> Self {
        PoolProvider {
            tx: self.tx.clone(),
            rx: self.rx.clone(),
        }
    }
}

impl<D: 'static> Provider<D> for PoolProvider<D> {
    async fn acquire_mr(&mut self) -> io::Result<Handle<D>> {
        match self.rx.lock().await.recv().await {
            Some(mr) => Ok(Handle {
                mr: Some(mr),
                release: Box::new(PoolReleaser {
                    tx: self.tx.clone(),
                }),
            }),
            None => Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "Memory region pool channel closed",
            )),
        }
    }
}
