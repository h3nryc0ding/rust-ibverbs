use ibverbs::{MemoryRegion, ProtectionDomain};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use tokio::io;
use tokio::sync::{Mutex, mpsc};

type MR<D> = MemoryRegion<Vec<D>>;

pub struct BufferGuard<D> {
    mr: Option<MR<D>>,
    tx: mpsc::Sender<MR<D>>,
}

impl<D> BufferGuard<D> {
    pub fn mr(&self) -> &MR<D> {
        self.mr.as_ref().unwrap()
    }
}

impl<D> Drop for BufferGuard<D> {
    fn drop(&mut self) {
        if let Some(mr) = self.mr.take() {
            self.tx.try_send(mr).ok();
        }
    }
}

impl<D> Deref for BufferGuard<D> {
    type Target = [D];

    fn deref(&self) -> &Self::Target {
        self.mr.as_ref().unwrap().inner().as_slice()
    }
}

impl<D> DerefMut for BufferGuard<D> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        self.mr.as_mut().unwrap().inner_mut().as_mut_slice()
    }
}

#[derive(Clone)]
pub struct PoolManager<D: Default, const P_SIZE: usize, const B_SIZE: usize> {
    tx: mpsc::Sender<MR<D>>,
    rx: Arc<Mutex<mpsc::Receiver<MR<D>>>>,
}

impl<D: Default + Clone + Copy, const P_SIZE: usize, const B_SIZE: usize>
    PoolManager<D, P_SIZE, B_SIZE>
{
    pub fn new(pd: &ProtectionDomain) -> io::Result<Self> {
        let (tx, rx) = mpsc::channel(P_SIZE);
        for _ in 0..P_SIZE {
            let data = vec![D::default(); B_SIZE];
            let mr = pd.register(data)?;
            tx.try_send(mr).unwrap();
        }
        Ok(Self {
            tx,
            rx: Arc::new(Mutex::new(rx)),
        })
    }

    pub async fn acquire(&mut self) -> io::Result<BufferGuard<D>> {
        match self.rx.lock().await.recv().await {
            Some(mr) => Ok(BufferGuard {
                mr: Some(mr),
                tx: self.tx.clone(),
            }),
            None => Err(io::Error::new(
                io::ErrorKind::Other,
                "Failed to acquire buffer",
            )),
        }
    }
}
