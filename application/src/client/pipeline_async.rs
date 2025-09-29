use crate::client::{AsyncClient, BaseClient, ClientConfig};
use ibverbs::{Context, MemoryRegion, RemoteMemorySlice, ibv_wc};
use std::collections::HashMap;
use std::sync::Arc;
use std::{hint, io};
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender, error::TryRecvError};
use tokio::sync::{Semaphore, mpsc};
use tokio::task;
use tracing::trace;

const CONCURRENT_REGISTRATIONS: usize = 4;
const CONCURRENT_DEREGISTRATIONS: usize = 2;

pub struct PipelineAsyncClient {
    base: BaseClient,

    reg_tx: UnboundedSender<RegistrationRequest>,
    post_rx: UnboundedReceiver<PostRequest>,
    dereg_tx: UnboundedSender<DeregistrationRequest>,
}

impl AsyncClient for PipelineAsyncClient {
    async fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let base = BaseClient::new(ctx, cfg)?;

        let (reg_tx, mut reg_rx) = mpsc::unbounded_channel();
        let (post_tx, post_rx) = mpsc::unbounded_channel();
        let (dereg_tx, mut dereg_rx) = mpsc::unbounded_channel();

        let reg_sem = Arc::new(Semaphore::new(CONCURRENT_REGISTRATIONS));
        {
            let pd = base.pd.clone();
            task::spawn(async move {
                while let Some(request) = reg_rx.recv().await {
                    let permit = reg_sem.clone().acquire_owned().await.unwrap();
                    let pd = pd.clone();
                    let post_tx = post_tx.clone();

                    task::spawn_blocking(move || {
                        let RegistrationRequest { src, remote } = request;
                        trace!("Registering MR");
                        let mr = unsafe {
                            pd.register_raw(src as *mut u8, base.cfg.mr_size)
                                .unwrap()
                        };
                        trace!("MR registered");
                        let request = PostRequest { mr, remote };
                        post_tx.send(request).unwrap();
                        drop(permit);
                    });
                }
            });
        }

        let dereg_sem = Arc::new(Semaphore::new(CONCURRENT_DEREGISTRATIONS));
        {
            task::spawn(async move {
                while let Some(request) = dereg_rx.recv().await {
                    let permit = dereg_sem.clone().acquire_owned().await.unwrap();
                    task::spawn_blocking(move || {
                        let DeregistrationRequest { mr } = request;
                        trace!("Deregistering MR");
                        drop(mr);
                        trace!("MR deregistered");
                        drop(permit);
                    });
                }
            });
        }

        Ok(Self {
            base,
            reg_tx,
            post_rx,
            dereg_tx,
        })
    }

    async fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let mr_size = self.base.cfg.mr_size;
        let chunks = (dst.len() + mr_size - 1) / mr_size;
        let mut completions = vec![ibv_wc::default(); chunks];

        let mut posted = 0u64;
        let mut finished = 0;
        let mut outstanding = HashMap::new();

        for chunk in 0..chunks {
            let src = dst[chunk * mr_size..].as_ptr() as usize;
            let remote = self
                .base
                .remote
                .slice(chunk * mr_size..(chunk + 1) * mr_size);

            self.reg_tx
                .send(RegistrationRequest { src, remote })
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::BrokenPipe, "registration channel closed")
                })?;
        }

        let mut retries = Vec::new();
        while finished < chunks {
            while posted < chunks as u64 {
                let req = if let Some(req) = retries.pop() {
                    req
                } else {
                    match self.post_rx.try_recv() {
                        Ok(req) => req,
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            return Err(io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "registration channel disconnected",
                            ));
                        }
                    }
                };

                let mr = req.mr;
                let remote = req.remote;
                let mut success = false;
                for qp in &mut self.base.qps {
                    match unsafe { qp.post_read(&[mr.slice_local(..)], remote, posted) } {
                        Ok(_) => {
                            success = true;
                            break;
                        }
                        Err(e) if e.kind() == io::ErrorKind::OutOfMemory => hint::spin_loop(),
                        Err(e) => return Err(e),
                    }
                }

                if success {
                    trace!("chunk {posted} posted");
                    outstanding.insert(posted, mr);
                    posted += 1;
                } else {
                    trace!("all QPs are busy");
                    retries.push(PostRequest { mr, remote });
                    break;
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                let wr_id = completion.wr_id();
                trace!("completion received: wr_id={wr_id}");
                assert!(completion.is_valid());

                if let Some(mr) = outstanding.remove(&wr_id) {
                    finished += 1;
                    self.dereg_tx
                        .send(DeregistrationRequest { mr })
                        .map_err(|_| {
                            io::Error::new(
                                io::ErrorKind::BrokenPipe,
                                "deregistration channel closed",
                            )
                        })?;
                }
            }

            if finished < chunks {
                task::yield_now().await;
            }
        }

        Ok(())
    }
}

struct RegistrationRequest {
    src: usize, // should be *const u8 but it's not Send/Sync
    remote: RemoteMemorySlice,
}

struct PostRequest {
    mr: MemoryRegion,
    remote: RemoteMemorySlice,
}

struct DeregistrationRequest {
    mr: MemoryRegion,
}
