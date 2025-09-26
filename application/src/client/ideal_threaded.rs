use crate::client::{BaseClient, Client};
use crate::{OPTIMAL_MR_SIZE, OPTIMAL_QP_COUNT};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::mpsc::{Receiver, RecvError, Sender, TryRecvError, channel};
use std::thread::JoinHandle;
use std::{hint, io, thread};
use tracing::trace;

const RX_DEPTH: usize = 128;

pub struct IdealThreadedClient {
    poster_handle: Option<JoinHandle<()>>,
    completion_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,

    post_tx: Sender<usize>,
    finished_rx: Receiver<()>,
}

impl Client for IdealThreadedClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base: BaseClient<OPTIMAL_QP_COUNT> = BaseClient::new(ctx, addr)?;
        let shutdown = Arc::new(AtomicBool::new(false));

        let mut mrs = Vec::with_capacity(RX_DEPTH);
        for _ in 0..RX_DEPTH {
            loop {
                match base.pd.allocate::<u8>(OPTIMAL_MR_SIZE) {
                    Ok(mr) => {
                        mrs.push(mr);
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                    Err(e) => return Err(e),
                }
            }
        }
        let mrs = Arc::new(mrs);

        let (post_tx, post_rx) = channel::<usize>();
        let (free_mr_tx, free_mr_rx) = channel::<usize>();
        let (finished_tx, finished_rx) = channel::<()>();

        for i in 0..RX_DEPTH {
            free_mr_tx.send(i).expect("Failed to populate MR pool");
        }

        let poster_handle = {
            let qps = base.qps;
            let remote = base.remote.clone();
            let mrs = mrs.clone();
            let shutdown = shutdown.clone();

            thread::spawn(move || {
                loop {
                    match post_rx.try_recv() {
                        Ok(chunk) => {
                            let mr_idx = match free_mr_rx.recv() {
                                Ok(idx) => idx,
                                Err(RecvError) => panic!("Failed to receive free MR index"),
                            };

                            let mr = &mrs[mr_idx];
                            let local = mr.slice_local(..);
                            let remote = remote
                                .slice(chunk * OPTIMAL_MR_SIZE..(chunk + 1) * OPTIMAL_MR_SIZE);

                            for i in 0..usize::MAX {
                                match unsafe {
                                    qps[i % qps.len()].post_read(&[local], remote, mr_idx as u64)
                                } {
                                    Ok(_) => break,
                                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                                        trace!("Out of memory when posting RDMA read, retrying...");
                                        hint::spin_loop()
                                    }
                                    Err(e) => panic!("Failed to post RDMA read: {e}"),
                                }
                            }
                        }
                        Err(TryRecvError::Empty) => {
                            if shutdown.load(Ordering::Relaxed) {
                                break;
                            } else {
                                hint::spin_loop();
                            }
                        }
                        Err(TryRecvError::Disconnected) => break,
                    }
                }
            })
        };

        let completion_handle = {
            let cq = base.cq.clone();
            let shutdown = shutdown.clone();

            thread::spawn(move || {
                let mut completions = vec![ibv_wc::default(); RX_DEPTH];

                while !shutdown.load(Ordering::Relaxed) {
                    if let Ok(completed) = cq.poll(&mut completions) {
                        for completion in completed {
                            let mr_idx = completion.wr_id() as usize;
                            if free_mr_tx.send(mr_idx).is_err() || finished_tx.send(()).is_err() {
                                return;
                            }
                        }
                    }
                }
            })
        };

        Ok(Self {
            poster_handle: Some(poster_handle),
            completion_handle: Some(completion_handle),
            shutdown,
            post_tx,
            finished_rx,
        })
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let chunks = (dst.len() + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;

        for chunk in 0..chunks {
            if self.post_tx.send(chunk).is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Poster thread has terminated.",
                ));
            }
        }

        for _ in 0..chunks {
            if self.finished_rx.recv().is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Completion thread has terminated.",
                ));
            }
        }

        Ok(())
    }
}

impl Drop for IdealThreadedClient {
    fn drop(&mut self) {
        self.shutdown.store(true, Ordering::Relaxed);

        if let Some(handle) = self.poster_handle.take() {
            let _ = handle.join();
        }
        if let Some(handle) = self.completion_handle.take() {
            let _ = handle.join();
        }
    }
}
