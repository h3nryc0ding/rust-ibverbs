use crate::OPTIMAL_MR_SIZE;
use crate::client::{BaseThreadedClient, Client};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::mem::ManuallyDrop;
use std::net::ToSocketAddrs;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{Receiver, RecvError, Sender, TryRecvError, channel};
use std::thread::JoinHandle;
use std::{hint, io, thread};
use tracing::trace;

const RX_DEPTH: usize = 128;

pub struct IdealThreadedAtomicClient {
    poster_handle: Option<JoinHandle<()>>,
    completion_handle: Option<JoinHandle<()>>,

    shutdown: Arc<AtomicBool>,

    posted: Arc<AtomicUsize>,
    completed: Arc<AtomicUsize>,
    target_chunks: Arc<AtomicUsize>,
}

impl Client for IdealThreadedAtomicClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseThreadedClient::new(ctx, addr)?;

        let mut mrs_vec = Vec::with_capacity(RX_DEPTH);
        let pd = base.pd.lock().unwrap();
        for _ in 0..RX_DEPTH {
            loop {
                match pd.allocate::<u8>(OPTIMAL_MR_SIZE) {
                    Ok(mr) => {
                        mrs_vec.push(mr);
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                    Err(e) => return Err(e),
                }
            }
        }
        let mrs = Arc::new(mrs_vec);

        let shutdown = Arc::new(AtomicBool::new(false));
        let posted = Arc::new(AtomicUsize::new(0));
        let completed = Arc::new(AtomicUsize::new(0));
        let target_chunks = Arc::new(AtomicUsize::new(0));

        let post_handle = {
            let mrs = mrs.clone();
            let shutdown = shutdown.clone();
            let posted = posted.clone();
            let target_chunks = target_chunks.clone();

            thread::spawn(move || {
                let mut qp = base.qp.lock().unwrap();
                let mut mr_idx = 0;

                while !shutdown.load(Ordering::Relaxed) {
                    let target = target_chunks.load(Ordering::Relaxed);
                    if target == 0 {
                        hint::spin_loop();
                        continue;
                    }

                    let current_posted = posted.load(Ordering::Relaxed);
                    if current_posted >= target {
                        hint::spin_loop();
                        continue;
                    }

                    let chunk_id = posted.fetch_add(1, Ordering::Relaxed);
                    if chunk_id >= target {
                        continue;
                    }

                    let mr = &mrs[mr_idx % mrs.len()];
                    let local = mr.slice_local(..);
                    let remote_slice = base.remote.slice(0..OPTIMAL_MR_SIZE);

                    loop {
                        if shutdown.load(Ordering::Relaxed) {
                            return;
                        }

                        match unsafe { qp.post_read(&[local], remote_slice, chunk_id as u64) } {
                            Ok(_) => {
                                mr_idx += 1;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                            Err(e) => panic!("{e}"),
                        }
                    }
                }
            })
        };

        let completion_handle = {
            let shutdown = shutdown.clone();
            let completed = completed.clone();
            let target_chunks = target_chunks.clone();

            thread::spawn(move || {
                let cq = base.cq.lock().unwrap();
                let mut completions = vec![ibv_wc::default(); RX_DEPTH];

                while !shutdown.load(Ordering::Relaxed) {
                    let target = target_chunks.load(Ordering::Relaxed);
                    if target == 0 {
                        hint::spin_loop();
                        continue;
                    }

                    if completed.load(Ordering::Relaxed) >= target {
                        hint::spin_loop();
                        continue;
                    }

                    if let Ok(polled) = cq.poll(&mut completions) {
                        if !polled.is_empty() {
                            completed.fetch_add(polled.len(), Ordering::Relaxed);
                        }
                    }
                }
            })
        };

        Ok(Self {
            poster_handle: Some(post_handle),
            completion_handle: Some(completion_handle),
            shutdown,
            posted,
            completed,
            target_chunks,
        })
    }

    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        let result = vec![0u8; size].into_boxed_slice();
        let total_chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;

        self.posted.store(0, Ordering::Relaxed);
        self.completed.store(0, Ordering::Relaxed);
        self.target_chunks.store(total_chunks, Ordering::Relaxed);

        while self.completed.load(Ordering::Relaxed) < total_chunks {
            hint::spin_loop();
        }

        Ok(result)
    }
}

impl Drop for IdealThreadedAtomicClient {
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

pub struct IdealThreadedChannelClient {
    poster_handle: Option<JoinHandle<()>>,
    completion_handle: Option<JoinHandle<()>>,
    shutdown: Arc<AtomicBool>,

    post: Sender<()>,
    finished: Receiver<()>,
}

impl Client for IdealThreadedChannelClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseThreadedClient::new(ctx, addr)?;
        let shutdown = Arc::new(AtomicBool::new(false));

        let mut mrs_vec = Vec::with_capacity(RX_DEPTH);
        let pd = base.pd.lock().unwrap();
        for _ in 0..RX_DEPTH {
            loop {
                match pd.allocate::<u8>(OPTIMAL_MR_SIZE) {
                    Ok(mr) => {
                        mrs_vec.push(mr);
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                    Err(e) => return Err(e),
                }
            }
        }
        let mrs = Arc::new(mrs_vec);

        let (post_tx, post_rx) = channel::<()>();
        let (free_mr_tx, free_mr_rx) = channel::<usize>();
        let (finished_tx, finished_rx) = channel::<()>();

        for i in 0..RX_DEPTH {
            free_mr_tx.send(i).expect("Failed to populate MR pool");
        }

        let poster_handle = {
            let qp = base.qp.clone();
            let remote = base.remote.clone();
            let mrs = mrs.clone();
            let shutdown = shutdown.clone();

            thread::spawn(move || {
                let mut qp = qp.lock().unwrap();

                loop {
                    match post_rx.try_recv() {
                        Ok(_) => {
                            let mr_idx = match free_mr_rx.recv() {
                                Ok(idx) => idx,
                                Err(RecvError) => panic!("Failed to receive free MR index"),
                            };

                            let mr = &mrs[mr_idx];
                            let local = mr.slice_local(..);
                            let remote_slice = remote.slice(0..OPTIMAL_MR_SIZE);

                            loop {
                                match unsafe { qp.post_read(&[local], remote_slice, mr_idx as u64) }
                                {
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
                let cq = cq.lock().unwrap();
                let mut completions = vec![ibv_wc::default(); RX_DEPTH];

                while !shutdown.load(Ordering::Relaxed) {
                    if let Ok(polled) = cq.poll(&mut completions) {
                        if polled.is_empty() {
                            hint::spin_loop();
                            continue;
                        }
                        for wc in polled {
                            let mr_idx = wc.wr_id() as usize;

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
            post: post_tx,
            finished: finished_rx,
        })
    }

    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        let result = vec![0u8; size].into_boxed_slice();
        let total_chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;

        for _ in 0..total_chunks {
            if self.post.send(()).is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Poster thread has terminated.",
                ));
            }
        }

        for _ in 0..total_chunks {
            if self.finished.recv().is_err() {
                return Err(io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "Completion thread has terminated.",
                ));
            }
        }

        Ok(result)
    }
}

impl Drop for IdealThreadedChannelClient {
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
