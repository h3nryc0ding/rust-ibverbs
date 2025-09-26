use crate::client::{BaseClient, Client};
use crate::{OPTIMAL_MR_SIZE, OPTIMAL_QP_COUNT, pin_thread_to_node};
use ibverbs::{Context, MemoryRegion, OwnedMemoryRegion, ibv_wc};
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};
use std::{hint, io, ptr, thread};
use tracing::trace;

const RX_DEPTH: usize = 64;
const COPY_THREADS: usize = 8;

const NUMA_NODE: usize = 1;

pub struct CopyThreadedClient {
    base: BaseClient<OPTIMAL_QP_COUNT>,
    mrs: Arc<Mutex<Vec<OwnedMemoryRegion>>>,

    sender: mpsc::Sender<CopyRequest>,
    finished: Arc<AtomicUsize>,
}

impl Client for CopyThreadedClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseClient::new(ctx, addr)?;

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
        let mrs = Arc::new(Mutex::new(mrs));

        let (tx, rx) = mpsc::channel();
        let rx = Arc::new(Mutex::new(rx));
        let finished = Arc::new(AtomicUsize::new(0));

        for _ in 0..COPY_THREADS {
            let rx = Arc::clone(&rx);
            let mrs = Arc::clone(&mrs);
            let finished = Arc::clone(&finished);
            thread::spawn(move || {
                pin_thread_to_node::<NUMA_NODE>().unwrap();

                while let Ok(request) = {
                    let rx = rx.lock().unwrap();
                    rx.recv()
                } {
                    let CopyRequest { mr, dst } = request;
                    trace!(
                        "copying {} from {:?} into {}",
                        OPTIMAL_MR_SIZE,
                        mr.as_ptr(),
                        dst as usize
                    );
                    let src = mr.as_ptr();
                    unsafe {
                        ptr::copy_nonoverlapping(src, dst, OPTIMAL_MR_SIZE);
                    }
                    {
                        trace!("returning {:?}", mr.as_ptr());
                        let mut mrs = mrs.lock().unwrap();
                        mrs.push(mr);
                    }
                    finished.fetch_add(1, Ordering::SeqCst);
                }
            });
        }

        pin_thread_to_node::<NUMA_NODE>()?;
        Ok(Self {
            base,
            mrs,
            finished,
            sender: tx,
        })
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let chunks = (dst.len() + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
        let mut completions = vec![ibv_wc::default(); RX_DEPTH];
        self.finished.store(0, Ordering::SeqCst);

        let mut posted = 0;
        let mut outstanding = HashMap::new();
        while self.finished.load(Ordering::SeqCst) < chunks {
            while posted < chunks {
                let mut mrs = self.mrs.lock().unwrap();
                if let Some(mr) = mrs.pop() {
                    let local = mr.slice_local(..);

                    let remote_slice = self
                        .base
                        .remote
                        .slice(posted * OPTIMAL_MR_SIZE..(posted + 1) * OPTIMAL_MR_SIZE);

                    let chunk_idx = posted as u64;
                    let mut success = false;
                    for qp in &mut self.base.qps {
                        match unsafe { qp.post_read(&[local], remote_slice, chunk_idx) } {
                            Ok(_) => {
                                success = true;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                                hint::spin_loop();
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    if success {
                        trace!("chunk {posted} posted");
                        outstanding.insert(posted, mr);
                        posted += 1;
                    } else {
                        trace!("all QPs are busy");
                        mrs.push(mr);
                        break;
                    }
                } else {
                    break;
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                let wr_id = completion.wr_id();
                trace!("completion received: wr_id={wr_id}");
                let chunk_idx = wr_id as usize;

                if let Some(mr) = outstanding.remove(&chunk_idx) {
                    let dst = dst[chunk_idx * OPTIMAL_MR_SIZE..].as_ptr() as *mut u8;
                    let request = CopyRequest { mr, dst };
                    self.sender.send(request).unwrap();
                }
            }
        }

        while self.finished.load(Ordering::SeqCst) < chunks {
            hint::spin_loop();
        }

        Ok(())
    }
}

struct CopyRequest {
    mr: OwnedMemoryRegion,
    dst: *mut u8,
}

unsafe impl Send for CopyRequest {}
unsafe impl Sync for CopyRequest {}
