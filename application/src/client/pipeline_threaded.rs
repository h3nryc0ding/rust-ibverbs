use crate::OPTIMAL_MR_SIZE;
use crate::client::{BaseThreadedMultiQPClient, Client};
use ibverbs::{
    BorrowedMemoryRegion, Context, MemoryRegion, RemoteMemorySlice, ibv_wc,
};
use std::collections::HashMap;
use std::net::ToSocketAddrs;
use std::sync::mpsc::{Receiver, Sender, TryRecvError};
use std::sync::{Arc, Mutex, mpsc};
use std::{hint, io, thread};
use tracing::trace;

const THREADS_REGISTRATION: usize = 2;
const THREADS_DEREGISTRATION: usize = 1;

pub struct PipelineThreadedClient {
    base: BaseThreadedMultiQPClient,

    reg_tx: Sender<RegistrationRequest>,
    post_rx: Receiver<PostRequest>,
    dereg_tx: Sender<DeregistrationRequest>,
}

impl Client for PipelineThreadedClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseThreadedMultiQPClient::new::<3>(ctx, addr)?;

        let (reg_tx, reg_rx) = mpsc::channel();
        let (post_tx, post_rx) = mpsc::channel();
        let (dereg_tx, dereg_rx) = mpsc::channel();

        let reg_rx = Arc::new(Mutex::new(reg_rx));
        for _ in 0..THREADS_REGISTRATION {
            let pd = base.pd.clone();
            let reg_rx = reg_rx.clone();
            let post_tx = post_tx.clone();
            thread::spawn(move || {
                while let Ok(request) = {
                    let reg_rx = reg_rx.lock().unwrap();
                    reg_rx.recv()
                } {
                    let RegistrationRequest { src, remote } = request;
                    trace!("Registering MR");
                    let mr =
                        unsafe { pd.register_unchecked(src as *mut u8, OPTIMAL_MR_SIZE) }.unwrap();
                    trace!("MR registered");
                    let request = PostRequest { mr, remote };
                    post_tx.send(request).unwrap();
                }
            });
        }

        let dereg_rx = Arc::new(Mutex::new(dereg_rx));
        for _ in 0..THREADS_DEREGISTRATION {
            let dereg_rx = dereg_rx.clone();
            thread::spawn(move || {
                while let Ok(request) = {
                    let dereg_rx = dereg_rx.lock().unwrap();
                    dereg_rx.recv()
                } {
                    let DeregistrationRequest { mr } = request;
                    trace!("Deregistering MR");
                    mr.deregister().unwrap();
                    trace!("MR deregistered");
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

    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        let result = Arc::new(vec![0u8; size]);
        let chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
        let mut completions = vec![ibv_wc::default(); chunks];

        let mut posted = 0u64;
        let mut finished = 0;
        let mut outstanding = HashMap::new();
        for chunk in 0..chunks {
            let src = result[chunk * OPTIMAL_MR_SIZE..].as_ptr();
            let remote = self
                .base
                .remote
                .slice(chunk * OPTIMAL_MR_SIZE..(chunk + 1) * OPTIMAL_MR_SIZE);
            self.reg_tx
                .send(RegistrationRequest { src, remote })
                .unwrap();
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
                        Err(e) => {
                            return Err(io::Error::new(
                                io::ErrorKind::Other,
                                format!("registration thread disconnected: {e}"),
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
                    self.dereg_tx.send(DeregistrationRequest { mr }).unwrap();
                }
            }
        }

        let result = Arc::try_unwrap(result).unwrap();
        Ok(result.into_boxed_slice())
    }
}

struct RegistrationRequest {
    src: *const u8,
    remote: RemoteMemorySlice,
}

struct PostRequest {
    mr: BorrowedMemoryRegion<'static>,
    remote: RemoteMemorySlice,
}

struct DeregistrationRequest {
    mr: BorrowedMemoryRegion<'static>,
}

unsafe impl Send for RegistrationRequest {}
unsafe impl Sync for RegistrationRequest {}
