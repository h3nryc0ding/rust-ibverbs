use crate::client::{
    BaseClient, ClientConfig, NonBlockingClient, RequestHandle, RequestState, decode_wr_id,
    encode_wr_id,
};
use crossbeam::channel;
use crossbeam::channel::Sender;
use ibverbs::{Context, MemoryRegion, RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::{io, thread};

pub struct PipelineThreadedClient {
    id: AtomicUsize,
    remote: RemoteMemorySlice,

    reg_tx: Sender<RegistrationMessage>,

    config: ClientConfig,
    _workers: Vec<JoinHandle<()>>,
}

const CONCURRENT_REGISTRATIONS: usize = 8;
const CONCURRENT_DEREGISTRATIONS: usize = 2;

impl NonBlockingClient for PipelineThreadedClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let mut base = BaseClient::new(ctx, cfg)?;
        let id = AtomicUsize::new(0);

        let (reg_tx, reg_rx) = channel::unbounded();
        let (post_tx, post_rx) = channel::unbounded();
        let (dereg_tx, dereg_rx) = channel::unbounded();

        let mut workers = Vec::new();
        for _ in 0..CONCURRENT_REGISTRATIONS {
            let pd = base.pd.clone();
            let reg_rx = reg_rx.clone();
            let post_tx = post_tx.clone();

            let handle = thread::spawn(move || {
                while let Ok(msg) = reg_rx.recv() {
                    let RegistrationMessage {
                        id,
                        chunk,
                        ptr,
                        len,
                        state,
                        remote,
                    } = msg;
                    let mr = unsafe { pd.register_raw(ptr, len).unwrap() };
                    state.registered_acquired.fetch_add(1, Ordering::Relaxed);
                    let msg = PostMessage {
                        id,
                        chunk,
                        state,
                        mr,
                        remote,
                    };
                    post_tx.send(msg).unwrap()
                }
            });

            workers.push(handle);
        }

        let handle = thread::spawn(move || {
            let mut pending = HashMap::new();
            let mut failed = VecDeque::new();
            let mut completions = [ibv_wc::default(); 16];

            loop {
                if let Some(PostMessage {
                    id,
                    chunk,
                    state,
                    mr,
                    remote,
                }) = failed.pop_front()
                {
                    let local = mr.slice_local(..);
                    let wr_id = encode_wr_id(id, chunk);

                    let mut posted = false;
                    for qp in &mut base.qps {
                        match unsafe { qp.post_read(&[local], remote, wr_id) } {
                            Ok(_) => {
                                posted = true;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                            Err(e) => panic!("{:?}", e),
                        }
                    }
                    if posted {
                        state.posted.fetch_add(1, Ordering::Relaxed);
                        pending.insert(wr_id, Pending { state, mr });
                    } else {
                        failed.push_front(PostMessage {
                            id,
                            chunk,
                            state,
                            mr,
                            remote,
                        });
                    }
                }

                match post_rx.try_recv() {
                    Ok(msg) => failed.push_back(msg),
                    Err(_) => (),
                }

                for completion in base.cq.poll(&mut completions).unwrap() {
                    assert!(completion.is_valid());
                    let wr_id = completion.wr_id();

                    if let Some(Pending { state, mr }) = pending.remove(&wr_id) {
                        let (id, chunk) = decode_wr_id(wr_id);
                        state.received.fetch_add(1, Ordering::Relaxed);
                        let msg = DeregistrationMessage {
                            id,
                            chunk,
                            state,
                            mr,
                        };
                        dereg_tx.send(msg).unwrap();
                    } else {
                        panic!("Unknown WR ID: {wr_id}")
                    }
                }
            }
        });
        workers.push(handle);

        for _ in 0..CONCURRENT_DEREGISTRATIONS {
            let dereg_rx = dereg_rx.clone();

            let handle = thread::spawn(move || {
                while let Ok(msg) = dereg_rx.recv() {
                    let DeregistrationMessage { state, mr, .. } = msg;
                    drop(mr);
                    state.deregistered_copied.fetch_add(1, Ordering::Relaxed);
                }
            });
            workers.push(handle);
        }

        Ok(Self {
            id,
            remote: base.remote,
            reg_tx,
            config: base.cfg,
            _workers: workers,
        })
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<RequestHandle> {
        let chunk_size = self.config.mr_size;

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<_> = dst.chunks_mut(chunk_size).collect();

        let handle = RequestHandle {
            chunks: chunks.len(),
            state: Default::default(),
        };

        for (idx, chunk) in chunks.into_iter().enumerate() {
            self.reg_tx
                .send(RegistrationMessage {
                    id,
                    chunk: idx,
                    state: handle.state.clone(),
                    ptr: chunk.as_mut_ptr(),
                    len: chunk.len(),
                    remote: self.remote.slice(idx * chunk_size..(idx + 1) * chunk_size),
                })
                .unwrap()
        }

        Ok(handle)
    }
}

struct Pending {
    state: Arc<RequestState>,
    mr: MemoryRegion,
}

struct RegistrationMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestState>,
    ptr: *mut u8,
    len: usize,
    remote: RemoteMemorySlice,
}

unsafe impl Send for RegistrationMessage {}

struct PostMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestState>,
    mr: MemoryRegion,
    remote: RemoteMemorySlice,
}

#[allow(dead_code)]
struct DeregistrationMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestState>,
    mr: MemoryRegion,
}
