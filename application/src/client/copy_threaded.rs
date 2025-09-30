use crate::client::{
    BaseClient, ClientConfig, NonBlockingClient, RequestHandle, RequestState, decode_wr_id,
    encode_wr_id,
};
use crate::{NUMA_NODE, pin_thread_to_node};
use crossbeam::channel;
use crossbeam::channel::{Sender, TryRecvError};
use ibverbs::{Context, MemoryRegion, RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{io, slice, thread};

const PRE_ALLOCATIONS: usize = 64;
const CONCURRENT_COPIES: usize = 8;

pub struct CopyThreadedClient {
    id: AtomicUsize,
    remote: RemoteMemorySlice,

    post_tx: Sender<PostMessage>,

    config: ClientConfig,
}

impl NonBlockingClient for CopyThreadedClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        pin_thread_to_node::<NUMA_NODE>()?;

        let mut base = BaseClient::new(ctx, cfg)?;
        let id = AtomicUsize::new(0);

        let (mr_tx, mr_rx) = channel::unbounded();
        let (post_tx, post_rx) = channel::unbounded();
        let (copy_tx, copy_rx) = channel::unbounded();

        thread::spawn(move || {
            let mut pending = HashMap::new();
            let mut completions = [ibv_wc::default(); 16];
            let mut outstanding = VecDeque::new();
            let mut mrs = VecDeque::with_capacity(PRE_ALLOCATIONS);

            loop {
                match mr_rx.try_recv() {
                    Ok(MRMessage { mr }) => mrs.push_back(mr),
                    Err(TryRecvError::Disconnected) => break,
                    _ => {}
                }
                match post_rx.try_recv() {
                    Ok(msg) => outstanding.push_back(msg),
                    Err(TryRecvError::Disconnected) => break,
                    _ => {}
                }

                match (mrs.pop_front(), outstanding.pop_front()) {
                    (Some(mr), None) => mrs.push_front(mr),
                    (None, Some(msg)) => outstanding.push_front(msg),
                    (
                        Some(mr),
                        Some(PostMessage {
                            id,
                            chunk,
                            state,
                            ptr,
                            len,
                            remote,
                        }),
                    ) => {
                        state.registered_acquired.fetch_add(1, Ordering::Relaxed);
                        let local = mr.slice_local(..);
                        let wr_id = encode_wr_id(id, chunk);

                        let mut posted = false;
                        for qp in &mut base.qps {
                            match unsafe { qp.post_read(&[local], remote, wr_id) } {
                                Ok(_) => {
                                    posted = true;
                                    break;
                                }
                                Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {}
                                Err(e) => panic!("{:?}", e),
                            }
                        }
                        if posted {
                            state.posted.fetch_add(1, Ordering::Relaxed);
                            pending.insert(
                                wr_id,
                                Pending {
                                    state,
                                    mr,
                                    ptr,
                                    len,
                                },
                            );
                        } else {
                            mrs.push_front(mr);
                            outstanding.push_front(PostMessage {
                                id,
                                chunk,
                                state,
                                ptr,
                                len,
                                remote,
                            });
                        }
                    }
                    _ => {}
                }

                for completion in base.cq.poll(&mut completions).unwrap() {
                    assert!(completion.is_valid());
                    let wr_id = completion.wr_id();

                    if let Some(Pending {
                        state,
                        mr,
                        ptr,
                        len,
                    }) = pending.remove(&wr_id)
                    {
                        let (id, chunk) = decode_wr_id(wr_id);
                        state.received.fetch_add(1, Ordering::Relaxed);
                        let msg = CopyMessage {
                            id,
                            chunk,
                            state,
                            mr,
                            ptr,
                            len,
                        };
                        copy_tx.send(msg).unwrap();
                    } else {
                        panic!("Unknown WR ID: {wr_id}")
                    }
                }
            }
        });

        for _ in 0..CONCURRENT_COPIES {
            let copy_rx = copy_rx.clone();
            let mr_tx = mr_tx.clone();
            thread::spawn(move || {
                pin_thread_to_node::<NUMA_NODE>().unwrap();

                while let Ok(msg) = copy_rx.recv() {
                    let CopyMessage {
                        ptr,
                        len,
                        mr,
                        state,
                        ..
                    } = msg;
                    let src = &mr[..len];
                    let dst = unsafe { slice::from_raw_parts_mut(ptr, len) };
                    dst.copy_from_slice(src);
                    state.deregistered_copied.fetch_add(1, Ordering::Relaxed);

                    let msg = MRMessage { mr };
                    mr_tx.send(msg).unwrap();
                }
            });
        }

        for _ in 0..PRE_ALLOCATIONS {
            loop {
                match base.pd.allocate::<u8>(base.cfg.mr_size) {
                    Ok(mr) => {
                        mr_tx.send(MRMessage { mr }).unwrap();
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                    Err(e) => panic!("{:?}", e),
                }
            }
        }

        Ok(Self {
            id,
            remote: base.remote,
            post_tx,
            config: base.cfg,
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
            self.post_tx
                .send(PostMessage {
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
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for Pending {}
unsafe impl Sync for Pending {}

struct MRMessage {
    mr: MemoryRegion,
}

struct PostMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestState>,
    ptr: *mut u8,
    len: usize,
    remote: RemoteMemorySlice,
}

unsafe impl Send for PostMessage {}

#[allow(dead_code)]
struct CopyMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestState>,
    mr: MemoryRegion,
    ptr: *mut u8,
    len: usize,
}

unsafe impl Send for CopyMessage {}
