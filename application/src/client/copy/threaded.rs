use super::lib::{CopyMessage, MRMessage, Pending, PostMessage};
use crate::client::{
    BaseClient, ClientConfig, NonBlockingClient, RequestHandle, decode_wr_id, encode_wr_id,
};
use crate::{NUMA_NODE, chunks_mut_exact, pin_thread_to_node};
use bytes::BytesMut;
use crossbeam::channel;
use crossbeam::channel::{Sender, TryRecvError};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{hint, io, thread};
use tracing::trace;

const PRE_ALLOCATIONS: usize = 64;
const CONCURRENT_COPIES: usize = 8;

pub struct Client {
    id: AtomicUsize,

    post_tx: Sender<PostMessage>,

    config: ClientConfig,
}

impl NonBlockingClient for Client {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        pin_thread_to_node::<NUMA_NODE>()?;

        let mut base = BaseClient::new(ctx, cfg)?;
        let id = AtomicUsize::new(0);

        let (mr_tx, mr_rx) = channel::unbounded::<MRMessage>();
        let (post_tx, post_rx) = channel::unbounded();
        let (copy_tx, copy_rx) = channel::unbounded();

        thread::spawn(move || {
            let mut pending = HashMap::new();
            let mut completions = [ibv_wc::default(); 16];
            let mut outstanding = VecDeque::new();
            let mut mrs: VecDeque<MemoryRegion> = VecDeque::with_capacity(PRE_ALLOCATIONS);

            loop {
                match mr_rx.try_recv() {
                    Ok(msg) => {
                        trace!(
                            message = debug(&msg),
                            operation = "try_recv",
                            channel = "mr"
                        );
                        mrs.push_back(msg.0)
                    }
                    Err(TryRecvError::Disconnected) => break,
                    _ => {}
                }
                match post_rx.try_recv() {
                    Ok(msg) => {
                        trace!(
                            message = debug(&msg),
                            operation = "try_recv",
                            channel = "post"
                        );
                        outstanding.push_back(msg)
                    }
                    Err(TryRecvError::Disconnected) => break,
                    _ => {}
                }

                if !mrs.is_empty() && !outstanding.is_empty() {
                    let mr = mrs.pop_front().unwrap();
                    let PostMessage {
                        id,
                        chunk,
                        state,
                        bytes,
                    } = outstanding.pop_front().unwrap();

                    let local = mr.slice_local(..);
                    let remote = base
                        .remote
                        .slice(chunk * base.cfg.mr_size..(chunk + 1) * base.cfg.mr_size);
                    let wr_id = encode_wr_id(id, chunk);

                    let mut posted = false;
                    for qp in &mut base.qps {
                        match unsafe { qp.post_read(&[local], remote, wr_id) } {
                            Ok(_) => {
                                posted = true;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                                hint::spin_loop();
                            }
                            Err(e) => panic!("{e:?}"),
                        }
                    }
                    if !posted {
                        mrs.push_front(mr);
                        outstanding.push_front(PostMessage {
                            id,
                            chunk,
                            state,
                            bytes,
                        })
                    } else {
                        state.progress.posted.fetch_add(1, Ordering::Relaxed);
                        pending.insert(wr_id, Pending { state, mr, bytes });
                    }
                }

                for completion in base.cq.poll(&mut completions).unwrap() {
                    assert!(completion.is_valid());
                    let wr_id = completion.wr_id();

                    if let Some(Pending { state, mr, bytes }) = pending.remove(&wr_id) {
                        let (id, chunk) = decode_wr_id(wr_id);
                        state.progress.received.fetch_add(1, Ordering::Relaxed);

                        let msg = CopyMessage {
                            id,
                            chunk,
                            state,
                            mr,
                            bytes,
                        };
                        trace!(message = debug(&msg), operation = "send", channel = "copy");
                        copy_tx.send(msg).unwrap()
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
                    trace!(message = debug(&msg), operation = "recv", channel = "copy");

                    let CopyMessage {
                        chunk,
                        state,
                        mr,
                        mut bytes,
                        ..
                    } = msg;
                    let src_slice = mr.as_slice();
                    let dst_slice = bytes.as_mut();
                    dst_slice.copy_from_slice(src_slice);

                    state
                        .progress
                        .deregistered_copied
                        .fetch_add(1, Ordering::Relaxed);
                    state.aggregator.bytes.insert(chunk, bytes);

                    let msg = MRMessage { 0: mr };
                    trace!(message = debug(&msg), operation = "send", channel = "mr");
                    mr_tx.send(msg).unwrap();
                }
            });
        }

        for _ in 0..PRE_ALLOCATIONS {
            loop {
                match base.pd.allocate_zeroed(base.cfg.mr_size) {
                    Ok(mr) => {
                        mr_tx.send(MRMessage { 0: mr }).unwrap();
                        break;
                    }
                    Err(e) => panic!("{:?}", e),
                }
            }
        }

        Ok(Self {
            id,
            post_tx,
            config: base.cfg,
        })
    }

    fn request(&mut self, bytes: BytesMut) -> io::Result<RequestHandle> {
        let chunk_size = self.config.mr_size;
        assert_eq!(bytes.len() % chunk_size, 0);

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<_> = chunks_mut_exact(bytes, chunk_size).collect();

        let handle = RequestHandle::new(chunks.len());

        for (chunk, bytes) in chunks.into_iter().enumerate() {
            let msg = PostMessage {
                id,
                chunk,
                state: handle.core.clone(),
                bytes,
            };
            trace!(message = debug(&msg), operation = "send", channel = "post");
            self.post_tx.send(msg).unwrap()
        }

        Ok(handle)
    }
}
