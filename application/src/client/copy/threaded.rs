use super::lib::{CopyMessage, MRMessage, Pending, PostMessage};
use crate::client::{
    BaseClient, NUMA_NODE, NonBlockingClient, RequestHandle,
    lib::{decode_wr_id, encode_wr_id},
};
use crate::{chunks_mut_exact, pin_thread_to_node};
use bytes::BytesMut;
use crossbeam::channel;
use crossbeam::channel::{Sender, TryRecvError};
use ibverbs::{MemoryRegion, RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{hint, io, thread};
use tracing::trace;

pub struct Config {
    pub mr_size: usize,
    pub mr_count: usize,
    pub concurrency: usize,
}

pub struct Client {
    id: AtomicUsize,
    post_tx: Sender<PostMessage>,

    config: Config,
}

impl Client {
    pub fn new(client: BaseClient, config: Config) -> io::Result<Self> {
        pin_thread_to_node::<NUMA_NODE>()?;

        let id = AtomicUsize::new(0);

        let (mr_tx, mr_rx) = channel::unbounded::<MRMessage>();
        let (post_tx, post_rx) = channel::unbounded();
        let (copy_tx, copy_rx) = channel::unbounded();

        for _ in 0..config.mr_count {
            loop {
                match client.pd.allocate_zeroed(config.mr_size) {
                    Ok(mr) => {
                        mr_tx.send(MRMessage { 0: mr }).unwrap();
                        break;
                    }
                    Err(e) => panic!("{:?}", e),
                }
            }
        }

        thread::spawn(move || {
            let mut pending = HashMap::new();
            let mut completions = [ibv_wc::default(); 16];
            let mut outstanding = VecDeque::new();
            let mut mrs: VecDeque<MemoryRegion> = VecDeque::with_capacity(config.mr_count);

            loop {
                match mr_rx.try_recv() {
                    Ok(msg) => {
                        trace!(message = ?msg,operation = "try_recv",channel = "mr");
                        mrs.push_back(msg.0)
                    }
                    Err(TryRecvError::Disconnected) => break,
                    _ => {}
                }
                match post_rx.try_recv() {
                    Ok(msg) => {
                        trace!(message = ?msg,operation = "try_recv",channel = "post");
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
                        remote,
                        bytes,
                    } = outstanding.pop_front().unwrap();

                    let local = mr.slice_local(..bytes.len()).collect::<Vec<_>>();
                    let wr_id = encode_wr_id(id, chunk);

                    let mut posted = false;
                    for qp in &client.qps {
                        match unsafe { qp.post_read(&local, remote, wr_id) } {
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
                            remote,
                            bytes,
                        })
                    } else {
                        state.progress.posted.fetch_add(1, Ordering::Relaxed);
                        pending.insert(wr_id, Pending { state, mr, bytes });
                    }
                }

                for completion in client.cq.poll(&mut completions).unwrap() {
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
                        trace!(message = ?msg, operation = "send", channel = "copy");
                        copy_tx.send(msg).unwrap()
                    }
                }
            }
        });

        for _ in 0..config.concurrency {
            let copy_rx = copy_rx.clone();
            let mr_tx = mr_tx.clone();
            thread::spawn(move || {
                pin_thread_to_node::<NUMA_NODE>().unwrap();

                while let Ok(msg) = copy_rx.recv() {
                    trace!(message = ?msg, operation = "recv", channel = "copy");

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
                    trace!(message = ?msg, operation = "send", channel = "mr");
                    mr_tx.send(msg).unwrap();
                }
            });
        }

        Ok(Self {
            id,
            post_tx,
            config,
        })
    }
}

impl NonBlockingClient for Client {
    fn prefetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<RequestHandle> {
        assert_eq!(bytes.len(), remote.len());
        let mr_size = self.config.mr_size;

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<_> = chunks_mut_exact(bytes, mr_size).collect();

        let handle = RequestHandle::new(chunks.len());

        for (chunk, bytes) in chunks.into_iter().enumerate() {
            let msg = PostMessage {
                id,
                chunk,
                state: handle.core.clone(),
                remote: remote.slice(chunk * mr_size..chunk * mr_size + bytes.len()),
                bytes,
            };
            trace!(message = ?msg, operation = "send", channel = "post");
            self.post_tx.send(msg).unwrap()
        }

        Ok(handle)
    }
}
