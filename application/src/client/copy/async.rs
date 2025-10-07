use super::lib::{CopyMessage, MRMessage, Pending, PostMessage};
use crate::client::{
    AsyncClient, BaseClient, ClientConfig, RequestHandle, decode_wr_id, encode_wr_id,
};
use crate::{NUMA_NODE, chunks_mut_exact, pin_thread_to_node};
use bytes::BytesMut;
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{hint, io};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task;

const PRE_ALLOCATIONS: usize = 64;

pub struct Client {
    id: AtomicUsize,

    post_tx: UnboundedSender<PostMessage>,

    config: ClientConfig,
}

impl AsyncClient for Client {
    async fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        pin_thread_to_node::<NUMA_NODE>()?;

        let mut base = BaseClient::new(ctx, cfg)?;
        let id = AtomicUsize::new(0);

        let (mr_tx, mut mr_rx) = mpsc::unbounded_channel::<MRMessage>();
        let (post_tx, mut post_rx) = mpsc::unbounded_channel();
        let (copy_tx, mut copy_rx) = mpsc::unbounded_channel();

        task::spawn_blocking(move || {
            let mut pending = HashMap::new();
            let mut completions = [ibv_wc::default(); 16];
            let mut outstanding = VecDeque::new();
            let mut mrs: VecDeque<MemoryRegion> = VecDeque::with_capacity(PRE_ALLOCATIONS);

            loop {
                match mr_rx.try_recv() {
                    Ok(msg) => mrs.push_back(msg.0),
                    Err(TryRecvError::Disconnected) => break,
                    _ => {}
                }
                match post_rx.try_recv() {
                    Ok(msg) => outstanding.push_back(msg),
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
                        copy_tx
                            .send(CopyMessage {
                                id,
                                chunk,
                                state,
                                mr,
                                bytes,
                            })
                            .unwrap()
                    }
                }
            }
        });

        task::spawn(async move {
            while let Some(request) = copy_rx.recv().await {
                let mr_tx = mr_tx.clone();
                task::spawn_blocking(move || {
                    pin_thread_to_node::<NUMA_NODE>().unwrap();

                    let CopyMessage {
                        chunk,
                        state,
                        mr,
                        mut bytes,
                        ..
                    } = request;
                    let src_slice = mr.as_slice();
                    let dst_slice = bytes.as_mut();
                    dst_slice.copy_from_slice(src_slice);
                    state
                        .progress
                        .deregistered_copied
                        .fetch_add(1, Ordering::Relaxed);
                    state.aggregator.bytes.insert(chunk, bytes);

                    mr_tx.send(MRMessage { 0: mr }).unwrap();
                });
            }
        });

        Ok(Self {
            id,
            post_tx,
            config: base.cfg,
        })
    }

    async fn request(&mut self, bytes: BytesMut) -> io::Result<RequestHandle> {
        let chunk_size = self.config.mr_size;
        assert_eq!(bytes.len() % chunk_size, 0);

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<_> = chunks_mut_exact(bytes, chunk_size).collect();

        let handle = RequestHandle::new(chunks.len());

        for (chunk, bytes) in chunks.into_iter().enumerate() {
            self.post_tx
                .send(PostMessage {
                    id,
                    chunk,
                    state: handle.core.clone(),
                    bytes,
                })
                .unwrap()
        }

        Ok(handle)
    }
}
