use super::lib::{CopyMessage, Handle, MRMessage, Pending, PostMessage};
use crate::chunks_mut_exact;
use crate::client::{
    AsyncClient, BaseClient, RequestHandle,
    lib::{decode_wr_id, encode_wr_id},
};
use bytes::BytesMut;
use ibverbs::{MemoryRegion, RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{hint, io};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task;
use tracing::trace;

#[cfg(feature = "hwlocality")]
use crate::client::NUMA_NODE;
#[cfg(feature = "hwlocality")]
use crate::hwlocality::pin_thread_to_node;

#[derive(Eq, PartialEq)]
pub struct Config {
    pub mr_size: usize,
    pub mr_count: usize,
}

pub struct Client {
    id: AtomicUsize,
    post_tx: UnboundedSender<PostMessage>,

    config: Config,
}

impl AsyncClient for Client {
    type Config = Config;

    async fn new(client: BaseClient, config: Config) -> io::Result<Self> {
        #[cfg(feature = "hwlocality")]
        pin_thread_to_node::<NUMA_NODE>()?;

        let id = AtomicUsize::new(0);

        let (mr_tx, mut mr_rx) = mpsc::unbounded_channel::<MRMessage>();
        let (post_tx, mut post_rx) = mpsc::unbounded_channel();
        let (copy_tx, mut copy_rx) = mpsc::unbounded_channel();

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

        task::spawn_blocking(move || {
            let mut pending = HashMap::new();
            let mut completions = [ibv_wc::default(); 16];
            let mut outstanding = VecDeque::new();
            let mut mrs: Vec<MemoryRegion> = Vec::with_capacity(config.mr_count);

            loop {
                match mr_rx.try_recv() {
                    Ok(msg) => {
                        trace!(message = ?msg,operation = "try_recv",channel = "mr");
                        mrs.push(msg.0)
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
                    let mr = mrs.pop().unwrap();
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
                        mrs.push(mr);
                        outstanding.push_front(PostMessage {
                            id,
                            chunk,
                            state,
                            remote,
                            bytes,
                        })
                    } else {
                        state.posted.fetch_add(1, Ordering::Relaxed);
                        pending.insert(wr_id, Pending { state, mr, bytes });
                    }
                }

                for completion in client.cq.poll(&mut completions).unwrap() {
                    assert!(completion.is_valid());
                    let wr_id = completion.wr_id();

                    if let Some(Pending { state, mr, bytes }) = pending.remove(&wr_id) {
                        let (id, chunk) = decode_wr_id(wr_id);
                        state.received.fetch_add(1, Ordering::Relaxed);

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

        task::spawn(async move {
            while let Some(msg) = copy_rx.recv().await {
                let mr_tx = mr_tx.clone();
                trace!(message = ?msg, operation = "recv", channel = "copy");
                task::spawn_blocking(move || {
                    #[cfg(feature = "hwlocality")]
                    pin_thread_to_node::<NUMA_NODE>().unwrap();

                    let CopyMessage {
                        chunk,
                        state,
                        mr,
                        mut bytes,
                        ..
                    } = msg;
                    let dst_slice = bytes.as_mut();
                    let src_slice = mr.as_slice()[..dst_slice.len()].as_ref();
                    dst_slice.copy_from_slice(src_slice);

                    state.bytes.insert(chunk, bytes);
                    state.copied.fetch_add(1, Ordering::Relaxed);

                    let msg = MRMessage { 0: mr };
                    trace!(message = ?msg, operation = "send", channel = "mr");
                    mr_tx.send(msg).unwrap();
                });
            }
        });

        Ok(Self {
            id,
            post_tx,
            config,
        })
    }

    async fn prefetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<BytesMut> {
        assert_eq!(bytes.len(), remote.len());
        let chunk_size = self.config.mr_size.min(bytes.len());

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<_> = chunks_mut_exact(bytes, chunk_size).collect();

        let handle = Handle::new(chunks.len());

        for (chunk, bytes) in chunks.into_iter().enumerate() {
            let msg = PostMessage {
                id,
                chunk,
                state: handle.state.clone(),
                remote: remote.slice(chunk * chunk_size..chunk * chunk_size + bytes.len()),
                bytes,
            };
            trace!(message = ?msg, operation = "send", channel = "post");
            self.post_tx.send(msg).unwrap()
        }

        task::spawn_blocking(move || handle.acquire()).await?
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }
}
