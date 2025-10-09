use super::lib::{DeregistrationMessage, PostMessage, RegistrationMessage};
use crate::chunks_mut_exact;
use crate::client::{
    AsyncClient, BaseClient, RequestHandle,
    lib::{decode_wr_id, encode_wr_id},
};
use bytes::BytesMut;
use ibverbs::{RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task;
use tracing::trace;

pub struct Config {
    pub chunk_size: usize,
}

pub struct Client {
    id: AtomicUsize,
    reg_tx: UnboundedSender<RegistrationMessage>,

    config: Config,
}

impl AsyncClient for Client {
    type Config = Config;

    async fn new(client: BaseClient, config: Config) -> io::Result<Self> {
        let id = AtomicUsize::new(0);

        let (reg_tx, mut reg_rx) = mpsc::unbounded_channel();
        let (post_tx, mut post_rx) = mpsc::unbounded_channel();
        let (dereg_tx, mut dereg_rx) = mpsc::unbounded_channel();

        let pd = Arc::new(client.pd);
        task::spawn(async move {
            while let Some(msg) = reg_rx.recv().await {
                let pd = pd.clone();
                let post_tx = post_tx.clone();
                task::spawn_blocking(move || {
                    trace!(message = ?msg, operation = "recv", channel = "reg");
                    let RegistrationMessage {
                        id,
                        chunk,
                        state,
                        remote,
                        bytes,
                    } = msg;
                    let mr = pd.register(bytes).unwrap();
                    state
                        .progress
                        .registered_acquired
                        .fetch_add(1, Ordering::Relaxed);
                    let msg = PostMessage {
                        id,
                        chunk,
                        state,
                        mr,
                        remote,
                    };
                    trace!(message = ?msg, operation = "send", channel = "post");
                    post_tx.send(msg).unwrap();
                });
            }
        });

        task::spawn_blocking(move || {
            let mut pending = HashMap::new();
            let mut waiting = VecDeque::new();
            let mut completions = [ibv_wc::default(); 16];

            loop {
                if let Some(PostMessage {
                    id,
                    chunk,
                    state,
                    mr,
                    remote,
                }) = waiting.pop_front()
                {
                    let local = mr.slice_local(..).collect::<Vec<_>>();
                    let wr_id = encode_wr_id(id, chunk);

                    let mut posted = false;
                    for qp in &client.qps {
                        match unsafe { qp.post_read(&local, remote, wr_id) } {
                            Ok(_) => {
                                posted = true;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                            Err(e) => panic!("{:?}", e),
                        }
                    }
                    if posted {
                        state.progress.posted.fetch_add(1, Ordering::Relaxed);
                        pending.insert(wr_id, (state, mr));
                    } else {
                        waiting.push_front(PostMessage {
                            id,
                            chunk,
                            state,
                            mr,
                            remote,
                        });
                    }
                }

                match post_rx.try_recv() {
                    Ok(msg) => {
                        trace!(message = ?msg,operation = "try_recv",channel = "post");
                        waiting.push_back(msg)
                    }
                    Err(TryRecvError::Disconnected) => return,
                    _ => {}
                }

                for completion in client.cq.poll(&mut completions).unwrap() {
                    assert!(completion.is_valid());
                    let wr_id = completion.wr_id();

                    if let Some((state, mr)) = pending.remove(&wr_id) {
                        let (id, chunk) = decode_wr_id(wr_id);
                        state.progress.received.fetch_add(1, Ordering::Relaxed);
                        let msg = DeregistrationMessage {
                            id,
                            chunk,
                            state,
                            mr,
                        };
                        trace!(message = ?msg, operation = "send", channel = "dereg");
                        dereg_tx.send(msg).unwrap();
                    } else {
                        panic!("Unknown WR ID: {wr_id}")
                    }
                }
            }
        });

        task::spawn(async move {
            while let Some(msg) = dereg_rx.recv().await {
                task::spawn_blocking(move || {
                    trace!(message = ?msg, operation = "recv", channel = "dereg");
                    let DeregistrationMessage {
                        chunk, state, mr, ..
                    } = msg;
                    let bytes = mr.deregister().unwrap();
                    state.aggregator.bytes.insert(chunk, bytes);
                    state
                        .progress
                        .deregistered_copied
                        .fetch_add(1, Ordering::Relaxed);
                });
            }
        });

        Ok(Self { id, reg_tx, config })
    }

    async fn prefetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<BytesMut> {
        assert_eq!(bytes.len(), remote.len());
        let chunk_size = self.config.chunk_size;

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks = chunks_mut_exact(bytes, chunk_size).collect::<Vec<_>>();

        let handle = RequestHandle::new(chunks.len());

        for (chunk, bytes) in chunks.into_iter().enumerate() {
            let msg = RegistrationMessage {
                id,
                chunk,
                state: handle.core.clone(),
                remote: remote.slice(chunk * chunk_size..chunk * chunk_size + bytes.len()),
                bytes,
            };
            trace!(message = ?msg, operation = "send", channel = "reg");
            self.reg_tx.send(msg).unwrap()
        }

        handle.wait() // TODO: no fire and forget
    }
}
