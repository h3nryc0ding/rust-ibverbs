use super::lib::{DeregistrationMessage, Handle, Pending, PostMessage, RegistrationMessage};
use crate::client::lib::{decode_wr_id, encode_wr_id};
use crate::{chunks_mut_exact, client};
use bytes::BytesMut;
use crossbeam::channel;
use crossbeam::channel::{Sender, TryRecvError};
use ibverbs::{RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{io, thread};
use tracing::trace;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct Config {
    pub chunk_size: usize,
    pub concurrency_reg: usize,
    pub concurrency_dereg: usize,
}

pub struct Client {
    id: AtomicUsize,
    reg_tx: Sender<RegistrationMessage>,

    config: Config,
}

impl client::Client for Client {
    type Config = Config;
    fn config(&self) -> &Self::Config {
        &self.config
    }
}

impl client::NonBlockingClient for Client {
    type Handle = Handle;

    fn new(client: client::BaseClient, config: Config) -> io::Result<Self> {
        let id = AtomicUsize::new(0);

        let (reg_tx, reg_rx) = channel::unbounded();
        let (post_tx, post_rx) = channel::unbounded();
        let (dereg_tx, dereg_rx) = channel::unbounded();

        for _ in 0..config.concurrency_reg {
            let pd = client.pd.clone();
            let reg_rx = reg_rx.clone();
            let post_tx = post_tx.clone();

            thread::spawn(move || {
                while let Ok(msg) = reg_rx.recv() {
                    trace!(message = ?msg, operation = "recv", channel = "reg");
                    let RegistrationMessage {
                        id,
                        chunk,
                        state,
                        remote,
                        bytes,
                    } = msg;
                    let mr = pd.register(bytes).unwrap();
                    state.registered.fetch_add(1, Ordering::Relaxed);
                    let msg = PostMessage {
                        id,
                        chunk,
                        state,
                        mr,
                        remote,
                    };
                    trace!(message = ?msg, operation = "send", channel = "post");
                    post_tx.send(msg).unwrap()
                }
            });
        }

        thread::spawn(move || {
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
                        state.posted.fetch_add(1, Ordering::Relaxed);
                        pending.insert(wr_id, Pending { state, mr });
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
                    Err(TryRecvError::Disconnected) if pending.is_empty() => return,
                    _ => {}
                }

                for completion in client.cq.poll(&mut completions).unwrap() {
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
                        trace!(message = ?msg, operation = "send", channel = "dereg");
                        dereg_tx.send(msg).unwrap();
                    }
                }
            }
        });

        for _ in 0..config.concurrency_dereg {
            let dereg_rx = dereg_rx.clone();

            thread::spawn(move || {
                while let Ok(msg) = dereg_rx.recv() {
                    trace!(message = ?msg, operation = "recv", channel = "dereg");
                    let DeregistrationMessage {
                        chunk, state, mr, ..
                    } = msg;
                    let bytes = mr.deregister().unwrap();
                    state.bytes.insert(chunk, bytes);
                    state.deregistered.fetch_add(1, Ordering::Relaxed);
                }
            });
        }

        Ok(Self { id, reg_tx, config })
    }

    fn prefetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<Self::Handle> {
        assert_eq!(bytes.len(), remote.len());
        let chunk_size = self.config.chunk_size.min(bytes.len());

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<_> = chunks_mut_exact(bytes, chunk_size).collect();

        let handle = Handle::new(chunks.len());

        for (chunk, bytes) in chunks.into_iter().enumerate() {
            let msg = RegistrationMessage {
                id,
                chunk,
                state: handle.state.clone(),
                remote: remote.slice(chunk * chunk_size..chunk * chunk_size + bytes.len()),
                bytes,
            };
            trace!(message = ?msg, operation = "send", channel = "reg");
            self.reg_tx.send(msg).unwrap()
        }

        Ok(handle)
    }
}
