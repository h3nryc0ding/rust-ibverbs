use super::lib::{DeregistrationMessage, Handle, Pending, PostMessage, RegistrationMessage};
use crate::client::{BaseClient, NonBlockingClient};
use bytes::BytesMut;
use crossbeam::channel;
use crossbeam::channel::{Sender, TryRecvError};
use ibverbs::{RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{io, thread};
use tracing::trace;

#[derive(Eq, PartialEq)]
pub struct Config {
    pub concurrency_reg: usize,
    pub concurrency_dereg: usize,
}

pub struct Client {
    id: AtomicUsize,
    reg_tx: Sender<RegistrationMessage>,

    config: Config,
}

impl NonBlockingClient for Client {
    type Config = Config;
    type Handle = Handle;

    fn new(client: BaseClient, config: Config) -> io::Result<Self> {
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
                        state,
                        bytes,
                        remote,
                    } = msg;

                    let mr = pd.register(bytes).unwrap();
                    state.registered.store(true, Ordering::Release);

                    let msg = PostMessage {
                        id,
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
                    state,
                    mr,
                    remote,
                }) = waiting.pop_front()
                {
                    let local = mr.slice_local(..).collect::<Vec<_>>();

                    let mut posted = false;
                    for qp in &client.qps {
                        match unsafe { qp.post_read(&local, remote, id as u64) } {
                            Ok(_) => {
                                posted = true;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                            Err(e) => panic!("{:?}", e),
                        }
                    }
                    if posted {
                        state.posted.store(true, Ordering::Release);
                        pending.insert(id, Pending { state, mr });
                    } else {
                        waiting.push_front(PostMessage {
                            id,
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
                    let id = completion.wr_id() as usize;

                    if let Some(Pending { state, mr }) = pending.remove(&id) {
                        state.received.store(true, Ordering::Release);

                        let msg = DeregistrationMessage { id, state, mr };
                        trace!(message = ?msg, operation = "send", channel = "dereg");
                        dereg_tx.send(msg).unwrap();
                    } else {
                        panic!("Unknown WR ID: {id}")
                    }
                }
            }
        });

        for _ in 0..config.concurrency_dereg {
            let dereg_rx = dereg_rx.clone();

            thread::spawn(move || {
                while let Ok(msg) = dereg_rx.recv() {
                    trace!(message = ?msg, operation = "recv", channel = "dereg");
                    let DeregistrationMessage { state, mr, .. } = msg;

                    let bytes = mr.deregister().unwrap();
                    state.bytes.lock().unwrap().replace(bytes);
                    state.deregistered.store(true, Ordering::Release);
                }
            });
        }

        Ok(Self { id, reg_tx, config })
    }

    fn prefetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<Self::Handle> {
        assert_eq!(bytes.len(), remote.len());

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let handle = Handle::default();

        let msg = RegistrationMessage {
            id,
            state: handle.state.clone(),
            bytes,
            remote: remote.slice(..),
        };
        trace!(message = ?msg, operation = "send", channel = "reg");
        self.reg_tx.send(msg).unwrap();

        Ok(handle)
    }

    fn config(&self) -> &Self::Config {
        &self.config
    }
}
