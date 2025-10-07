use super::lib::{DeregistrationMessage, PostMessage, RegistrationMessage};
use crate::client::{BaseClient, ClientConfig, NonBlockingClient, RequestHandle};
use bytes::BytesMut;
use crossbeam::channel;
use crossbeam::channel::{Sender, TryRecvError};
use ibverbs::{Context, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::{io, thread};
use tracing::trace;

const CONCURRENT_REGISTRATIONS: usize = 8;
const CONCURRENT_DEREGISTRATIONS: usize = 2;

pub struct Client {
    id: AtomicUsize,

    reg_tx: Sender<RegistrationMessage>,
}

impl NonBlockingClient for Client {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let mut base = BaseClient::new(ctx, cfg)?;
        let id = AtomicUsize::new(0);

        let (reg_tx, reg_rx) = channel::unbounded();
        let (post_tx, post_rx) = channel::unbounded();
        let (dereg_tx, dereg_rx) = channel::unbounded();

        for _ in 0..CONCURRENT_REGISTRATIONS {
            let pd = base.pd.clone();
            let reg_rx = reg_rx.clone();
            let post_tx = post_tx.clone();

            thread::spawn(move || {
                while let Ok(msg) = reg_rx.recv() {
                    trace!(message = debug(&msg), operation = "recv", channel = "reg");
                    let RegistrationMessage { id, state, bytes } = msg;

                    let mr = pd.register(bytes).unwrap();
                    state
                        .progress
                        .registered_acquired
                        .fetch_add(1, Ordering::Relaxed);

                    let msg = PostMessage { id, state, mr };
                    trace!(message = debug(&msg), operation = "send", channel = "post");
                    post_tx.send(msg).unwrap()
                }
            });
        }

        thread::spawn(move || {
            let mut pending = HashMap::new();
            let mut waiting = VecDeque::new();
            let mut completions = [ibv_wc::default(); 16];

            loop {
                if let Some(PostMessage { id, state, mr }) = waiting.pop_front() {
                    let local = mr.slice_local(..);
                    let remote = base.remote.slice(0..mr.len());

                    let mut posted = false;
                    for qp in &mut base.qps {
                        match unsafe { qp.post_read(&[local], remote, id as u64) } {
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
                        pending.insert(id, (state, mr));
                    } else {
                        waiting.push_front(PostMessage { id, state, mr });
                    }
                }

                match post_rx.try_recv() {
                    Ok(msg) => {
                        trace!(
                            message = debug(&msg),
                            operation = "try_recv",
                            channel = "post"
                        );
                        waiting.push_back(msg)
                    }
                    Err(TryRecvError::Disconnected) => return,
                    _ => {}
                }

                for completion in base.cq.poll(&mut completions).unwrap() {
                    assert!(completion.is_valid());
                    let id = completion.wr_id() as usize;

                    if let Some((state, mr)) = pending.remove(&id) {
                        state.progress.received.fetch_add(1, Ordering::Relaxed);

                        let msg = DeregistrationMessage { id, state, mr };
                        trace!(message = debug(&msg), operation = "send", channel = "dereg");
                        dereg_tx.send(msg).unwrap();
                    } else {
                        panic!("Unknown WR ID: {id}")
                    }
                }
            }
        });

        for _ in 0..CONCURRENT_DEREGISTRATIONS {
            let dereg_rx = dereg_rx.clone();

            thread::spawn(move || {
                while let Ok(msg) = dereg_rx.recv() {
                    trace!(message = debug(&msg), operation = "recv", channel = "dereg");
                    let DeregistrationMessage { state, mr, .. } = msg;

                    let bytes = mr.deregister().unwrap();

                    state
                        .progress
                        .deregistered_copied
                        .fetch_add(1, Ordering::Relaxed);
                    state.aggregator.bytes.insert(0, bytes);
                }
            });
        }

        Ok(Self { id, reg_tx })
    }

    fn request(&mut self, bytes: BytesMut) -> io::Result<RequestHandle> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);

        let handle = RequestHandle::new(1);

        let msg = RegistrationMessage {
            id,
            state: handle.core.clone(),
            bytes,
        };
        trace!(message = debug(&msg), operation = "send", channel = "reg");
        self.reg_tx.send(msg).unwrap();

        Ok(handle)
    }
}
