use super::lib::{DeregistrationMessage, PostMessage, RegistrationMessage};
use crate::client::{AsyncClient, BaseClient, ClientConfig, RequestHandle};
use bytes::BytesMut;
use ibverbs::{Context, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task;
use tracing::trace;

pub struct Client {
    id: AtomicUsize,

    reg_tx: UnboundedSender<RegistrationMessage>,
}

impl AsyncClient for Client {
    async fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let mut base = BaseClient::new(ctx, cfg)?;
        let id = AtomicUsize::new(0);

        let (reg_tx, mut reg_rx) = mpsc::unbounded_channel();
        let (post_tx, mut post_rx) = mpsc::unbounded_channel();
        let (dereg_tx, mut dereg_rx) = mpsc::unbounded_channel();

        let pd = Arc::new(base.pd);
        task::spawn(async move {
            while let Some(msg) = reg_rx.recv().await {
                let pd = pd.clone();
                let post_tx = post_tx.clone();

                task::spawn_blocking(move || {
                    trace!(message = ?msg, operation = "recv", channel = "reg");
                    let RegistrationMessage { id, state, bytes } = msg;

                    let mr = pd.register(bytes).unwrap();
                    state
                        .progress
                        .registered_acquired
                        .fetch_add(1, Ordering::Relaxed);

                    let msg = PostMessage { id, state, mr };
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
                        trace!(message = ?msg,operation = "try_recv",channel = "post");
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
                        trace!(message = ?msg, operation = "send", channel = "dereg");
                        dereg_tx.send(msg).unwrap();
                    } else {
                        panic!("Unknown WR ID: {id}")
                    }
                }
            }
        });

        task::spawn(async move {
            while let Some(msg) = dereg_rx.recv().await {
                task::spawn_blocking(move || {
                    trace!(message = ?msg, operation = "recv", channel = "dereg");
                    let DeregistrationMessage { state, mr, .. } = msg;

                    let bytes = mr.deregister().unwrap();

                    state
                        .progress
                        .deregistered_copied
                        .fetch_add(1, Ordering::Relaxed);
                    state.aggregator.bytes.insert(0, bytes);
                });
            }
        });

        Ok(Self { id, reg_tx })
    }

    async fn request(&mut self, bytes: BytesMut) -> io::Result<RequestHandle> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);

        let handle = RequestHandle::new(1);

        let msg = RegistrationMessage {
            id,
            state: handle.core.clone(),
            bytes,
        };
        trace!(message = ?msg, operation = "send", channel = "reg");
        self.reg_tx.send(msg).unwrap();

        Ok(handle)
    }
}
