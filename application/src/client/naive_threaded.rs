use crate::client::{BaseClient, ClientConfig, NonBlockingClient, RequestCore, RequestHandle};
use bytes::BytesMut;
use crossbeam::channel;
use crossbeam::channel::{Sender, TryRecvError};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::{io, thread};

pub struct NaiveThreadedClient {
    id: AtomicUsize,

    reg_tx: Sender<RegistrationMessage>,

    _workers: Vec<JoinHandle<()>>,
}

const CONCURRENT_REGISTRATIONS: usize = 8;
const CONCURRENT_DEREGISTRATIONS: usize = 2;

impl NonBlockingClient for NaiveThreadedClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let mut base = BaseClient::new(ctx, cfg)?;
        let id = AtomicUsize::new(0);

        let (reg_tx, reg_rx) = channel::unbounded();
        let (post_tx, post_rx) = channel::unbounded();
        let (dereg_tx, dereg_rx) = channel::unbounded();

        let mut workers = Vec::new();
        for _ in 0..CONCURRENT_REGISTRATIONS {
            let pd = base.pd.clone();
            let reg_rx = reg_rx.clone();
            let post_tx = post_tx.clone();

            let handle = thread::spawn(move || {
                while let Ok(RegistrationMessage { id, state, bytes }) = reg_rx.recv() {
                    let mr = pd.register(bytes).unwrap();
                    state
                        .progress
                        .registered_acquired
                        .fetch_add(1, Ordering::Relaxed);
                    let msg = PostMessage { id, state, mr };
                    post_tx.send(msg).unwrap()
                }
            });

            workers.push(handle);
        }

        let handle = thread::spawn(move || {
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
                    Ok(msg) => waiting.push_back(msg),
                    Err(TryRecvError::Disconnected) => return,
                    _ => {}
                }

                for completion in base.cq.poll(&mut completions).unwrap() {
                    assert!(completion.is_valid());
                    let id = completion.wr_id() as usize;

                    if let Some((state, mr)) = pending.remove(&id) {
                        state.progress.received.fetch_add(1, Ordering::Relaxed);
                        let msg = DeregistrationMessage { id, state, mr };
                        dereg_tx.send(msg).unwrap();
                    } else {
                        panic!("Unknown WR ID: {id}")
                    }
                }
            }
        });
        workers.push(handle);

        for _ in 0..CONCURRENT_DEREGISTRATIONS {
            let dereg_rx = dereg_rx.clone();

            let handle = thread::spawn(move || {
                while let Ok(msg) = dereg_rx.recv() {
                    let DeregistrationMessage { state, mr, .. } = msg;
                    let bytes = mr.deregister().unwrap();
                    state
                        .progress
                        .deregistered_copied
                        .fetch_add(1, Ordering::Relaxed);
                    state.aggregator.bytes.insert(0, bytes);
                }
            });
            workers.push(handle);
        }

        Ok(Self {
            id,
            reg_tx,
            _workers: workers,
        })
    }

    fn request(&mut self, bytes: BytesMut) -> io::Result<RequestHandle> {
        let id = self.id.fetch_add(1, Ordering::Relaxed);

        let handle = RequestHandle::new(1);
        self.reg_tx
            .send(RegistrationMessage {
                id,
                state: handle.core.clone(),
                bytes,
            })
            .unwrap();

        Ok(handle)
    }
}

struct RegistrationMessage {
    id: usize,
    state: Arc<RequestCore>,
    bytes: BytesMut,
}

struct PostMessage {
    id: usize,
    state: Arc<RequestCore>,
    mr: MemoryRegion,
}

struct DeregistrationMessage {
    #[allow(dead_code)]
    id: usize,
    state: Arc<RequestCore>,
    mr: MemoryRegion,
}
