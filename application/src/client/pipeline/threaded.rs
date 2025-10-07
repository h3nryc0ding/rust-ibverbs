use crate::chunks_mut_exact;
use crate::client::{
    BaseClient, ClientConfig, NonBlockingClient, RequestCore, RequestHandle, decode_wr_id,
    encode_wr_id,
};
use bytes::BytesMut;
use crossbeam::channel;
use crossbeam::channel::{Sender, TryRecvError};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread::JoinHandle;
use std::{io, thread};

pub struct Client {
    id: AtomicUsize,

    reg_tx: Sender<RegistrationMessage>,

    config: ClientConfig,
    _workers: Vec<JoinHandle<()>>,
}

const CONCURRENT_REGISTRATIONS: usize = 8;
const CONCURRENT_DEREGISTRATIONS: usize = 2;

impl NonBlockingClient for Client {
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
                while let Ok(RegistrationMessage {
                                 id,
                                 chunk,
                                 state,
                                 bytes,
                             }) = reg_rx.recv()
                {
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
                    };
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
                if let Some(PostMessage {
                                id,
                                chunk,
                                state,
                                mr,
                            }) = waiting.pop_front()
                {
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
                        });
                    }
                }

                match post_rx.try_recv() {
                    Ok(msg) => waiting.push_back(msg),
                    Err(TryRecvError::Disconnected) => return,
                    _ => {}
                }

                for completion in base.cq.poll(&mut completions).unwrap() {
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
                        dereg_tx.send(msg).unwrap();
                    } else {
                        panic!("Unknown WR ID: {wr_id}")
                    }
                }
            }
        });
        workers.push(handle);

        for _ in 0..CONCURRENT_DEREGISTRATIONS {
            let dereg_rx = dereg_rx.clone();

            let handle = thread::spawn(move || {
                while let Ok(msg) = dereg_rx.recv() {
                    let DeregistrationMessage {
                        chunk, state, mr, ..
                    } = msg;
                    let bytes = mr.deregister().unwrap();
                    state
                        .progress
                        .deregistered_copied
                        .fetch_add(1, Ordering::Relaxed);
                    state.aggregator.bytes.insert(chunk, bytes);
                }
            });
            workers.push(handle);
        }

        Ok(Self {
            id,
            reg_tx,
            config: base.cfg,
            _workers: workers,
        })
    }

    fn request(&mut self, bytes: BytesMut) -> io::Result<RequestHandle> {
        let chunk_size = self.config.mr_size;
        assert_eq!(bytes.len() % chunk_size, 0);

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<_> = chunks_mut_exact(bytes, chunk_size).collect();

        let handle = RequestHandle::new(chunks.len());

        for (chunk, bytes) in chunks.into_iter().enumerate() {
            self.reg_tx
                .send(RegistrationMessage {
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

struct RegistrationMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestCore>,
    bytes: BytesMut,
}

struct PostMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestCore>,
    mr: MemoryRegion,
}

struct DeregistrationMessage {
    #[allow(dead_code)]
    id: usize,
    chunk: usize,
    state: Arc<RequestCore>,
    mr: MemoryRegion,
}
