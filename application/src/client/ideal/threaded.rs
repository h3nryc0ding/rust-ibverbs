use super::lib::{Handle, MRMessage, Pending, PostMessage};
use crate::client::{BaseClient, NonBlockingClient};
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
}

pub struct Client {
    id: AtomicUsize,
    post_tx: Sender<PostMessage>,

    config: Config,
}

impl NonBlockingClient for Client {
    type Config = Config;
    type Handle = Handle;

    fn new(client: BaseClient, config: Config) -> io::Result<Self> {
        let id = AtomicUsize::new(0);

        let (mr_tx, mr_rx) = channel::unbounded::<MRMessage>();
        let (post_tx, post_rx) = channel::unbounded();

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
                        trace!(message = ?msg, operation = "try_recv", channel = "mr");
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
                    let PostMessage { id, state, remote } = outstanding.pop_front().unwrap();

                    let local = mr.slice_local(..).collect::<Vec<_>>();
                    let mut posted = false;
                    for qp in &client.qps {
                        match unsafe { qp.post_read(&local, remote, id as u64) } {
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
                        outstanding.push_front(PostMessage { id, state, remote })
                    } else {
                        state.posted.store(true, Ordering::Relaxed);
                        pending.insert(id, Pending { state, mr });
                    }
                }

                for completion in client.cq.poll(&mut completions).unwrap() {
                    assert!(completion.is_valid());
                    let wr_id = completion.wr_id() as usize;

                    if let Some(Pending { state, mr }) = pending.remove(&wr_id) {
                        state.received.store(true, Ordering::Relaxed);

                        let msg = MRMessage { 0: mr };
                        trace!(message = ?msg, operation = "send", channel = "mr");
                        mr_tx.send(msg).unwrap();
                    }
                }
            }
        });

        Ok(Self {
            id,
            post_tx,
            config,
        })
    }

    fn prefetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<Self::Handle> {
        assert_eq!(bytes.len(), remote.len());
        assert_eq!(bytes.len(), self.config.mr_size);

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let handle = Handle::default();

        let msg = PostMessage {
            id,
            state: handle.state.clone(),
            remote: remote.slice(..),
        };
        trace!(message = ?msg, operation = "send", channel = "post");
        self.post_tx.send(msg).unwrap();

        Ok(handle)
    }
}
