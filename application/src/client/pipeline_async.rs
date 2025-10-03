use crate::chunks_mut_exact;
use crate::client::{
    AsyncClient, BaseClient, ClientConfig, RequestCore, RequestHandle, decode_wr_id, encode_wr_id,
};
use bytes::BytesMut;
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::sync::mpsc::error::TryRecvError;
use tokio::task;

pub struct PipelineAsyncClient {
    id: AtomicUsize,

    reg_tx: UnboundedSender<RegistrationMessage>,

    config: ClientConfig,
}

impl AsyncClient for PipelineAsyncClient {
    async fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let mut base = BaseClient::new(ctx, cfg)?;
        let id = AtomicUsize::new(0);

        let (reg_tx, mut reg_rx) = mpsc::unbounded_channel();
        let (post_tx, mut post_rx) = mpsc::unbounded_channel();
        let (dereg_tx, mut dereg_rx) = mpsc::unbounded_channel();

        let pd = Arc::new(base.pd);
        task::spawn(async move {
            while let Some(RegistrationMessage {
                id,
                chunk,
                state,
                bytes,
            }) = reg_rx.recv().await
            {
                let pd = pd.clone();
                let post_tx = post_tx.clone();
                task::spawn_blocking(move || {
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

        task::spawn(async move {
            while let Some(request) = dereg_rx.recv().await {
                task::spawn_blocking(move || {
                    let DeregistrationMessage {
                        chunk, state, mr, ..
                    } = request;
                    let bytes = mr.deregister().unwrap();
                    state
                        .progress
                        .deregistered_copied
                        .fetch_add(1, Ordering::Relaxed);
                    state.aggregator.bytes.insert(chunk, bytes);
                });
            }
        });

        Ok(Self {
            id,
            reg_tx,
            config: base.cfg,
        })
    }

    async fn request(&mut self, bytes: BytesMut) -> io::Result<RequestHandle> {
        let chunk_size = self.config.mr_size;

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
