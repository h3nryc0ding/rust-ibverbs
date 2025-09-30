use crate::client::{
    BaseClient, ClientConfig, NonBlockingClient, RequestHandle, RequestState, decode_wr_id,
    encode_wr_id,
};
use dashmap::DashMap;
use ibverbs::{Context, MemoryRegion, RemoteMemorySlice, ibv_wc};
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio::sync::mpsc;
use tokio::sync::mpsc::UnboundedSender;
use tokio::task;

pub struct PipelineAsyncClient {
    id: AtomicUsize,
    remote: RemoteMemorySlice,

    reg_tx: UnboundedSender<RegistrationMessage>,

    config: ClientConfig,
}

impl NonBlockingClient for PipelineAsyncClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
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
                    let RegistrationMessage {
                        id,
                        chunk,
                        ptr,
                        len,
                        state,
                        remote,
                    } = msg;
                    let mr = unsafe { pd.register_raw(ptr as *mut u8, len).unwrap() };
                    state.registered_acquired.fetch_add(1, Ordering::Relaxed);
                    let msg = PostMessage {
                        id,
                        chunk,
                        state,
                        mr,
                        remote,
                    };
                    post_tx.send(msg).unwrap();
                });
            }
        });

        let pending = Arc::new(DashMap::new());
        {
            let pending = pending.clone();
            task::spawn_blocking(move || {
                while let Some(msg) = post_rx.blocking_recv() {
                    let PostMessage {
                        id,
                        chunk,
                        state,
                        mr,
                        remote,
                    } = msg;

                    let local = mr.slice_local(..);
                    let wr_id = encode_wr_id(id, chunk);
                    'l: loop {
                        for qp in &mut base.qps {
                            match unsafe { qp.post_read(&[local], remote, wr_id) } {
                                Ok(_) => break 'l,
                                Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                                Err(e) => panic!("{:?}", e),
                            }
                        }
                    }

                    state.posted.fetch_add(1, Ordering::Relaxed);
                    pending.insert(wr_id, Pending { state, mr });
                }
            });
        }

        {
            let pending = pending.clone();
            task::spawn_blocking(move || {
                let mut completions = [ibv_wc::default(); 16];
                loop {
                    let completed = base.cq.poll(&mut completions).unwrap();
                    for completion in completed {
                        assert!(completion.is_valid());
                        let wr_id = completion.wr_id();
                        if let Some((_, Pending { state, mr })) = pending.remove(&wr_id) {
                            let (id, chunk) = decode_wr_id(wr_id);
                            state.received.fetch_sub(1, Ordering::Relaxed);
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
        }

        task::spawn(async move {
            while let Some(request) = dereg_rx.recv().await {
                task::spawn_blocking(move || {
                    let DeregistrationMessage { state, mr, .. } = request;
                    drop(mr);
                    state.deregistered_copied.fetch_add(1, Ordering::Relaxed);
                });
            }
        });

        Ok(Self {
            id,
            remote: base.remote,
            reg_tx,
            config: base.cfg,
        })
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<RequestHandle> {
        let chunk_size = self.config.mr_size;

        let id = self.id.fetch_add(1, Ordering::Relaxed);
        let chunks: Vec<_> = dst.chunks_mut(chunk_size).collect();

        let handle = RequestHandle {
            id,
            chunks: chunks.len(),
            state: Default::default(),
        };

        for (idx, chunk) in chunks.into_iter().enumerate() {
            self.reg_tx
                .send(RegistrationMessage {
                    id,
                    chunk: idx,
                    state: handle.state.clone(),
                    ptr: chunk.as_mut_ptr() as usize,
                    len: chunk.len(),
                    remote: self.remote.slice(idx * chunk_size..(idx + 1) * chunk_size),
                })
                .unwrap()
        }

        Ok(handle)
    }
}

struct Pending {
    state: Arc<RequestState>,
    mr: MemoryRegion,
}

struct RegistrationMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestState>,
    // ptr: *mut u8,
    ptr: usize, // TODO: fix the ptr type
    len: usize,
    remote: RemoteMemorySlice,
}

unsafe impl Send for RegistrationMessage {}

struct PostMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestState>,
    mr: MemoryRegion,
    remote: RemoteMemorySlice,
}

struct DeregistrationMessage {
    id: usize,
    chunk: usize,
    state: Arc<RequestState>,
    mr: MemoryRegion,
}
