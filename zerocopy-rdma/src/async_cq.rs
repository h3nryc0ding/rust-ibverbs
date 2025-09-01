use ibverbs::{CompletionQueue, LocalMemorySlice, QueuePair, ibv_wc};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{Mutex, oneshot};
use tokio::task;
use tracing::{debug, error, instrument, trace, warn};

#[derive(Clone)]
pub struct AsyncCompletionQueue {
    cq: Arc<Mutex<CompletionQueue>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<ibv_wc>>>>,
    id: Arc<AtomicU64>,
}

impl AsyncCompletionQueue {
    #[instrument(skip(cq), name = "AsyncCompletionQueue::new")]
    pub fn new(cq: CompletionQueue) -> Self {
        let cq = Arc::new(Mutex::new(cq));
        let pending = Arc::new(Mutex::new(HashMap::<u64, oneshot::Sender<ibv_wc>>::new()));
        let id = Arc::new(AtomicU64::new(0));
        {
            let cq = cq.clone();
            let pending = pending.clone();
            task::spawn(async move {
                loop {
                    let mut completions = [ibv_wc::default(); 16];
                    let completed = {
                        let cq = cq.lock().await;
                        match cq.poll(&mut completions) {
                            Ok(c) => c,
                            Err(e) => {
                                error!(error = %e, "Failed to poll CQ");
                                continue;
                            }
                        }
                    };
                    for completion in completed {
                        let wr_id = completion.wr_id();
                        debug!(wr_id, "Received WC");
                        if let Some(sender) = pending.lock().await.remove(&completion.wr_id()) {
                            let _ = sender.send(*completion);
                            debug!(wr_id, "Sent WC to pending sender");
                        } else {
                            warn!(wr_id, "No pending sender for WC");
                        }
                    }
                    task::yield_now().await;
                }
            });
        }
        Self { cq, pending, id }
    }

    #[instrument(skip_all, err, level = "debug")]
    pub async fn post_receive(
        &mut self,
        qp: &mut QueuePair,
        local: &[LocalMemorySlice],
    ) -> io::Result<oneshot::Receiver<ibv_wc>> {
        self.post(qp, local, |qp, local, id| unsafe {
            qp.post_receive(local, id).map_err(|e| {
                io::Error::new(
                    io::ErrorKind::Other,
                    format!("Failed to post receive: {}", e),
                )
            })
        })
        .await
    }
    #[instrument(skip_all, err, level = "debug")]
    pub async fn post_send(
        &mut self,
        qp: &mut QueuePair,
        local: &[LocalMemorySlice],
    ) -> io::Result<oneshot::Receiver<ibv_wc>> {
        self.post(qp, local, |qp, local, id| unsafe {
            qp.post_send(local, id).map_err(|e| {
                io::Error::new(io::ErrorKind::Other, format!("Failed to post send: {}", e))
            })
        })
        .await
    }

    #[instrument(skip_all, err, level = "trace")]
    async fn post<F>(
        &mut self,
        qp: &mut QueuePair,
        local: &[LocalMemorySlice],
        post_fn: F,
    ) -> io::Result<oneshot::Receiver<ibv_wc>>
    where
        F: FnOnce(&mut QueuePair, &[LocalMemorySlice], u64) -> io::Result<()> + Send,
    {
        let wr_id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(wr_id, sender);
        debug!(wr_id, "Posting WR");
        post_fn(qp, local, wr_id)?;
        debug!(wr_id, "Posted WR");
        Ok(receiver)
    }
}
