use ibverbs::{CompletionQueue, LocalMemorySlice, QueuePair, ibv_wc};
use std::collections::HashMap;
use std::io;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use tokio::sync::{Mutex, oneshot};
use tokio::task;
use tracing::{Instrument, debug, debug_span, error, instrument, warn};

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
                polling_loop(cq, pending).await;
            });
        }
        Self { cq, pending, id }
    }

    #[instrument(
        skip_all,
        name = "AsyncCompletionQueue::post_receive",
        err,
        level = "debug"
    )]
    pub async fn post_receive(
        &mut self,
        qp: &mut QueuePair,
        local: &[LocalMemorySlice],
    ) -> io::Result<oneshot::Receiver<ibv_wc>> {
        self.post(qp, local, |qp, local, id| unsafe {
            qp.post_receive(local, id)
        })
        .await
    }
    #[instrument(skip_all, name = "AsyncCompletionQueue::post_send" err, level = "debug")]
    pub async fn post_send(
        &mut self,
        qp: &mut QueuePair,
        local: &[LocalMemorySlice],
    ) -> io::Result<oneshot::Receiver<ibv_wc>> {
        self.post(qp, local, |qp, local, id| unsafe {
            qp.post_send(local, id)
        })
        .await
    }

    #[instrument(skip_all, fields(wr_id), err)]
    async fn post<F>(
        &mut self,
        qp: &mut QueuePair,
        local: &[LocalMemorySlice],
        mut post_fn: F,
    ) -> io::Result<oneshot::Receiver<ibv_wc>>
    where
        F: FnMut(&mut QueuePair, &[LocalMemorySlice], u64) -> io::Result<()> + Send,
    {
        let wr_id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        tracing::Span::current().record("wr_id", &wr_id);
        let (sender, receiver) = oneshot::channel();
        self.pending.lock().await.insert(wr_id, sender);
        debug!("Posting");
        while let Err(e) = post_fn(qp, local, wr_id) {
            if e.kind() != io::ErrorKind::OutOfMemory {
                return Err(e);
            }
            error!(error = %e, "Failed");
            task::yield_now().await;
        }
        debug!("Posted");
        Ok(receiver)
    }
}

#[instrument(skip_all, name = "AsyncCompletionQueue::polling_loop", level = "debug")]
async fn polling_loop(
    cq: Arc<Mutex<CompletionQueue>>,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<ibv_wc>>>>,
) {
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

            let span = debug_span!("WC",wr_id);
            async {
                debug!("Received");
                if let Some(sender) = pending.lock().await.remove(&wr_id) {
                    let _ = sender.send(*completion);
                    debug!("Sender notified");
                } else {
                    warn!("Sender not found");
                }
            }
            .instrument(span)
            .await;
        }
        task::yield_now().await;
    }
}
