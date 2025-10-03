use crate::chunks_mut_exact;
use crate::client::{BaseClient, BlockingClient, ClientConfig};
use bytes::BytesMut;
use ibverbs::{Context, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;

pub struct PipelineClient(BaseClient);

impl BlockingClient for PipelineClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        Ok(Self(BaseClient::new(ctx, cfg)?))
    }

    fn request(&mut self, bytes: BytesMut) -> io::Result<BytesMut> {
        let mr_size = self.0.cfg.mr_size;
        assert_eq!(bytes.len() % mr_size, 0);

        let mut completions = vec![ibv_wc::default(); 16];

        let total = bytes.len() / mr_size;

        // VecDeque<(chunk, bytes)>
        let mut chunks = VecDeque::from_iter(chunks_mut_exact(bytes, mr_size).enumerate());
        // VecDeque<(chunk, mr)>
        let mut allocated = VecDeque::new();
        // HashMap<chunk, mr>
        let mut pending = HashMap::new();
        // HashMap<chunk, mr>
        let mut received = HashMap::new();

        while received.len() < total {
            if let Some((chunk, bytes)) = chunks.pop_front() {
                let mr = self.0.pd.register(bytes)?;
                allocated.push_back((chunk, mr));
            }

            if let Some((chunk, mr)) = allocated.pop_front() {
                let local = mr.slice_local(..);
                let remote = self.0.remote.slice(chunk * mr_size..(chunk + 1) * mr_size);

                let mut posted = false;
                for qp in &mut self.0.qps {
                    match unsafe { qp.post_read(&[local], remote, chunk as u64) } {
                        Ok(_) => {
                            posted = true;
                            break;
                        }
                        Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                        Err(e) => return Err(e),
                    }
                }

                if posted {
                    pending.insert(chunk, mr);
                } else {
                    allocated.push_front((chunk, mr));
                }
            }

            for completion in self.0.cq.poll(&mut completions)? {
                assert!(completion.is_valid());
                let chunk = completion.wr_id() as usize;

                if let Some(mr) = pending.remove(&chunk) {
                    let bytes = mr.deregister()?;
                    received.insert(chunk, bytes);
                } else {
                    panic!("unknown completion: {completion:?}");
                }
            }
        }

        let mut result = received
            .remove(&0)
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))?;

        for i in 1..total {
            if let Some(next) = received.remove(&i) {
                result.unsplit(next);
            } else {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
        }

        Ok(result)
    }
}
