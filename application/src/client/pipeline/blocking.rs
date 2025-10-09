use crate::chunks_mut_exact;
use crate::client::{BaseClient, BlockingClient};
use bytes::BytesMut;
use ibverbs::{RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;

pub struct Config {
    pub chunk_size: usize,
}

pub struct Client {
    base: BaseClient,
    config: Config,
}

impl BlockingClient for Client {
    type Config = Config;

    fn new(client: BaseClient, config: Config) -> io::Result<Self> {
        Ok(Self {
            base: client,
            config,
        })
    }

    fn fetch(&mut self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<BytesMut> {
        assert_eq!(bytes.len(), remote.len());
        let chunk_size = self.config.chunk_size;

        let mut completions = vec![ibv_wc::default(); 16];

        let mut chunks = VecDeque::new();
        for (chunk, bytes) in chunks_mut_exact(bytes, chunk_size).enumerate() {
            let start = chunk * chunk_size;
            let length = bytes.len();
            chunks.push_back((chunk, bytes, remote.slice(start..start + length)));
        }

        let mut allocated = VecDeque::new();
        let mut pending = HashMap::new();
        let mut received = HashMap::new();

        let total = chunks.len();
        while received.len() < total {
            if let Some((chunk, bytes, remote)) = chunks.pop_front() {
                let mr = self.base.pd.register(bytes)?;
                allocated.push_back((chunk, mr, remote));
            }

            if let Some((chunk, mr, remote)) = allocated.pop_front() {
                let local = mr.slice_local(..).collect::<Vec<_>>();

                let mut posted = false;
                for qp in &self.base.qps {
                    match unsafe { qp.post_read(&local, remote, chunk as u64) } {
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
                    allocated.push_front((chunk, mr, remote));
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
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

        let Some(mut result) = received.remove(&0) else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Failed to reassemble bytes: missing chunk 0 of {}", total),
            ));
        };
        for i in 1..total {
            let Some(bytes) = received.remove(&i) else {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("Failed to reassemble bytes: missing chunk {i} of {total}"),
                ));
            };
            result.unsplit(bytes);
        }

        Ok(result)
    }
}
