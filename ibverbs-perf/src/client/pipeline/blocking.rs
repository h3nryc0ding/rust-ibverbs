use crate::{chunks_mut_exact, chunks_unsplit, client};
use bytes::BytesMut;
use ibverbs::{RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;

#[derive(Eq, PartialEq, Clone, Debug)]
pub struct Config {
    pub chunk_size: usize,
}

pub struct Client {
    base: client::BaseClient,

    config: Config,
}

impl client::Client for Client {
    type Config = Config;
    fn config(&self) -> &Self::Config {
        &self.config
    }
}

impl client::BlockingClient for Client {
    fn new(client: client::BaseClient, config: Config) -> io::Result<Self> {
        Ok(Self {
            base: client,
            config,
        })
    }

    fn fetch(&mut self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<BytesMut> {
        assert_eq!(bytes.len(), remote.len());
        let chunk_size = self.config.chunk_size.min(bytes.len());

        let mut completions = vec![ibv_wc::default(); 1];

        let mut chunks = VecDeque::with_capacity(bytes.len() / chunk_size + 1);
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

        chunks_unsplit((0..total).map(|i| received.remove(&i).unwrap()))
    }
}
