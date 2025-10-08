use crate::chunks_mut_exact;
use crate::client::{BaseClient, BlockingClient};
use bytes::BytesMut;
use ibverbs::{MemoryRegion, RemoteMemorySlice, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::{hint, io};

pub struct Config {
    pub mr_size: usize,
    pub mr_count: usize,
}

pub struct Client {
    base: BaseClient,
    mrs: VecDeque<MemoryRegion>,

    config: Config,
}

impl Client {
    pub fn new(client: BaseClient, config: Config) -> io::Result<Self> {
        let mut mrs = VecDeque::with_capacity(config.mr_count);
        for _ in 0..config.mr_count {
            loop {
                match client.pd.allocate_zeroed(config.mr_size) {
                    Ok(mr) => {
                        mrs.push_back(mr);
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(Self {
            base: client,
            mrs,
            config,
        })
    }
}

impl BlockingClient for Client {
    fn fetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<BytesMut> {
        assert_eq!(bytes.len() % remote.len(), 0);
        let mr_size = self.config.mr_size;
        let mut completions = vec![ibv_wc::default(); self.config.mr_count];

        let chunks = chunks_mut_exact(bytes, mr_size).collect::<Vec<_>>();

        let mut unused = VecDeque::from_iter(0..self.config.mr_count);
        let mut outstanding = HashMap::new();
        let mut chunk = 0;
        let mut received = 0;

        while received < chunks.len() {
            while chunk < chunks.len() {
                if let Some(mr_id) = unused.pop_front() {
                    let mr = &self.mrs[mr_id];
                    let start = chunk * mr_size;
                    let length = chunks[chunk].len();
                    let local = mr.slice_local(..length).collect::<Vec<_>>();
                    let remote = remote.slice(start..start + length);

                    let mut posted = false;
                    for qp in &self.base.qps {
                        match unsafe { qp.post_read(&local, remote, chunk as u64) } {
                            Ok(_) => {
                                posted = true;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                                hint::spin_loop();
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    if !posted {
                        unused.push_front(mr_id);
                        break;
                    } else {
                        outstanding.insert(chunk, mr_id);
                        chunk += 1;
                    }
                } else {
                    break;
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                assert!(completion.is_valid());
                let chunk = completion.wr_id() as usize;

                if let Some(mr_id) = outstanding.remove(&chunk) {
                    unused.push_back(mr_id);
                    received += 1;
                } else {
                    panic!("unknown completion: {completion:?}");
                }
            }
        }

        chunks
            .into_iter()
            .reduce(|mut res, nxt| {
                res.unsplit(nxt);
                res
            })
            .ok_or_else(|| io::Error::from(io::ErrorKind::UnexpectedEof))
    }
}
