use crate::client::{BaseClient, BlockingClient, ClientConfig};
use bytes::BytesMut;
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::{hint, io};

const PRE_ALLOCATIONS: usize = 128;

pub struct IdealClient {
    base: BaseClient,
    mrs: VecDeque<MemoryRegion>,
}

impl BlockingClient for IdealClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let base = BaseClient::new(ctx, cfg)?;

        let mut mrs = VecDeque::with_capacity(PRE_ALLOCATIONS);
        for _ in 0..PRE_ALLOCATIONS {
            loop {
                match base.pd.allocate_zeroed(base.cfg.mr_size) {
                    Ok(mr) => {
                        mrs.push_back(mr);
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(Self { base, mrs })
    }

    fn request(&mut self, bytes: BytesMut) -> io::Result<BytesMut> {
        let mr_size = self.base.cfg.mr_size;
        assert_eq!(bytes.len() % mr_size, 0);

        let chunks = bytes.len() / mr_size;
        let mut completions = vec![ibv_wc::default(); PRE_ALLOCATIONS];

        let mut outstanding = HashMap::new();
        let mut posted = 0;
        let mut received = 0;

        while received < chunks {
            while posted < chunks {
                if let Some(mr) = self.mrs.pop_front() {
                    let local = mr.slice_local(..);
                    let remote_slice = self
                        .base
                        .remote
                        .slice(posted * mr_size..(posted + 1) * mr_size);
                    let chunk = posted as u64;

                    let mut success = false;
                    for qp in &mut self.base.qps {
                        match unsafe { qp.post_read(&[local], remote_slice, chunk) } {
                            Ok(_) => {
                                success = true;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                                hint::spin_loop();
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    if !success {
                        self.mrs.push_front(mr);
                        break;
                    } else {
                        outstanding.insert(posted, mr);
                        posted += 1;
                    }
                } else {
                    break;
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                assert!(completion.is_valid());
                let chunk = completion.wr_id() as usize;

                if let Some(mr) = outstanding.remove(&chunk) {
                    self.mrs.push_back(mr);
                    received += 1;
                } else {
                    panic!("unknown completion: {completion:?}");
                }
            }
        }

        Ok(bytes)
    }
}
