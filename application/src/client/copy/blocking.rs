use super::lib::PRE_ALLOCATIONS;
use crate::client::{BaseClient, BlockingClient, ClientConfig};
use bytes::BytesMut;
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::{hint, io};

pub struct Client {
    base: BaseClient,
    mrs: VecDeque<MemoryRegion>,
}

impl BlockingClient for Client {
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

    fn request(&mut self, mut bytes: BytesMut) -> io::Result<BytesMut> {
        let mr_size = self.base.cfg.mr_size;
        assert_eq!(bytes.len() % mr_size, 0);

        let chunks = bytes.len() / mr_size;
        let mut completions = vec![ibv_wc::default(); PRE_ALLOCATIONS];

        let mut outstanding = HashMap::new();
        let mut chunk = 0;
        let mut received = 0;

        while received < chunks {
            while chunk < chunks {
                if let Some(mr) = self.mrs.pop_front() {
                    let local = mr.slice_local(..);
                    let remote_slice = self
                        .base
                        .remote
                        .slice(chunk * mr_size..(chunk + 1) * mr_size);

                    let mut posted = false;
                    for qp in &mut self.base.qps {
                        match unsafe { qp.post_read(&[local], remote_slice, chunk as u64) } {
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
                        self.mrs.push_front(mr);
                        break;
                    } else {
                        outstanding.insert(chunk, mr);
                        chunk += 1;
                    }
                } else {
                    break;
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                assert!(completion.is_valid());
                let chunk = completion.wr_id() as usize;

                if let Some(mr) = outstanding.remove(&chunk) {
                    let src_slice = mr.as_slice();
                    let dst_slice = &mut bytes[chunk * mr_size..(chunk + 1) * mr_size];
                    dst_slice.copy_from_slice(src_slice);

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
