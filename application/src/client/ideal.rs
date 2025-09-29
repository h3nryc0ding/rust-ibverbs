use crate::client::{BaseClient, BlockingClient, ClientConfig};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::VecDeque;
use std::{hint, io};

const RX_DEPTH: usize = 128;

pub struct IdealClient {
    base: BaseClient,
    mrs: Vec<MemoryRegion>,
}

impl BlockingClient for IdealClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let base = BaseClient::new(ctx, cfg)?;

        let mut mrs = Vec::with_capacity(RX_DEPTH);
        for _ in 0..RX_DEPTH {
            loop {
                match base.pd.allocate::<u8>(base.cfg.mr_size) {
                    Ok(mr) => {
                        mrs.push(mr);
                        break;
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => continue,
                    Err(e) => return Err(e),
                }
            }
        }

        Ok(Self { base, mrs })
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let mr_size = self.base.cfg.mr_size;
        let chunks = (dst.len() + mr_size - 1) / mr_size;
        let mut completions = vec![ibv_wc::default(); RX_DEPTH];

        let mut unused = (0..RX_DEPTH).collect::<VecDeque<usize>>();
        let mut posted = 0;
        let mut received = 0;

        while received < chunks {
            while posted < chunks {
                if let Some(mr_idx) = unused.pop_front() {
                    let mr = &self.mrs[mr_idx];
                    let local = mr.slice_local(..);
                    let remote_slice = self
                        .base
                        .remote
                        .slice(posted * mr_size..(posted + 1) * mr_size);

                    let mut success = false;
                    for i in 0..self.base.qps.len() {
                        match unsafe {
                            self.base.qps[i].post_read(&[local], remote_slice, mr_idx as u64)
                        } {
                            Ok(_) => {
                                posted += 1;
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
                        unused.push_front(mr_idx);
                        break;
                    }
                } else {
                    break;
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                let mr_idx = completion.wr_id() as usize;
                unused.push_back(mr_idx);
                received += 1;
            }
        }
        Ok(())
    }
}
