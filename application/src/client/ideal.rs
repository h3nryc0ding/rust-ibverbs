use crate::OPTIMAL_MR_SIZE;
use crate::client::{BaseMultiQPClient, Client};
use ibverbs::{Context, MemoryRegion, OwnedMemoryRegion, ibv_wc};
use std::collections::VecDeque;
use std::net::ToSocketAddrs;
use std::{hint, io};

const RX_DEPTH: usize = 128;

pub struct IdealClient {
    base: BaseMultiQPClient,
    mrs: Vec<OwnedMemoryRegion>,
}

impl Client for IdealClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseMultiQPClient::new::<3>(ctx, addr)?;

        let mut mrs = Vec::with_capacity(RX_DEPTH);
        for _ in 0..RX_DEPTH {
            loop {
                match base.pd.allocate::<u8>(OPTIMAL_MR_SIZE) {
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

    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        let result = vec![0u8; size].into_boxed_slice();
        let chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
        let mut completions = vec![ibv_wc::default(); RX_DEPTH];

        let mut unused = (0..RX_DEPTH).collect::<VecDeque<usize>>();
        let mut completed = 0;
        let mut pending = 0;

        while pending > 0 || completed < chunks {
            while completed < chunks {
                if let Some(mr_idx) = unused.pop_front() {
                    let mr = &self.mrs[mr_idx];
                    let local = mr.slice_local(..);
                    let remote_slice = self
                        .base
                        .remote
                        .slice(completed * OPTIMAL_MR_SIZE..(completed + 1) * OPTIMAL_MR_SIZE);

                    for i in 0..self.base.qps.len() {
                        match unsafe {
                            self.base.qps[i].post_read(&[local], remote_slice, mr_idx as u64)
                        } {
                            Ok(_) => {
                                completed += 1;
                                pending += 1;
                                break;
                            }
                            Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                                hint::spin_loop();
                            }
                            Err(e) => return Err(e),
                        }
                    }
                } else {
                    break;
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                let mr_idx = completion.wr_id() as usize;
                unused.push_back(mr_idx);
                pending -= 1;
            }
        }
        Ok(result)
    }
}
