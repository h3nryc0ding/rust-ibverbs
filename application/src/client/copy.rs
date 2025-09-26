use crate::client::{BaseClient, Client};
use crate::{OPTIMAL_MR_SIZE, OPTIMAL_QP_COUNT};
use ibverbs::{Context, MemoryRegion, OwnedMemoryRegion, ibv_wc};
use std::collections::VecDeque;
use std::net::ToSocketAddrs;
use std::{hint, io};

const RX_DEPTH: usize = 128;

pub struct CopyClient {
    base: BaseClient<OPTIMAL_QP_COUNT>,
    mrs: Vec<OwnedMemoryRegion>,
}

impl Client for CopyClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseClient::new(ctx, addr)?;

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

    fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let chunks = (dst.len() + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
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
                        .slice(posted * OPTIMAL_MR_SIZE..(posted + 1) * OPTIMAL_MR_SIZE);

                    let chunk_idx = posted;
                    let wr_id = (chunk_idx as u64) << 32 | (mr_idx as u64);

                    let mut success = false;
                    for qp in &mut self.base.qps {
                        match unsafe { qp.post_read(&[local], remote_slice, wr_id) } {
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
                let wr_id = completion.wr_id();
                let chunk_idx = (wr_id >> 32) as usize;
                let mr_idx = (wr_id & 0xFFFFFFFF) as usize;

                let mr = &self.mrs[mr_idx];
                let start = chunk_idx * OPTIMAL_MR_SIZE;
                let end = start + OPTIMAL_MR_SIZE;

                let src_slice = mr.as_slice();
                let dest_slice = &mut dst[start..end];
                dest_slice.copy_from_slice(src_slice);

                unused.push_back(mr_idx);
                received += 1;
            }
        }
        Ok(())
    }
}
