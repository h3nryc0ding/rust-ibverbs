use crate::client::{BaseClient, Client};
use crate::{OPTIMAL_MR_SIZE, OPTIMAL_QP_COUNT};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::net::ToSocketAddrs;
use std::{hint, io};
use tracing::trace;

pub struct PipelineClient {
    base: BaseClient<OPTIMAL_QP_COUNT>,
}

impl Client for PipelineClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseClient::new(ctx, addr)?;
        Ok(Self { base })
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let chunks = (dst.len() + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
        let mut completions = vec![ibv_wc::default(); 16];

        let mut mrs = Vec::with_capacity(chunks);
        let mut outstanding = HashMap::new();
        let mut allocated = 0;
        let mut posted = 0;
        let mut received = 0;

        while received < chunks {
            if allocated < chunks {
                let ptr = dst[allocated * OPTIMAL_MR_SIZE..].as_ptr();
                match unsafe {
                    self.base
                        .pd
                        .register_unchecked(ptr as *mut u8, OPTIMAL_MR_SIZE)
                } {
                    Ok(mr) => {
                        trace!("Allocated MR for chunk {}", allocated);
                        mrs.push(mr);
                        allocated += 1;
                    }
                    Err(e) if e.kind() == ErrorKind::OutOfMemory => {
                        trace!("Out of memory when allocating MR");
                    }
                    Err(e) => return Err(e),
                }
            }

            if posted < chunks {
                if let Some(mr) = mrs.pop() {
                    let local = mr.slice_local(..);
                    let remote_slice = self
                        .base
                        .remote
                        .slice(posted * OPTIMAL_MR_SIZE..(posted + 1) * OPTIMAL_MR_SIZE);

                    let mut success = false;
                    for i in 0..self.base.qps.len() {
                        match unsafe {
                            self.base.qps[i].post_read(&[local], remote_slice, posted as u64)
                        } {
                            Ok(_) => {
                                trace!("Posted RDMA read for chunk {} on QP {}", posted, i);
                                success = true;
                                break;
                            }
                            Err(e) if e.kind() == ErrorKind::OutOfMemory => {
                                trace!("QP {} out of memory when posting RDMA read", i);
                                hint::spin_loop();
                            }
                            Err(e) => return Err(e),
                        }
                    }
                    if success {
                        outstanding.insert(posted, mr);
                        posted += 1;
                    } else {
                        mrs.push(mr);
                        break;
                    }
                } else {
                    break;
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                let chunk = completion.wr_id() as usize;
                trace!("Received completion for chunk {chunk}");
                if let Some(mr) = outstanding.remove(&chunk) {
                    mr.deregister()?;
                    trace!("Deregistered MR for chunk {chunk}");
                } else {
                    return Err(io::Error::new(
                        ErrorKind::Other,
                        "Received completion for unknown WR ID",
                    ));
                }
                received += 1;
            }
        }
        Ok(())
    }
}
