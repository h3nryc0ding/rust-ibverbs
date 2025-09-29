use crate::client::{BaseClient, BlockingClient, ClientConfig};
use ibverbs::{Context, ibv_wc};
use std::collections::HashMap;
use std::io::ErrorKind;
use std::{hint, io};
use tracing::trace;

pub struct PipelineClient {
    base: BaseClient,
}

impl BlockingClient for PipelineClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let base = BaseClient::new(ctx, cfg)?;
        Ok(Self { base })
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let mr_size = self.base.cfg.mr_size;
        let chunks = (dst.len() + mr_size - 1) / mr_size;
        let mut completions = vec![ibv_wc::default(); 16];

        let mut mrs = Vec::with_capacity(chunks);
        let mut outstanding = HashMap::new();
        let mut allocated = 0;
        let mut posted = 0;
        let mut received = 0;

        while received < chunks {
            if allocated < chunks {
                let data = &mut dst[allocated * mr_size..(allocated + 1) * mr_size];
                match unsafe { self.base.pd.register(data) } {
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
                        .slice(posted * mr_size..(posted + 1) * mr_size);

                    let mut success = false;
                    for qp in &mut self.base.qps {
                        match unsafe { qp.post_read(&[local], remote_slice, posted as u64) } {
                            Ok(_) => {
                                trace!("Posted RDMA read for chunk {}", posted);
                                success = true;
                                break;
                            }
                            Err(e) if e.kind() == ErrorKind::OutOfMemory => {
                                trace!("QP out of memory when posting RDMA read",);
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
                    drop(mr);
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
