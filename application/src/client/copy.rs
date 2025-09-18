use crate::OPTIMAL_MR_SIZE;
use crate::client::{BaseClient, Client};
use ibverbs::{MemoryRegion, OwnedMemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;
use tracing::{info, trace};

const RX_DEPTH: usize = 32;

pub struct CopyClient {
    base: BaseClient,
    mrs: VecDeque<OwnedMemoryRegion>,
}

impl Client for CopyClient {
    fn new(base: BaseClient) -> io::Result<Self> {
        let mut mrs = VecDeque::with_capacity(RX_DEPTH);
        while mrs.len() < RX_DEPTH {
            match base.pd.allocate::<u8>(OPTIMAL_MR_SIZE) {
                Ok(mr) => {
                    trace!("preallocated mr addr={:?} len={}", mr.addr(), mr.len());
                    mrs.push_back(mr);
                }
                Err(e) => {
                    if e.kind() == io::ErrorKind::OutOfMemory {
                        trace!("preallocate mr OOM, retrying");
                    } else {
                        return Err(e);
                    }
                }
            }
        }
        Ok(Self { base, mrs })
    }

    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        let mut result = vec![0u8; size].into_boxed_slice();
        let total_chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
        let mut next_chunk = 0usize;

        let mut outstanding = HashMap::new();
        let mut completions = [ibv_wc::default(); RX_DEPTH];

        while next_chunk < total_chunks || !outstanding.is_empty() {
            // free MRs AND chunks left
            if next_chunk < total_chunks {
                if let Some(mr) = self.mrs.pop_front() {
                    let local = mr.slice_local(..);
                    let remote = self
                        .base
                        .remote
                        .slice(next_chunk * OPTIMAL_MR_SIZE..(next_chunk + 1) * OPTIMAL_MR_SIZE);
                    info!(
                        "reading from remote addr={:?} len={} into addr={:?} len={}",
                        remote.addr(),
                        remote.len(),
                        local.addr(),
                        local.len()
                    );

                    let id = next_chunk as u64;
                    match unsafe { self.base.qp.post_read(&[local], remote, id) } {
                        Ok(_) => {
                            trace!("posted read id={} len={}", id, local.len());
                            outstanding.insert(id, mr);
                            next_chunk += 1;
                        }
                        Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                            trace!("post_read OOM, retrying later");
                            self.mrs.push_back(mr);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            for completion in self.base.cq.poll(&mut completions)? {
                if let Some((e, _)) = completion.error() {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("wc error: {:?}", e),
                    ));
                }

                let id = completion.wr_id();
                if let Some(mr) = outstanding.remove(&id) {
                    let src = mr.as_slice();
                    let src_len = src.len();
                    let offset = (id as usize) * OPTIMAL_MR_SIZE;
                    let end = (offset + src_len).min(result.len());
                    let dst = &mut result[offset..end];
                    dst.copy_from_slice(&src[..dst.len()]);

                    self.mrs.push_back(mr);
                    trace!("completed id={} offset={} len={}", id, offset, src_len);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::Other,
                        format!("unknown wr_id: {}", id),
                    ));
                }
            }
        }
        Ok(result)
    }
}
