use crate::OPTIMAL_MR_SIZE;
use crate::client::Client;
use crate::transfer::{CopyStrategy, TransferStrategy};
use ibverbs::{MemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;
use tracing::trace;

const RX_DEPTH: usize = 4;

impl TransferStrategy for CopyStrategy {
    fn request(client: &mut Client, size: usize) -> io::Result<Box<[u8]>> {
        let mut result = vec![0u8; size].into_boxed_slice();
        let mut mrs = VecDeque::with_capacity(RX_DEPTH);
        while mrs.len() < RX_DEPTH {
            if let Ok(mr) = client.pd.allocate::<u8>(OPTIMAL_MR_SIZE) {
                mrs.push_back(mr);
            }
        }
        let total_chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
        let mut next_chunk = 0usize;

        let mut outstanding = HashMap::new();
        let mut completions = [ibv_wc::default(); 32];

        while next_chunk < total_chunks || !outstanding.is_empty() {
            // free MRs AND chunks left
            if next_chunk < total_chunks {
                if let Some(mr) = mrs.pop_front() {
                    let local = mr.slice_local(..);

                    let id = next_chunk as u64;
                    match unsafe { client.qp.post_read(&[local], client.remote, id) } {
                        Ok(_) => {
                            trace!("posted read id={} len={}", id, local.len());
                            outstanding.insert(id, mr);
                            next_chunk += 1;
                        }
                        Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                            trace!("post_read OOM, retrying later");
                            mrs.push_back(mr);
                        }
                        Err(e) => return Err(e),
                    }
                }
            }

            for completion in client.cq.poll(&mut completions)? {
                if let Some((e, _)) = completion.error() {
                    return Err(io::Error::new(io::ErrorKind::Other, format!("wc error: {:?}", e)));
                }

                let id = completion.wr_id();
                if let Some(mr) = outstanding.remove(&id) {

                    let src = mr.as_slice();
                    let src_len = src.len();
                    let offset = (id as usize) * OPTIMAL_MR_SIZE;
                    let end   = (offset + src_len).min(result.len());
                    let dst   = &mut result[offset..end];
                    dst.copy_from_slice(&src[..dst.len()]);

                    mrs.push_back(mr);
                    trace!("completed id={} offset={} len={}", id, offset, src_len);
                } else {
                    return Err(io::Error::new(io::ErrorKind::Other,
                                              format!("unknown wr_id: {}", id)));
                }
            }
        }
        Ok(result)
    }
}
