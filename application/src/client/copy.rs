use crate::client::{BaseClient, BlockingClient, ClientConfig};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::{hint, io};

const RX_DEPTH: usize = 128;

pub struct CopyClient {
    base: BaseClient,
    mrs: Vec<MemoryRegion>,
}

impl BlockingClient for CopyClient {
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
        let mut chunks: Vec<_> = dst.chunks_mut(mr_size).collect();
        let mut completions = vec![ibv_wc::default(); RX_DEPTH];

        let mut unused: Vec<_> = (0..RX_DEPTH).collect();
        let mut posted = 0;
        let mut received = 0;

        while received < chunks.len() {
            while posted < chunks.len() {
                if let Some(mr_idx) = unused.pop() {
                    let mr = &self.mrs[mr_idx];
                    let local = mr.slice_local(..);

                    let remote_slice = self
                        .base
                        .remote
                        .slice(posted * mr_size..(posted + 1) * mr_size);

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
                        unused.push(mr_idx);
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

                let src_slice = mr.as_slice();
                let dest_slice = &mut chunks[chunk_idx][..];
                dest_slice.copy_from_slice(src_slice);

                unused.push(mr_idx);
                received += 1;
            }
        }
        Ok(())
    }
}
