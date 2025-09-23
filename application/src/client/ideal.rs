use crate::OPTIMAL_MR_SIZE;
use crate::client::{Client, BaseClient};
use ibverbs::{Context, MemoryRegion, OwnedMemoryRegion, ibv_wc};
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::ToSocketAddrs;
use tracing::{debug, trace};

const RX_DEPTH: usize = 8;

pub struct IdealClient {
    base: BaseClient,
    mrs: VecDeque<OwnedMemoryRegion>,
}

impl IdealClient {
    fn post_read(
        &mut self,
        next_chunk: &mut usize,
        total_chunks: usize,
        outstanding: &mut HashMap<u64, OwnedMemoryRegion>,
    ) -> io::Result<()> {
        if *next_chunk >= total_chunks {
            return Ok(());
        }

        if let Some(mr) = self.mrs.pop_front() {
            let local = mr.slice_local(..);
            let remote = self.base.remote.slice(0..OPTIMAL_MR_SIZE);

            debug!(
                "reading from remote addr={:?} len={} into addr={:?} len={}",
                remote.addr(),
                remote.len(),
                local.addr(),
                local.len()
            );

            let id = *next_chunk as u64;
            match unsafe { self.base.qp.post_read(&[local], remote, id) } {
                Ok(_) => {
                    trace!("posted read id={} len={}", id, local.len());
                    outstanding.insert(id, mr);
                    *next_chunk += 1;
                }
                Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                    trace!("post_read OOM, retry later");
                    self.mrs.push_back(mr);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(())
    }

    fn handle_completion(
        &mut self,
        completion: &ibv_wc,
        outstanding: &mut HashMap<u64, OwnedMemoryRegion>,
    ) -> io::Result<()> {
        if let Some((e, _)) = completion.error() {
            return Err(io::Error::new(
                io::ErrorKind::Other,
                format!("wc error: {:?}", e),
            ));
        }

        let id = completion.wr_id();
        let mr = outstanding.remove(&id).ok_or_else(|| {
            io::Error::new(io::ErrorKind::Other, format!("unknown wr_id: {}", id))
        })?;

        self.mrs.push_back(mr);
        trace!("completed id={}", id);
        Ok(())
    }
}

impl Client for IdealClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseClient::new(ctx, addr)?;
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
        let result = vec![0u8; size].into_boxed_slice();
        let total_chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
        let mut next_chunk = 0usize;

        let mut outstanding = HashMap::new();
        let mut completions = [ibv_wc::default(); RX_DEPTH];

        while next_chunk < total_chunks || !outstanding.is_empty() {
            if next_chunk < total_chunks {
                self.post_read(&mut next_chunk, total_chunks, &mut outstanding)?;
            }

            for completion in self.base.cq.poll(&mut completions)? {
                self.handle_completion(completion, &mut outstanding)?;
            }
        }
        Ok(result)
    }
}
