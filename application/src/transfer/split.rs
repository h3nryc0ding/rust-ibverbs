use crate::client::Client;
use crate::transfer::{SplitStrategy, TransferStrategy};
use crate::OPTIMAL_MR_SIZE;
use ibverbs::{BorrowedMemoryRegion, MemoryRegion, ibv_wc};
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::io;
use tracing::trace;

impl TransferStrategy for SplitStrategy {
    fn request(client: &mut Client, size: usize) -> io::Result<Box<[u8]>> {
        let mut result: Vec<u8> = Vec::with_capacity(size);
        unsafe { result.set_len(size) };
        let result_ptr = result.as_mut_ptr();

        let chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;
        let mut outstanding_mrs = HashMap::new();
        let mut reg_queue = VecDeque::new();
        let mut post_queue = VecDeque::new();

        for i in 0..chunks {
            let start = i * OPTIMAL_MR_SIZE;
            let end = min(start + OPTIMAL_MR_SIZE, size);
            let len = end - start;
            let ptr = unsafe { result_ptr.add(start) };
            reg_queue.push_back(RegistrationJob {
                id: i as u64,
                ptr,
                len,
            });
        }

        while !reg_queue.is_empty() || !post_queue.is_empty() || !outstanding_mrs.is_empty() {
            if let Some(job) = reg_queue.pop_front() {
                match unsafe { client.pd.register_unchecked::<u8>(job.ptr, job.len) } {
                    Ok(mr) => {
                        trace!(
                            "registered mr id={} ptr={:?} len={}",
                            job.id, job.ptr, job.len
                        );
                        post_queue.push_back(PostJob { id: job.id, mr });
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                        trace!("registration OOM, retrying later");
                        reg_queue.push_back(job);
                    }
                    Err(e) => panic!("register error: {:?}", e),
                }
            }

            if let Some(job) = post_queue.pop_front() {
                let local = job.mr.slice_local(..);
                match unsafe { client.qp.post_read(&[local], client.remote, job.id) } {
                    Ok(_) => {
                        trace!("posted read id={} len={}", job.id, local.len());
                        outstanding_mrs.insert(job.id, job.mr);
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                        trace!("post_read OOM, retrying later");
                        post_queue.push_back(job);
                    }
                    Err(e) => panic!("post_read error: {:?}", e),
                }
            }

            let mut completions = [ibv_wc::default(); 16];
            for completion in client.cq.poll(&mut completions)? {
                if let Some((e, _)) = completion.error() {
                    panic!("wc error: {:?}", e)
                }
                let id = completion.wr_id();
                if let Some(mr) = outstanding_mrs.remove(&id) {
                    let ptr = mr.addr() as *mut u8;
                    let len = mr.len();
                    mr.deregister()?;
                    trace!("registered mr id={} ptr={:?} len={}", &id, ptr, len);
                } else {
                    panic!("unknown wr_id: {}", id);
                }
            }
        }

        Ok(result.into_boxed_slice())
    }
}

struct RegistrationJob {
    id: u64,
    ptr: *mut u8,
    len: usize,
}

struct PostJob<'mr> {
    id: u64,
    mr: BorrowedMemoryRegion<'mr>,
}
