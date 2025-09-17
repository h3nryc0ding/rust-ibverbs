use crate::client::ConcurrentClient;
use crate::transfer::{ConcurrentTransferStrategy, PipelineStrategy};
use crate::OPTIMAL_MR_SIZE;
use ibverbs::BorrowedMemoryRegion;
use std::io;
use std::sync::mpsc;

impl ConcurrentTransferStrategy for PipelineStrategy {
    fn request(client: &mut ConcurrentClient, size: usize) -> io::Result<Box<[u8]>> {
        let result = vec![0u8; size].into_boxed_slice();
        let chunks = (size + OPTIMAL_MR_SIZE - 1) / OPTIMAL_MR_SIZE;

        let (reg_tx, reg_rx) = mpsc::channel::<RegistrationJob>();
        let (post_tx, post_rx) = mpsc::channel::<PostJob>();
        let (comp_tx, comp_rx) = mpsc::channel::<CompletionJob>();
        let (done_tx, done_rx) = mpsc::channel::<()>();

        todo!()
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

struct CompletionJob<'mr> {
    id: u64,
    mr: BorrowedMemoryRegion<'mr>,
}
