#![allow(dead_code)]

use ibverbs::{CompletionQueue, ibv_wc};
use std::io;

pub mod client;
mod memory;
pub mod server;

#[repr(C)]
#[derive(Debug, Default, Clone)]
pub struct ServerMeta {
    size: usize,
}

#[repr(C)]
#[derive(Debug, Default, Clone)]
pub struct ClientMeta {
    addr: usize,
    rkey: usize,
}

fn await_completions<const N: usize>(cq: &mut CompletionQueue) -> io::Result<()> {
    let mut c = N;
    let mut completions = [ibv_wc::default(); N];
    while c > 0 {
        for completion in cq.poll(&mut completions)? {
            assert!(completion.is_valid());
            c -= 1;
        }
    }

    Ok(())
}
