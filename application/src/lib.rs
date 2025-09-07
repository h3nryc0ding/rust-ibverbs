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
            if let Some(err) = completion.error() {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    format!("Work completion error: {:?}", err),
                ));
            }
            c -= 1;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn server_meta_size() {
        assert_eq!(size_of::<ServerMeta>(), 8);
    }

    fn client_meta_size() {
        assert_eq!(size_of::<ClientMeta>(), 16);
    }
}
