use crate::client::Client;
use crate::transfer::{SimpleStrategy, TransferStrategy};
use ibverbs::{MemoryRegion, ibv_wc};
use std::io;

impl TransferStrategy for SimpleStrategy {
    fn request(client: &mut Client, size: usize) -> io::Result<Box<[u8]>> {
        let local = client.pd.allocate(size)?;
        unsafe {
            client
                .qp
                .post_read(&[local.slice_local(..)], client.remote, 0)?
        };

        let mut completed = false;
        let mut completions = [ibv_wc::default(); 1];
        while !completed {
            for completion in client.cq.poll(&mut completions)? {
                if let Some((e, _)) = completion.error() {
                    panic!("wc error: {:?}", e)
                }
                completed = true;
            }
        }

        local.deregister()
    }
}
