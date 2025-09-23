use crate::client::{Client, BaseClient};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::io;
use std::net::ToSocketAddrs;

pub struct NaiveClient(BaseClient);

impl Client for NaiveClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseClient::new(ctx, addr)?;
        Ok(Self(base))
    }

    fn request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        let local = self.0.pd.allocate(size)?;
        unsafe {
            self.0
                .qp
                .post_read(&[local.slice_local(..)], self.0.remote, 0)?
        };

        let mut completed = false;
        let mut completions = [ibv_wc::default(); 1];
        while !completed {
            for completion in self.0.cq.poll(&mut completions)? {
                if let Some((e, _)) = completion.error() {
                    panic!("wc error: {:?}", e)
                }
                completed = true;
            }
        }

        local.deregister()
    }
}
