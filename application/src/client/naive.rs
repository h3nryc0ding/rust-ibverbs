use crate::client::{BaseClient, Client};
use ibverbs::{Context, MemoryRegion, ibv_wc};
use std::io;
use std::net::ToSocketAddrs;

pub struct NaiveClient(BaseClient<1>);

impl Client for NaiveClient {
    fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let base = BaseClient::new(ctx, addr)?;
        Ok(Self(base))
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let local = self.0.pd.register(dst)?;
        unsafe { self.0.qps[0].post_read(&[local.slice_local(..)], self.0.remote, 0)? };

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

        local.deregister()?;
        Ok(())
    }
}
