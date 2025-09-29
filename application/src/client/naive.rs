use crate::client::{BaseClient, BlockingClient, ClientConfig};
use ibverbs::{Context, ibv_wc};
use std::io;

pub struct NaiveClient(BaseClient);

impl BlockingClient for NaiveClient {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        let base = BaseClient::new(ctx, cfg)?;
        Ok(Self(base))
    }

    fn request(&mut self, dst: &mut [u8]) -> io::Result<()> {
        let local = unsafe { self.0.pd.register(dst)? };
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

        Ok(())
    }
}
