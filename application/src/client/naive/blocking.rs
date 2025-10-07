use crate::client::{BaseClient, BlockingClient, ClientConfig};
use bytes::BytesMut;
use ibverbs::{Context, ibv_wc};
use std::io;

pub struct Client(BaseClient);

impl BlockingClient for Client {
    fn new(ctx: Context, cfg: ClientConfig) -> io::Result<Self> {
        Ok(Self(BaseClient::new(ctx, cfg)?))
    }

    fn request(&mut self, bytes: BytesMut) -> io::Result<BytesMut> {
        let mr = self.0.pd.register(bytes)?;
        unsafe { self.0.qps[0].post_read(&[mr.slice_local(..)], self.0.remote, 0)? };

        let mut completed = false;
        let mut completions = [ibv_wc::default(); 1];
        while !completed {
            for completion in self.0.cq.poll(&mut completions)? {
                assert!(completion.is_valid());
                completed = true;
            }
        }

        mr.deregister()
    }
}
