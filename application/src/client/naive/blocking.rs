use crate::client::{BaseClient, BlockingClient};
use bytes::BytesMut;
use ibverbs::{RemoteMemorySlice, ibv_wc};
use std::io;

pub struct Config;

pub struct Client {
    base: BaseClient,
}

impl BlockingClient for Client {
    type Config = Config;

    fn new(client: BaseClient, _: Config) -> io::Result<Self> {
        Ok(Self { base: client })
    }

    fn fetch(&mut self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<BytesMut> {
        assert_eq!(bytes.len(), remote.len());

        let mr = self.base.pd.register(bytes)?;
        let local = mr.slice_local(..).collect::<Vec<_>>();
        unsafe { self.base.qps[0].post_read(&local, remote.slice(..), 0)? };

        let mut completed = false;
        let mut completions = [ibv_wc::default(); 1];
        while !completed {
            for completion in self.base.cq.poll(&mut completions)? {
                assert!(completion.is_valid());
                completed = true;
            }
        }

        mr.deregister()
    }
}
