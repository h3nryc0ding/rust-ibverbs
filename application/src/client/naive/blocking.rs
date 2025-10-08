use crate::client::{BaseClient, BlockingClient};
use bytes::BytesMut;
use ibverbs::{RemoteMemorySlice, ibv_wc};
use std::io;

pub struct Client {
    base: BaseClient,
}

impl Client {
    pub fn new(client: BaseClient) -> io::Result<Self> {
        Ok(Self { base: client })
    }
}

impl BlockingClient for Client {
    fn fetch(&self, bytes: BytesMut, remote: &RemoteMemorySlice) -> io::Result<BytesMut> {
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
