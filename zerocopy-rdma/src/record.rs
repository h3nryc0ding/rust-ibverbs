use bytemuck::{Pod, Zeroable};
use md5::{Digest, Md5};
use std::usize;

#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable)]
pub struct MockRecord {
    pub id: usize,
    pub checksum: usize,
    pub payload: [u8; 1024],
}

impl MockRecord {
    pub fn new(id: usize) -> Self {
        let payload = [id as u8; 1024];
        let checksum = Self::checksum(&payload);
        MockRecord {
            id,
            checksum,
            payload,
        }
    }

    fn checksum(payload: &[u8; 1024]) -> usize {
        let mut hasher = Md5::new();
        hasher.update(payload);
        let res = hasher.finalize();

        let mut arr = [0u8; size_of::<usize>()];
        arr.copy_from_slice(&res[..size_of::<usize>()]);
        usize::from_le_bytes(arr)
    }

    pub fn validate(&self) -> bool {
        Self::checksum(&self.payload) == self.checksum
    }
}
