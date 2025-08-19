use bytemuck::{Pod, Zeroable};
use md5::{Digest, Md5};

#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable)]
pub struct MockRecord {
    pub id: u64,
    pub checksum: u64,
    pub payload: [u8; 1024],
}

impl MockRecord {
    pub fn new(id: u64) -> Self {
        let payload = [id as u8; 1024];
        let checksum = Self::checksum(&payload);
        MockRecord {
            id,
            checksum,
            payload,
        }
    }

    fn checksum(payload: &[u8; 1024]) -> u64 {
        let mut hasher = Md5::new();
        hasher.update(payload);
        let result = hasher.finalize();
        u64::from_le_bytes(result[0..8].try_into().unwrap())
    }

    pub fn validate(&self) -> bool {
        Self::checksum(&self.payload) == self.checksum
    }
}
