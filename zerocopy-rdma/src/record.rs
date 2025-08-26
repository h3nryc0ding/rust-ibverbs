use std::usize;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct MockRecord<const N: usize = 1024> {
    pub id: usize,
    pub checksum: u32,
    pub payload: [u8; N],
}

impl<const N: usize> MockRecord<N> {
    pub fn new(id: usize) -> Self {
        let payload = [id as u8; N];
        let checksum = Self::checksum(&payload);
        MockRecord {
            id,
            checksum,
            payload,
        }
    }

    fn checksum(payload: &[u8; N]) -> u32 {
        crc32fast::hash(payload)
    }

    pub fn validate(&self) -> bool {
        Self::checksum(&self.payload) == self.checksum
    }
}

impl<const N: usize> Default for MockRecord<N> {
    fn default() -> Self {
        let payload = [0; N];
        let checksum = Self::checksum(&payload);
        Self {
            id: 0,
            checksum,
            payload,
        }
    }
}
