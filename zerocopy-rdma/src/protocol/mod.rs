use bytemuck::{Pod, Zeroable};
use std::ops;

#[derive(Debug, Pod, Zeroable, Copy, Clone, PartialEq)]
#[repr(C)]
pub struct EchoPacket {
    len: usize,
    msg: [u8; 128],
}

impl EchoPacket {
    pub fn new(s: &str) -> Self {
        let mut arr = [0; 128];
        let bytes = s.as_bytes();
        let len = bytes.len().min(arr.len());
        arr[0..len].clone_from_slice(&bytes[..len]);
        Self { msg: arr, len }
    }

    pub fn as_str(&self) -> &str {
        unsafe { std::str::from_utf8_unchecked(&self.msg[..self.len]) }
    }

    pub fn bounds(&self) -> ops::Range<usize> {
        // len + msg[0..len]
        0..(size_of::<usize>() + size_of::<u8>() * self.len)
    }
}
