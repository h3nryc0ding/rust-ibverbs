use std::ops;

pub trait BoundedPacket {
    fn bounds(&self) -> ops::Range<usize>;
}

#[derive(Debug, PartialEq, Clone)]
#[repr(C)]
pub struct DataPacket {
    len: usize,
    msg: [u8; 128],
}

impl DataPacket {
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
}

impl BoundedPacket for DataPacket {
    fn bounds(&self) -> ops::Range<usize> {
        // len + msg[0..len]
        0..(size_of::<usize>() + size_of::<u8>() * self.len)
    }
}

#[derive(Debug, PartialEq)]
#[repr(C)]
pub struct MetaPacket {
    pub status: Status,
    pub id: usize,
}

impl MetaPacket {
    pub fn is_ready(&self) -> bool {
        self.status == Status::Ready
    }

    pub fn is_empty(&self) -> bool {
        self.status == Status::Empty
    }
}

impl BoundedPacket for MetaPacket {
    fn bounds(&self) -> ops::Range<usize> {
        // status + id
        0..(size_of::<Status>() + size_of::<usize>())
    }
}

#[repr(usize)]
#[derive(Debug, PartialEq)]
pub enum Status {
    Empty = 0,
    Ready = 1,
    Error = 2,
}
