use crate::memory::ManagedMemoryRegion;
use std::fmt::Debug;

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct QueryRequest {
    pub offset: usize,
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self { offset: 0 }
    }
}

pub type QueryResponse<'a> = ManagedMemoryRegion<'a>;

#[derive(Debug)]
#[repr(C)]
pub struct QueryMeta {
    size: u32,
}

impl QueryMeta {
    pub fn new(size: u32) -> Self {
        Self { size }
    }

    pub fn size(&self) -> u32 {
        self.size
    }
}

impl From<u32> for QueryMeta {
    fn from(value: u32) -> Self {
        Self { size: value }
    }
}

impl From<QueryMeta> for u32 {
    fn from(value: QueryMeta) -> Self {
        value.size
    }
}
