use crate::memory::Handle;
use crate::record::MockRecord;
use std::cmp::min;
use std::fmt::Debug;
use std::ops::{Deref, RangeBounds};

#[repr(C)]
#[derive(Debug, Copy, Clone)]
pub struct QueryRequest {
    pub offset: usize,
    pub count: usize,
}

impl Default for QueryRequest {
    fn default() -> Self {
        Self {
            offset: 0,
            count: 0,
        }
    }
}

impl QueryRequest {
    pub fn slice(offset: usize, count: usize) -> impl RangeBounds<usize> {
        offset * size_of::<Self>()..(offset + count) * size_of::<Self>()
    }
}

#[repr(C)]
#[derive(Debug)]
pub struct QueryResponseBuffer<const N: usize>([MockRecord; N]);

impl<const N: usize> Default for QueryResponseBuffer<N> {
    fn default() -> Self {
        let mut data: [MockRecord; N] = [MockRecord::default(); N];
        for i in 0..N {
            data[i] = MockRecord::new(i)
        }
        Self(data)
    }
}

impl<const N: usize> Deref for QueryResponseBuffer<N> {
    type Target = [MockRecord; N];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Debug)]
pub struct QueryMeta {
    id: u16,
    count: u16,
}

impl QueryMeta {
    pub fn new(id: u16, count: u16) -> Self {
        Self { id, count }
    }

    pub fn id(&self) -> u16 {
        self.id
    }

    pub fn count(&self) -> u16 {
        self.count
    }
}

impl From<u32> for QueryMeta {
    fn from(value: u32) -> Self {
        Self {
            id: (value & 0xFFFF) as u16,
            count: ((value >> 16) & 0xFFFF) as u16,
        }
    }
}

impl From<QueryMeta> for u32 {
    fn from(value: QueryMeta) -> Self {
        (value.id as u32) | ((value.count as u32) << 16)
    }
}

pub struct QueryResponse<const N: usize> {
    handle: Handle<QueryResponseBuffer<N>>,
    meta: QueryMeta,
}

impl<const N: usize> QueryResponse<N> {
    pub fn new(handle: Handle<QueryResponseBuffer<N>>, meta: QueryMeta) -> Self {
        Self { handle, meta }
    }

    pub fn as_slice(&self) -> &[MockRecord] {
        &self.handle.0[..min(N, self.meta.count as usize)]
    }
}

impl<const N: usize> Deref for QueryResponse<N> {
    type Target = [MockRecord];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl<const N: usize> Debug for QueryResponse<N> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueryResponse")
            .field("meta", &self.meta)
            .field("data", &self.as_slice())
            .finish()
    }
}