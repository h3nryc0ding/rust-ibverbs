use crate::record::MockRecord;
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
pub struct QueryResponse<const N: usize>([MockRecord; N]);

impl<const N: usize> Default for QueryResponse<N> {
    fn default() -> Self {
        let mut data: [MockRecord; N] = [MockRecord::default(); N];
        for i in 0..N {
            data[i] = MockRecord::new(i)
        }
        Self(data)
    }
}

impl<const N: usize> Deref for QueryResponse<N> {
    type Target = [MockRecord; N];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}
