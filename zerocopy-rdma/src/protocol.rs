use crate::record::MockRecord;
use std::ops::RangeBounds;

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

pub type QueryResponse = &'static [MockRecord];

impl QueryRequest {
    pub fn slice(offset: usize, count: usize) -> impl RangeBounds<usize> {
        offset * size_of::<Self>()..(offset + count) * size_of::<Self>()
    }
}

fn test() {
    let t = QueryResponse::default();
    println!("{:?}", t);
}