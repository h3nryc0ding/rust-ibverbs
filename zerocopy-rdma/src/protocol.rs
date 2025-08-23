use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable)]
pub struct QueryRequest {
    pub offset: usize,
    pub count: usize,
}
