use bytemuck::{Pod, Zeroable};

#[repr(C)]
#[derive(Debug, Copy, Clone, Pod, Zeroable)]
pub struct QueryRequest {
    pub offset: u64,
    pub limit: u64,
}

