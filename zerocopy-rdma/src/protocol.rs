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
