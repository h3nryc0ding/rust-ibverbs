use bincode::config::{standard, Configuration};

pub mod client;
pub mod server;

pub static KB: usize = 1024;
pub static MB: usize = 1024 * KB;
pub static GB: usize = 1024 * MB;

pub const BINCODE_CONFIG: Configuration = standard();

pub const SERVER_DATA_SIZE: usize = 2 * GB;
pub const OPTIMAL_MR_SIZE: usize = 4 * MB;
