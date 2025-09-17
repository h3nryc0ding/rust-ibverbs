use bincode::config::standard;

pub mod client;
pub mod server;
pub mod transfer;

pub static KB: usize = 1024;
pub static MB: usize = 1024 * KB;
pub static GB: usize = 1024 * MB;

pub const SERVER_DATA_SIZE: usize = 2 * GB;
pub const BINCODE_CONFIG: bincode::config::Configuration = standard();

pub const OPTIMAL_MR_SIZE: usize = 4 * MB;
