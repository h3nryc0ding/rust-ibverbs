use bincode::config::standard;

pub mod client;
mod memory;
pub mod server;

pub static KB: usize = 1024;
pub static MB: usize = 1024 * KB;
pub static GB: usize = 1024 * MB;

pub const SERVER_DATA_SIZE: usize = 16 * GB;
pub const BINCODE_CONFIG: bincode::config::Configuration = standard();
