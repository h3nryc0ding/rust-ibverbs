// TODO: remove in final version
#![allow(dead_code, unused_variables)]
pub mod memory;
pub mod transfer;

pub const REQUEST_SIZE_MAX: usize = 1 * 2usize.pow(30);
pub const REQUEST_SIZE_SEED: u64 = 1337;
pub const REQUEST_COUNT: usize = 16;
