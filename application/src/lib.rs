use bincode::config::{Configuration, standard};
use bytes::BytesMut;
use hwlocality::Topology;
use hwlocality::cpu::binding::CpuBindingFlags;
use hwlocality::cpu::cpuset::CpuSet;
use hwlocality::memory::binding::{MemoryBindingFlags, MemoryBindingPolicy};
use hwlocality::object::depth::Depth;
use std::{io, iter};

pub mod args;
pub mod client;
pub mod server;

pub const PORT: u16 = 1337;

pub static KI_B: usize = 1024;
pub static KB: usize = 1000;
pub static MI_B: usize = 1024 * KI_B;
pub static MB: usize = 1000 * KB;
pub static GI_B: usize = 1024 * MI_B;
pub static GB: usize = 1000 * MB;

pub(crate) const BINCODE_CONFIG: Configuration = standard();

fn pin_thread_to_node<const NODE: usize>() -> io::Result<()> {
    let tid = hwlocality::current_thread_id();
    let topology = Topology::new().unwrap();

    let node = topology
        .objects_at_depth(Depth::NUMANode)
        .nth(NODE)
        .unwrap();
    let nodeset = node.nodeset().unwrap();
    let cpuset = CpuSet::from_nodeset(&topology, nodeset);

    topology
        .bind_thread_cpu(tid, &cpuset, CpuBindingFlags::empty())
        .unwrap();

    topology
        .bind_memory(
            &cpuset,
            MemoryBindingPolicy::Bind,
            MemoryBindingFlags::THREAD,
        )
        .unwrap();

    Ok(())
}

pub fn chunks_mut_exact(mut buf: BytesMut, chunk_size: usize) -> impl Iterator<Item = BytesMut> {
    assert_eq!(buf.len() % chunk_size, 0);

    iter::from_fn(move || {
        if buf.is_empty() {
            None
        } else {
            Some(buf.split_to(chunk_size))
        }
    })
}
