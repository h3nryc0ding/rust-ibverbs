use bincode::config::{Configuration, standard};
use hwlocality::Topology;
use hwlocality::cpu::binding::CpuBindingFlags;
use hwlocality::cpu::cpuset::CpuSet;
use hwlocality::memory::binding::{MemoryBindingFlags, MemoryBindingPolicy};
use hwlocality::object::depth::Depth;
use std::io;

pub mod client;
pub mod server;

pub static KB: usize = 1024;
pub static MB: usize = 1024 * KB;
pub static GB: usize = 1024 * MB;

pub const BINCODE_CONFIG: Configuration = standard();

pub const SERVER_DATA_SIZE: usize = 2 * GB;
pub const OPTIMAL_MR_SIZE: usize = 4 * MB;

pub const OPTIMAL_QP_COUNT: usize = 3;

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
