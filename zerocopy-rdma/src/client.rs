use crate::protocol::QueryRequest;
use crate::rdma::Connection;
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol};
use std::{io, time};

const REQUESTS: usize = 100_000;

pub fn run<P: Protocol>(c: Connection) -> io::Result<()> {
    let mut client = P::Client::new(c)?;

    let start = time::Instant::now();
    let mut records = 0;
    for i in 0..REQUESTS {
        let req = QueryRequest {
            offset: 0,
            limit: i as u64,
        };
        let res = client.request(req)?;
        assert!(res.iter().all(MockRecord::validate));
        records += i
    }
    let duration = start.elapsed();

    println!("Sent {REQUESTS} requests in {duration:?}");
    println!("Average time per request: {:?}", duration / REQUESTS as u32);
    println!(
        "Average throughput: {:.2} requests/sec",
        REQUESTS as f64 / duration.as_secs_f64()
    );
    println!(
        "Average throughput: {:.2} GB/sec",
        REQUESTS as f64 * records as f64 * size_of::<MockRecord>() as f64
            / duration.as_secs_f64()
            / 1_000_000_000.0
    );
    Ok(())
}
