use application::bench::{REMOTE_IP, doubled};
use application::client::{BaseClient, NonBlockingClient, RequestHandle, naive};
use application::{GI_B, KI_B};
use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;
use tokio::time::Instant;

fn threaded(c: &mut Criterion) {
    for concurrency in 1..10 {
        for req_size in doubled(4 * KI_B, 1 * GI_B) {
            let mut group = c.benchmark_group(format!("naive::threaded::{}", concurrency));

            let base = BaseClient::new(REMOTE_IP).unwrap();
            let remote = base.remotes()[0].slice(0..req_size);
            let config = naive::threaded::Config {
                concurrency_reg: concurrency,
                concurrency_dereg: 2,
            };
            let client = naive::threaded::Client::new(base, config).unwrap();
            group.throughput(Throughput::Bytes(req_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(req_size),
                &req_size,
                |b, &size| {
                    b.iter_custom(|iters| {
                        let iters = iters as usize;

                        let mut bytes = vec![BytesMut::zeroed(size); iters];
                        let mut handles = Vec::with_capacity(iters);

                        let start = Instant::now();
                        while let Some(bytes) = bytes.pop() {
                            handles.push(client.prefetch(bytes, &remote).unwrap());
                        }
                        for handle in &handles {
                            handle.wait_available();
                        }

                        let elapsed = start.elapsed();
                        drop(handles);
                        elapsed
                    })
                },
            );
            group.finish();
        }
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(10));
    targets = threaded
);
criterion_main!(benches);
