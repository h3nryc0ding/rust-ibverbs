use application::bench::{REMOTE_IP, doubled};
use application::client::{BaseClient, BlockingClient, ideal};
use application::{GI_B, KI_B};
use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;
use tokio::time::Instant;

fn blocking(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        for chunk_size in doubled(4 * KI_B, req_size) {
            assert_eq!(req_size % chunk_size, 0);
            let mut group = c.benchmark_group(format!("ideal::blocking::{}", chunk_size));

            group.throughput(Throughput::Bytes(req_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(req_size),
                &req_size,
                |b, &size| {
                    let base = BaseClient::new(REMOTE_IP).unwrap();
                    let remote = base.remotes()[0].slice(0..req_size);
                    let config = ideal::blocking::Config {
                        mr_size: chunk_size,
                        mr_count: req_size / chunk_size,
                    };
                    let mut client = ideal::blocking::Client::new(base, config).unwrap();

                    let mut garbage = Vec::new();
                    b.iter_custom(|iters| {
                        let iters = iters as usize;
                        let mut bytes = (0..iters)
                            .map(|_| BytesMut::zeroed(size))
                            .collect::<Vec<_>>();

                        let start = Instant::now();
                        for bytes in bytes.drain(..) {
                            let res = client.fetch(bytes, &remote).unwrap();
                            garbage.push(res);
                        }
                        start.elapsed()
                    });
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
    targets = blocking
);
criterion_main!(benches);
