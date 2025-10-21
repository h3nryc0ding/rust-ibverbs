use application::bench::{REMOTE_IP, doubled};
use application::client::{BaseClient, BlockingClient, naive};
use application::{GI_B, KI_B};
use bytes::BytesMut;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::time::Duration;

fn blocking(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        let mut group = c.benchmark_group("naive::blocking");

        let base = BaseClient::new(REMOTE_IP).unwrap();
        let remote = base.remotes()[0].slice(0..req_size);
        let config = naive::blocking::Config;
        let mut client = naive::blocking::Client::new(base, config).unwrap();
        group.throughput(Throughput::Bytes(req_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(req_size),
            &req_size,
            |b, &size| {
                b.iter_batched(
                    || BytesMut::zeroed(size),
                    |bytes| client.fetch(bytes, &remote).unwrap(),
                    BatchSize::PerIteration,
                );
            },
        );
        group.finish();
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
