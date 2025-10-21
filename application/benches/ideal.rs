use application::client::{BaseClient, BlockingClient, ideal};
use application::{GI_B, KI_B};
use bytes::BytesMut;
use criterion::{BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::iter;
use std::net::{IpAddr, Ipv4Addr};
use std::time::Duration;

static REMOTE_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(141, 76, 47, 9));

fn doubled(start: usize, end: usize) -> impl Iterator<Item = usize> {
    iter::successors(Some(start), move |&current| {
        let next = current * 2;
        if next > end { None } else { Some(next) }
    })
}

fn blocking(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        for chunk_size in doubled(4 * KI_B, req_size) {
            assert_eq!(req_size % chunk_size, 0);
            let mut group = c.benchmark_group(format!("ideal::blocking::{}", chunk_size));

            let base = BaseClient::new(REMOTE_IP).unwrap();
            let remote = base.remotes()[0].slice(0..req_size);
            let config = ideal::blocking::Config {
                mr_size: chunk_size,
                mr_count: req_size / chunk_size,
            };
            let mut client = ideal::blocking::Client::new(base, config).unwrap();
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
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .measurement_time(Duration::from_secs(30))
        .warm_up_time(Duration::from_secs(10));
    targets = blocking
);
criterion_main!(benches);
