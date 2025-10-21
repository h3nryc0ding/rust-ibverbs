use application::bench::{REMOTE_IP, doubled};
use application::client::{BaseClient, BlockingClient, ideal};
use application::{GI_B, KI_B};
use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ibverbs::RemoteMemorySlice;
use std::time::Duration;
use tokio::time::Instant;

fn blocking(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        for chunk_size in doubled(4 * KI_B, req_size) {
            assert_eq!(req_size % chunk_size, 0);
            let mut group = c.benchmark_group(format!("ideal::blocking::{}", chunk_size));

            let mut client = None::<(ideal::blocking::Client, RemoteMemorySlice)>;
            group.throughput(Throughput::Bytes(req_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(req_size),
                &req_size,
                |b, &size| {
                    b.iter_custom(|iters| {
                        let iters = iters as usize;
                        let config = ideal::blocking::Config {
                            mr_size: chunk_size,
                            mr_count: req_size / chunk_size,
                        };

                        let rebuild = client
                            .as_ref()
                            .map(|(client, _)| *client.config() != config)
                            .unwrap_or(true);

                        if rebuild {
                            let base = BaseClient::new(REMOTE_IP).unwrap();
                            let remote = base.remotes()[0].slice(0..req_size);
                            let new_client = ideal::blocking::Client::new(base, config).unwrap();
                            client = Some((new_client, remote));
                        }
                        let (client, remote) = client.as_mut().unwrap();

                        let mut bytes = vec![BytesMut::zeroed(size); iters];
                        let mut results = Vec::with_capacity(iters);

                        let start = Instant::now();
                        while let Some(bytes) = bytes.pop() {
                            results.push(client.fetch(bytes, &remote).unwrap());
                        }

                        let elapsed = start.elapsed();
                        drop(results);
                        elapsed
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
