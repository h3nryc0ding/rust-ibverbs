use application::bench::{REMOTE_IP, doubled};
use application::client::{AsyncClient, BaseClient, copy};
use application::{GI_B, KI_B};
use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

fn r#async(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        for chunk_size in doubled(4 * KI_B, req_size) {
            assert_eq!(req_size % chunk_size, 0);
            let mut group = c.benchmark_group(format!("copy::async::{}", chunk_size));
            let rt = tokio::runtime::Runtime::new().unwrap();

            let base = BaseClient::new(REMOTE_IP).unwrap();
            let remote = base.remotes()[0].slice(0..req_size);
            let config = copy::r#async::Config {
                mr_size: chunk_size,
                mr_count: req_size / chunk_size,
            };
            let client = Arc::new(
                rt.block_on(async { copy::r#async::Client::new(base, config).await.unwrap() }),
            );
            group.throughput(Throughput::Bytes(req_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(req_size),
                &req_size,
                |b, &size| {
                    let client = client.clone();
                    b.to_async(&rt).iter_custom(move |iters| {
                        let client = client.clone();
                        async move {
                            let iters = iters as usize;

                            let mut bytes = vec![BytesMut::zeroed(size); iters];
                            let mut futures = Vec::with_capacity(iters);

                            let start = Instant::now();
                            while let Some(bytes) = bytes.pop() {
                                futures.push(client.prefetch(bytes, &remote))
                            }
                            let results = futures::future::join_all(futures).await;
                            for result in &results {
                                assert!(result.is_ok());
                            }

                            let elapsed = start.elapsed();
                            drop(results);
                            elapsed
                        }
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
    targets = r#async
);
criterion_main!(benches);
