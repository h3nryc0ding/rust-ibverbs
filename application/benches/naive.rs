use application::bench::{REMOTE_IP, doubled};
use application::client::{
    AsyncClient, BaseClient, BlockingClient, NonBlockingClient, RequestHandle, naive,
};
use application::{GI_B, KI_B};
use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::time::Instant;

fn blocking(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        let mut group = c.benchmark_group("naive::blocking");

        group.throughput(Throughput::Bytes(req_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(req_size),
            &req_size,
            |b, &size| {
                let base = BaseClient::new(REMOTE_IP).unwrap();
                let remote = base.remotes()[0].slice(0..req_size);
                let config = naive::blocking::Config;
                let mut client = naive::blocking::Client::new(base, config).unwrap();

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

fn threaded(c: &mut Criterion) {
    for concurrency in 1..10 {
        for req_size in doubled(4 * KI_B, 1 * GI_B) {
            let mut group = c.benchmark_group(format!("naive::threaded::{}", concurrency));

            group.throughput(Throughput::Bytes((req_size) as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(req_size),
                &req_size,
                |b, &size| {
                    let base = BaseClient::new(REMOTE_IP).unwrap();
                    let remote = base.remotes()[0].slice(0..req_size);
                    let config = naive::threaded::Config {
                        concurrency_reg: concurrency,
                        concurrency_dereg: 2,
                    };
                    let client = naive::threaded::Client::new(base, config).unwrap();

                    let mut garbage = Vec::new();
                    b.iter_custom(|iters| {
                        let iters = iters as usize;
                        let mut bytes = (0..iters)
                            .map(|_| BytesMut::zeroed(size))
                            .collect::<Vec<_>>();
                        let mut handles = Vec::with_capacity(iters);

                        let start = Instant::now();
                        while let Some(bytes) = bytes.pop() {
                            handles.push(client.prefetch(bytes, &remote).unwrap());
                        }
                        for handle in &handles {
                            handle.wait_available();
                        }

                        let elapsed = start.elapsed();
                        garbage.extend(handles);
                        elapsed
                    })
                },
            );
            group.finish();
        }
    }
}

fn r#async(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        let mut group = c.benchmark_group("naive::async");

        group.throughput(Throughput::Bytes(req_size as u64));
        group.bench_with_input(
            BenchmarkId::from_parameter(req_size),
            &req_size,
            |b, &size| {
                let rt = tokio::runtime::Runtime::new().unwrap();

                let base = BaseClient::new(REMOTE_IP).unwrap();
                let remote = base.remotes()[0].slice(0..req_size);
                let config = naive::r#async::Config;
                let client =
                    Arc::new(rt.block_on(async {
                        naive::r#async::Client::new(base, config).await.unwrap()
                    }));

                let garbage = Arc::new(Mutex::new(Vec::new()));
                b.to_async(&rt).iter_custom(move |iters| {
                    let client = Arc::clone(&client);
                    let garbage = Arc::clone(&garbage);
                    async move {
                        let iters = iters as usize;
                        let mut bytes = (0..iters)
                            .map(|_| BytesMut::zeroed(size))
                            .collect::<Vec<_>>();
                        let mut futures = Vec::with_capacity(iters);

                        let start = Instant::now();
                        while let Some(bytes) = bytes.pop() {
                            futures.push(client.prefetch(bytes, &remote));
                        }
                        let results = futures::future::join_all(futures).await;
                        for result in &results {
                            assert!(result.is_ok());
                        }

                        let elapsed = start.elapsed();
                        garbage.lock().unwrap().extend(results);
                        elapsed
                    }
                })
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
    targets = blocking, threaded, r#async
);
criterion_main!(benches);
