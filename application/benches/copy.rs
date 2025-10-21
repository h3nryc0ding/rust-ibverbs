use application::bench::{REMOTE_IP, doubled};
use application::client::{
    AsyncClient, BaseClient, BlockingClient, NonBlockingClient, RequestHandle, copy,
};
use application::{GI_B, KI_B};
use bytes::BytesMut;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use ibverbs::RemoteMemorySlice;
use std::cell::RefCell;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::Instant;

fn blocking(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        for chunk_size in doubled(4 * KI_B, req_size) {
            assert_eq!(req_size % chunk_size, 0);
            let mut group = c.benchmark_group(format!("copy::blocking::{chunk_size}"));

            let mut client = None::<(copy::blocking::Client, RemoteMemorySlice)>;
            group.throughput(Throughput::Bytes(req_size as u64));
            group.bench_with_input(
                BenchmarkId::from_parameter(req_size),
                &req_size,
                |b, &size| {
                    b.iter_custom(|iters| {
                        let iters = iters as usize;
                        let config = copy::blocking::Config {
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
                            let new_client = copy::blocking::Client::new(base, config).unwrap();
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

fn threaded(c: &mut Criterion) {
    for concurrency in 1..10 {
        for req_size in doubled(4 * KI_B, 1 * GI_B) {
            for chunk_size in doubled(4 * KI_B, req_size) {
                assert_eq!(req_size % chunk_size, 0);
                let mut group =
                    c.benchmark_group(format!("copy::threaded::{}::{}", concurrency, chunk_size));

                let mut client = None::<(copy::threaded::Client, RemoteMemorySlice)>;
                group.throughput(Throughput::Bytes(req_size as u64));
                group.bench_with_input(
                    BenchmarkId::from_parameter(req_size),
                    &req_size,
                    |b, &size| {
                        b.iter_custom(|iters| {
                            let iters = iters as usize;
                            let config = copy::threaded::Config {
                                mr_size: chunk_size,
                                mr_count: req_size / chunk_size,
                                concurrency,
                            };

                            let rebuild = client
                                .as_ref()
                                .map(|(client, _)| *client.config() != config)
                                .unwrap_or(true);

                            if rebuild {
                                let base = BaseClient::new(REMOTE_IP).unwrap();
                                let remote = base.remotes()[0].slice(0..req_size);
                                let new_client = copy::threaded::Client::new(base, config).unwrap();
                                client = Some((new_client, remote));
                            }
                            let (client, remote) = client.as_ref().unwrap();

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
}

fn r#async(c: &mut Criterion) {
    for req_size in doubled(4 * KI_B, 1 * GI_B) {
        for chunk_size in doubled(4 * KI_B, req_size) {
            assert_eq!(req_size % chunk_size, 0);
            let mut group = c.benchmark_group(format!("copy::async::{}", chunk_size));
            let rt = tokio::runtime::Runtime::new().unwrap();

            let client = Arc::new(RefCell::new(
                None::<(copy::r#async::Client, RemoteMemorySlice)>,
            ));
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
                            let config = copy::r#async::Config {
                                mr_size: chunk_size,
                                mr_count: req_size / chunk_size,
                            };

                            let rebuild = {
                                let cell = client.borrow();
                                cell.as_ref()
                                    .map(|(client, _)| *client.config() != config)
                                    .unwrap_or(true)
                            };

                            if rebuild {
                                let base = BaseClient::new(REMOTE_IP).unwrap();
                                let remote = base.remotes()[0].slice(0..req_size);
                                let new_client =
                                    copy::r#async::Client::new(base, config).await.unwrap();
                                *client.borrow_mut() = Some((new_client, remote));
                            }

                            let (client_owned, remote) = client.borrow_mut().take().unwrap();

                            let mut bytes = vec![BytesMut::zeroed(size); iters];
                            let mut futures = Vec::with_capacity(iters);

                            let start = Instant::now();
                            while let Some(bytes) = bytes.pop() {
                                futures.push(client_owned.prefetch(bytes, &remote));
                            }

                            let results = futures::future::join_all(futures).await;
                            for result in &results {
                                assert!(result.is_ok());
                            }

                            let elapsed = start.elapsed();

                            *client.borrow_mut() = Some((client_owned, remote));

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
    targets = blocking, threaded, r#async
);
criterion_main!(benches);
