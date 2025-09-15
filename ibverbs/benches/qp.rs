use criterion::BatchSize::PerIteration;
use criterion::{criterion_group, criterion_main, Criterion};
use ffi::{ibv_qp_type, ibv_wc};
use ibverbs::CompletionQueue;
use std::hint;
use std::time::Duration;

pub fn benchmark(c: &mut Criterion) {
    static KB: usize = 1024;
    static MB: usize = 1024 * KB;
    static GB: usize = 1024 * MB;

    let ctx = ibverbs::devices()
        .unwrap()
        .iter()
        .next()
        .unwrap()
        .open()
        .unwrap();
    let pd = ctx.alloc_pd().unwrap();
    let cq = ctx.create_cq(2i32.pow(16), 0).unwrap();
    let mut qp = {
        let pqp = pd
            .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)
            .unwrap()
            .allow_remote_rw()
            .build()
            .unwrap();
        let local = pqp.endpoint().unwrap();
        pqp.handshake(local).unwrap()
    };
    let mr = pd.allocate::<u8>(4 * GB).unwrap();
    let remote = mr.remote();

    let mut group = c.benchmark_group("QueuePair");
    for size in [
        4 * KB,
        16 * KB,
        64 * KB,
        256 * KB,
        1 * MB,
        2 * MB,
        4 * MB,
        8 * MB,
        16 * MB,
        128 * MB,
        512 * MB,
        1 * GB,
    ] {
        group.bench_function(format!("{} KB post_read", size / KB), |b| {
            const BATCH_SIZE: usize = 128;

            b.iter_batched(
                || {
                    let mut mrs = Vec::with_capacity(BATCH_SIZE);
                    for _ in 0..BATCH_SIZE {
                        mrs.push(pd.allocate::<u8>(size).unwrap());
                    }
                    mrs
                },
                |mrs| unsafe {
                    let mut done = 0;
                    let mut completions = [ibv_wc::default(); BATCH_SIZE];

                    for (idx, mr) in mrs.iter().enumerate() {
                        let local_slice = mr.slice(..);
                        let remote_slice = remote.slice(idx * size..(idx + 1) * size);
                        while let Err(_) = qp.post_read(&[local_slice], remote_slice, idx as u64) {
                            done += poll(&cq, &mut completions[done..]);
                            hint::spin_loop();
                        }
                    }

                    while done < BATCH_SIZE {
                        done += poll(&cq, &mut completions[done..]);
                        hint::spin_loop();
                    }

                    fn poll(cq: &CompletionQueue, completions: &mut [ibv_wc]) -> usize {
                        let mut done = 0;
                        for completion in cq.poll(completions).unwrap() {
                            if let Some(e) = completion.error() {
                                panic!("{:?}", e);
                            }
                            done += 1;
                        }
                        done
                    }

                    mrs
                },
                PerIteration,
            )
        });
    }
}

criterion_group!(
    name = benches;
    config = Criterion::default()
    .measurement_time(Duration::from_secs(10))
    .warm_up_time(Duration::from_secs(10))
    ;
    targets = benchmark
);
criterion_main!(benches);
