use crate::client::{AsyncClient, BaseClient, BlockingClient, NonBlockingClient, RequestHandle};
use crate::{GI_B, KI_B, sequence_multiplied};
use bytes::BytesMut;
use futures::task::noop_waker;
use std::collections::VecDeque;
use std::task::Context;
use std::time::{Duration, Instant};
use std::{io, net};

const MAX_INFLIGHT_MEM: usize = 10 * GI_B;
const MAX_INFLIGHT_OPS: usize = 4 * 1024;

#[derive(Debug, clap::Parser)]
pub struct BaseCLI {
    pub addr: net::IpAddr,

    #[arg(long, default_value_t = false)]
    pub skip_validation: bool,

    #[arg(long, default_value_t = false)]
    pub skip_latency: bool,

    #[arg(long, default_value_t = false)]
    pub skip_throughput: bool,

    #[arg(long, default_value_t = 5)]
    pub warmup: usize,

    #[arg(long, default_value_t = 180)]
    pub measure: usize,

    #[arg(long, default_value_t = 4 * KI_B)]
    pub size_min: usize,

    #[arg(long, default_value_t = 1 * GI_B)]
    pub size_max: usize,

    #[arg(long, default_value_t = 2)]
    pub size_multiplier: usize,

    #[arg(long, default_value_t = false)]
    pub logging: bool,

    #[arg(long, default_value_t = false)]
    bench: bool,
}

impl BaseCLI {
    pub fn parse() -> Self {
        let args: BaseCLI = clap::Parser::parse();
        if args.logging {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::TRACE)
                .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
                .with_target(true)
                .compact()
                .init();
        }
        args
    }

    pub fn sizes(&self) -> impl Iterator<Item = usize> {
        sequence_multiplied(self.size_min, self.size_max, self.size_multiplier)
    }

    pub fn warmup(&self) -> Duration {
        Duration::from_secs(self.warmup as u64)
    }

    pub fn measure(&self) -> Duration {
        Duration::from_secs(self.measure as u64)
    }
}

#[derive(Debug)]
pub struct LatencyStats {
    pub p50: Duration,
    pub p90: Duration,
    pub p95: Duration,
    pub p99: Duration,
    pub p99_9: Duration,
    pub p99_99: Duration,
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,

    pub count: usize,
    pub duration: Duration,
}

impl LatencyStats {
    fn from_latencies(latencies: &mut [Duration]) -> LatencyStats {
        latencies.sort();

        let count = latencies.len() as f64;
        let p50 = latencies[(count * 0.50).floor() as usize];
        let p90 = latencies[(count * 0.90).floor() as usize];
        let p95 = latencies[(count * 0.95).floor() as usize];
        let p99 = latencies[(count * 0.99).floor() as usize];
        let p99_9 = latencies[(count * 0.999).floor() as usize];
        let p99_99 = latencies[(count * 0.9999).floor() as usize];

        let min = latencies[0];
        let max = latencies[count as usize - 1];

        let sum = latencies.iter().sum::<Duration>();
        let mean = sum / count as u32;

        let duration = latencies.iter().sum::<Duration>();
        Self {
            p50,
            p90,
            p95,
            p99,
            p99_9,
            p99_99,
            min,
            max,
            mean,
            count: count as usize,
            duration,
        }
    }
}

#[derive(Debug)]
pub struct ThroughputStats {
    pub gib_s: f64,
    pub op_s: f64,

    pub bytes: usize,
    pub count: usize,
    pub duration: Duration,
}

impl ThroughputStats {
    fn from_latencies(size: usize, latencies: &[Duration]) -> ThroughputStats {
        let count = latencies.len();
        let duration = latencies.iter().sum::<Duration>();

        Self::from_count(size, count, duration)
    }

    fn from_count(size: usize, count: usize, duration: Duration) -> ThroughputStats {
        let bytes = count * size;

        let gib = bytes as f64 / GI_B as f64;
        let gib_s = gib / duration.as_secs_f64();
        let op_s = count as f64 / duration.as_secs_f64();

        Self {
            gib_s,
            op_s,
            bytes,
            count,
            duration,
        }
    }
}

pub fn blocking<C: BlockingClient, F>(cli: &BaseCLI, mut f: F) -> io::Result<()>
where
    F: FnMut(BaseClient, usize) -> io::Result<C>,
{
    for size in cli.sizes() {
        let base = BaseClient::new(cli.addr)?;
        let remote = base.remotes[0].slice(0..size);
        let mut client = f(base, size)?;
        println!("size: {size}, config: {:?}", client.config());

        if !cli.skip_validation {
            let bytes = BytesMut::zeroed(size);
            let result = client.fetch(bytes, &remote)?;
            validate(&result)?;
        }

        if cli.skip_latency && cli.skip_throughput {
            return Ok(());
        }

        let mut bytes = BytesMut::zeroed(size);
        let start = Instant::now();
        while start.elapsed() < cli.warmup() {
            bytes = client.fetch(bytes, &remote)?;
        }

        let mut latencies = Vec::new();
        let start = Instant::now();
        while start.elapsed() < cli.measure() {
            let start = Instant::now();
            bytes = client.fetch(bytes, &remote)?;
            let end = Instant::now();

            latencies.push(end - start);
        }

        if !cli.skip_latency {
            let stats = LatencyStats::from_latencies(&mut latencies);
            println!("{stats:?}")
        }

        if !cli.skip_throughput {
            let stats = ThroughputStats::from_latencies(size, &mut latencies);
            println!("{stats:?}")
        }
    }

    Ok(())
}

pub fn non_blocking<C: NonBlockingClient, F>(cli: &BaseCLI, mut f: F) -> io::Result<()>
where
    F: FnMut(BaseClient, usize) -> io::Result<C>,
{
    for size in cli.sizes() {
        let base = BaseClient::new(cli.addr)?;
        let remote = base.remotes[0].slice(0..size);
        let client = f(base, size)?;
        println!("size: {size}, config: {:?}", client.config());

        if !cli.skip_validation {
            let bytes = BytesMut::zeroed(size);
            let handle = client.prefetch(bytes, &remote)?;
            let result = handle.acquire()?;
            validate(&result)?;
        }

        if !cli.skip_latency {
            let mut bytes = BytesMut::zeroed(size);
            let start = Instant::now();
            while start.elapsed() < cli.warmup() {
                let handle = client.prefetch(bytes, &remote)?;
                bytes = handle.acquire()?;
            }

            let mut latencies = Vec::new();
            let start = Instant::now();
            while start.elapsed() < cli.measure() {
                let start = Instant::now();
                let handle = client.prefetch(bytes, &remote)?;
                handle.wait_available();
                let end = Instant::now();

                bytes = handle.acquire()?;
                latencies.push(end - start);
            }

            let stats = LatencyStats::from_latencies(&mut latencies);
            println!("{stats:?}")
        }

        if !cli.skip_throughput {
            let inflight = MAX_INFLIGHT_OPS.min(MAX_INFLIGHT_MEM / size).max(1);

            let mut bytes = (0..inflight)
                .map(|_| BytesMut::zeroed(size))
                .collect::<VecDeque<_>>();
            let mut handles = VecDeque::with_capacity(inflight);

            let start = Instant::now();
            while start.elapsed() < cli.warmup() {
                if let Some(bytes) = bytes.pop_front() {
                    let handle = client.prefetch(bytes, &remote)?;
                    handles.push_back(handle);
                }
                if let Some(handle) = handles.front() {
                    if handle.is_acquirable() {
                        let handle = handles.pop_front().unwrap();
                        bytes.push_back(handle.acquire()?);
                    }
                }
            }
            for handle in handles.drain(..) {
                bytes.push_back(handle.acquire()?);
            }

            assert_eq!(bytes.len(), inflight);
            assert_eq!(handles.len(), 0);

            let start = Instant::now();
            let mut completed = 0;
            while start.elapsed() < cli.measure() {
                if let Some(bytes) = bytes.pop_front() {
                    let handle = client.prefetch(bytes, &remote)?;
                    handles.push_back(handle);
                }
                if let Some(handle) = handles.front() {
                    if handle.is_acquirable() {
                        completed += 1;
                        let handle = handles.pop_front().unwrap();
                        bytes.push_back(handle.acquire()?);
                    }
                }
            }
            // Handles are *expected* to finish FIFO,
            // however internal concurrency *may* reorder requests slightly.
            // By checking from right to left we are trying to avoid more
            // requests finishing at the beginning of the queue
            handles.iter().rev().for_each(|handle| {
                if handle.is_acquirable() {
                    completed += 1;
                }
            });
            let stop = Instant::now();

            let stats = ThroughputStats::from_count(size, completed, stop - start);
            println!("{stats:?}")
        }
    }

    Ok(())
}

pub async fn r#async<C: AsyncClient, F, Fut>(cli: &BaseCLI, mut f: F) -> io::Result<()>
where
    F: FnMut(BaseClient, usize) -> Fut,
    Fut: Future<Output = io::Result<C>>,
{
    for size in cli.sizes() {
        let base = BaseClient::new(cli.addr)?;
        let remote = base.remotes[0].slice(0..size);
        let client = f(base, size).await?;
        println!("size: {size}, config: {:?}", client.config());

        if !cli.skip_validation {
            let bytes = BytesMut::zeroed(size);
            let result = client.prefetch(bytes, &remote).await?;
            validate(&result)?;
        }

        if !cli.skip_latency {
            let mut bytes = BytesMut::zeroed(size);
            let start = Instant::now();
            while start.elapsed() < cli.warmup() {
                bytes = client.prefetch(bytes, &remote).await?;
            }

            let mut latencies = Vec::new();
            let start = Instant::now();
            while start.elapsed() < cli.measure() {
                let start = Instant::now();
                bytes = client.prefetch(bytes, &remote).await?;
                let end = Instant::now();

                latencies.push(end - start);
            }

            let stats = LatencyStats::from_latencies(&mut latencies);
            println!("{stats:?}")
        }

        if !cli.skip_throughput {
            let inflight = MAX_INFLIGHT_OPS.min(MAX_INFLIGHT_MEM / size).max(1);

            let waker = noop_waker();
            let mut cx = Context::from_waker(&waker);

            let mut bytes = (0..inflight)
                .map(|_| BytesMut::zeroed(size))
                .collect::<VecDeque<_>>();
            let mut futures = VecDeque::with_capacity(inflight);

            let start = Instant::now();
            while start.elapsed() < cli.warmup() {
                if futures.len() < inflight {
                    if let Some(bytes) = bytes.pop_front() {
                        futures.push_back(Box::pin(client.prefetch(bytes, &remote)));
                    }
                }
                if let Some(future) = futures.front_mut() {
                    if future.as_mut().poll(&mut cx).is_ready() {
                        let future = futures.pop_front().unwrap();
                        bytes.push_back(future.await?);
                    }
                }
            }

            let mut completed = 0;
            let start = Instant::now();
            while start.elapsed() < cli.measure() {
                if futures.len() < inflight {
                    if let Some(bytes) = bytes.pop_front() {
                        futures.push_back(Box::pin(client.prefetch(bytes, &remote)));
                    }
                }
                if let Some(future) = futures.front_mut() {
                    if future.as_mut().poll(&mut cx).is_ready() {
                        let future = futures.pop_front().unwrap();
                        bytes.push_back(future.await?);
                        completed += 1;
                    }
                }
            }
            // Handles are *expected* to finish FIFO,
            // however internal concurrency *may* reorder requests slightly.
            // By checking from right to left we are trying to avoid more
            // requests finishing at the beginning of the queue
            futures.iter_mut().rev().for_each(|future| {
                if future.as_mut().poll(&mut cx).is_ready() {
                    completed += 1;
                }
            });
            let stop = Instant::now();

            let stats = ThroughputStats::from_count(size, completed, stop - start);
            println!("{stats:?}")
        }
    }

    Ok(())
}

fn validate(bytes: &[u8]) -> io::Result<()> {
    for (offset, byte) in bytes.iter().enumerate() {
        let expected = offset as u8;
        if *byte != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "Validation failed at offset {offset} (got {byte:#x}, expected {expected:#x})"
                ),
            ));
        }
    }
    Ok(())
}
