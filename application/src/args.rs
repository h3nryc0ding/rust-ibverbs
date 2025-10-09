use crate::client::{AsyncClient, BaseClient, BlockingClient, NonBlockingClient};
use crate::{GI_B, MI_B, chunks_mut_exact};
use bytes::BytesMut;
use clap::Parser;
use ibverbs::RemoteMemorySlice;
use std::io;
use std::net::IpAddr;
use std::time::Instant;

#[derive(Debug, Parser)]
pub struct DefaultCLI {
    pub addr: IpAddr,

    #[arg(long, default_value_t = true)]
    pub validate: bool,

    #[arg(long, default_value_t = true)]
    pub latency: bool,

    #[arg(long, default_value_t = true)]
    pub throughput: bool,

    #[arg(short, long, default_value_t = 1000)]
    pub iterations: usize,

    #[arg(short, long, default_value_t = 512 * MI_B)]
    pub size: usize,
}

pub fn bench_blocking<C: BlockingClient>(args: &DefaultCLI, config: C::Config) -> io::Result<()> {
    init_tracing();

    let client = BaseClient::new(args.addr)?;
    let remotes = client.remotes();

    let mut client = C::new(client, config)?;
    let remote = remotes[0].slice(0..512 * MI_B);

    if args.latency {
        latency_blocking(&mut client, &remote, args)?;
    }
    if args.throughput {
        throughput_blocking(&mut client, &remote, args)?;
    }

    if args.validate {
        validate_blocking(&mut client, &remote)?;
    }

    Ok(())
}

pub fn bench_non_blocking<C: NonBlockingClient>(
    args: &DefaultCLI,
    config: C::Config,
) -> io::Result<()> {
    init_tracing();

    let client = BaseClient::new(args.addr)?;
    let remotes = client.remotes();

    let mut client = C::new(client, config)?;
    let remote = remotes[0].slice(0..512 * MI_B);

    if args.latency {
        latency_threaded(&mut client, &remote, args)?;
    }
    if args.throughput {
        throughput_threaded(&mut client, &remote, args)?;
    }

    if args.validate {
        validate_threaded(&mut client, &remote)?;
    }

    Ok(())
}

pub async fn bench_async<C: AsyncClient>(args: &DefaultCLI, config: C::Config) -> io::Result<()> {
    init_tracing();

    let client = BaseClient::new(args.addr)?;
    let remotes = client.remotes();

    let mut client = C::new(client, config).await?;
    let remote = remotes[0].slice(0..512 * MI_B);

    if args.latency {
        latency_async(&mut client, &remote, args).await?;
    }
    if args.throughput {
        throughput_async(&mut client, &remote, args).await?;
    }

    if args.validate {
        validate_async(&mut client, &remote).await?;
    }

    Ok(())
}

fn latency_blocking<C: BlockingClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
    args: &DefaultCLI,
) -> io::Result<()> {
    let mut latencies = Vec::with_capacity(args.iterations);
    for _ in 0..args.iterations {
        let bytes = BytesMut::zeroed(args.size);

        let start = Instant::now();
        let _ = client.fetch(bytes, remote)?;
        let end = Instant::now();

        latencies.push(end - start);
    }

    print_latency(&latencies);
    Ok(())
}

fn throughput_blocking<C: BlockingClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
    args: &DefaultCLI,
) -> io::Result<()> {
    let bytes = BytesMut::zeroed(args.size * args.iterations);

    let start = Instant::now();
    for bytes in chunks_mut_exact(bytes, args.size) {
        let _ = client.fetch(bytes, remote)?;
    }
    let end = Instant::now();

    print_throughput(args.size as f64 * args.iterations as f64, end - start);
    Ok(())
}

fn validate_blocking<C: BlockingClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
) -> io::Result<()> {
    let size = remote.len();

    let bytes = BytesMut::zeroed(size);
    let result = client.fetch(bytes, remote)?;

    validate(&result)?;
    Ok(())
}

fn latency_threaded<C: NonBlockingClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
    args: &DefaultCLI,
) -> io::Result<()> {
    let mut latencies = Vec::with_capacity(args.iterations);
    for _ in 0..args.iterations {
        let bytes = BytesMut::zeroed(args.size);

        let start = Instant::now();
        let _ = client.prefetch(bytes, remote)?.wait();
        let end = Instant::now();

        latencies.push(end - start);
    }

    print_latency(&latencies);
    Ok(())
}

fn throughput_threaded<C: NonBlockingClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
    args: &DefaultCLI,
) -> io::Result<()> {
    let bytes = BytesMut::zeroed(args.size * args.iterations);
    let mut handles = Vec::with_capacity(args.iterations);

    let start = Instant::now();
    for bytes in chunks_mut_exact(bytes, args.size) {
        handles.push(client.prefetch(bytes, remote)?);
    }
    for handle in handles {
        handle.wait()?;
    }
    let end = Instant::now();

    print_throughput(args.iterations as f64 * args.size as f64, end - start);
    Ok(())
}

fn validate_threaded<C: NonBlockingClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
) -> io::Result<()> {
    let size = remote.len();

    let bytes = BytesMut::zeroed(size);
    let result = client.prefetch(bytes, remote)?.wait()?;

    validate(&result)?;
    Ok(())
}

async fn latency_async<C: AsyncClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
    args: &DefaultCLI,
) -> io::Result<()> {
    let mut latencies = Vec::with_capacity(args.iterations);
    for _ in 0..args.iterations {
        let bytes = BytesMut::zeroed(args.size);

        let start = Instant::now();
        let _ = client.prefetch(bytes, remote).await?;
        let end = Instant::now();

        latencies.push(end - start);
    }

    print_latency(&latencies);
    Ok(())
}

async fn throughput_async<C: AsyncClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
    args: &DefaultCLI,
) -> io::Result<()> {
    let bytes = BytesMut::zeroed(args.size * args.iterations);
    let mut futures = Vec::with_capacity(args.iterations);

    let start = Instant::now();
    for bytes in chunks_mut_exact(bytes, args.size) {
        futures.push(client.prefetch(bytes, remote));
    }
    futures::future::join_all(futures).await;
    let end = Instant::now();

    print_throughput(args.size as f64 * args.iterations as f64, end - start);
    Ok(())
}

async fn validate_async<C: AsyncClient>(
    client: &mut C,
    remote: &RemoteMemorySlice,
) -> io::Result<()> {
    let size = remote.len();

    let bytes = BytesMut::zeroed(size);
    let result = client.prefetch(bytes, remote).await?;

    validate(&result)?;
    Ok(())
}

fn print_latency(latencies: &[std::time::Duration]) {
    if latencies.is_empty() {
        println!("Latency: No data recorded.");
        return;
    }
    let iterations = latencies.len();
    let avg = latencies.iter().map(|d| d.as_secs_f64()).sum::<f64>() / iterations as f64;
    let min = latencies
        .iter()
        .map(|d| d.as_secs_f64())
        .fold(f64::INFINITY, f64::min);
    let max = latencies
        .iter()
        .map(|d| d.as_secs_f64())
        .fold(f64::NEG_INFINITY, f64::max);

    println!(
        "Latency: avg = {:.3} µs, min = {:.3} µs, max = {:.3} µs",
        avg * 1e6,
        min * 1e6,
        max * 1e6
    );
}

fn print_throughput(bytes: f64, duration: std::time::Duration) {
    let secs = duration.as_secs_f64();
    let gbps = bytes / GI_B as f64 / secs;

    println!(
        "Throughput: {:.2} GiB/s (transferred {} bytes in {:.3}s",
        gbps, bytes, secs
    );
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
    println!("Validation passed with {} bytes", bytes.len());
    Ok(())
}

fn init_tracing() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .with_thread_ids(true)
        .compact()
        .init();
}
