use crate::client::{AsyncClient, BlockingClient, NonBlockingClient};
use crate::{GI_B, chunks_mut_exact};
use bytes::BytesMut;
use clap::Parser;
use ibverbs::RemoteMemorySlice;
use std::io;
use std::net::IpAddr;
use std::time::Instant;

#[derive(Debug, Parser)]
pub struct Args {
    pub addr: IpAddr,

    #[arg(long, default_value_t = true)]
    pub validate: bool,

    #[arg(long, default_value_t = true)]
    pub latency: bool,

    #[arg(long, default_value_t = true)]
    pub throughput: bool,
}

pub fn latency_blocking<C: BlockingClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
) -> io::Result<()> {
    let iterations = 10;
    let size = remote.len();

    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let bytes = BytesMut::zeroed(size);

        let start = Instant::now();
        let _ = client.fetch(bytes, remote)?;
        let end = Instant::now();

        latencies.push(end - start);
    }

    print_latency(&latencies);
    Ok(())
}

pub fn throughput_blocking<C: BlockingClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
) -> io::Result<()> {
    let iterations = 10;
    let size = remote.len();

    let bytes = BytesMut::zeroed(size * iterations);

    let start = Instant::now();
    for bytes in chunks_mut_exact(bytes, size) {
        let _ = client.fetch(bytes, remote)?;
    }
    let end = Instant::now();

    print_throughput(size as f64 * iterations as f64, end - start);
    Ok(())
}

pub fn validate_blocking<C: BlockingClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
) -> io::Result<()> {
    let size = remote.len();

    let bytes = BytesMut::zeroed(size);
    let result = client.fetch(bytes, remote)?;

    validate(&result)?;
    Ok(())
}

pub fn latency_threaded<C: NonBlockingClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
) -> io::Result<()> {
    let iterations = 10;
    let size = remote.len();

    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let bytes = BytesMut::zeroed(size);

        let start = Instant::now();
        let _ = client.prefetch(bytes, remote)?.wait();
        let end = Instant::now();

        latencies.push(end - start);
    }

    print_latency(&latencies);
    Ok(())
}

pub fn throughput_threaded<C: NonBlockingClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
) -> io::Result<()> {
    let iterations = 10;
    let size = remote.len();

    let bytes = BytesMut::zeroed(size * iterations);
    let mut handles = Vec::with_capacity(iterations);

    let start = Instant::now();
    for bytes in chunks_mut_exact(bytes, size) {
        handles.push(client.prefetch(bytes, remote)?);
    }
    for handle in handles {
        handle.wait()?;
    }
    let end = Instant::now();

    print_throughput(iterations as f64 * size as f64, end - start);
    Ok(())
}

pub fn validate_threaded<C: NonBlockingClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
) -> io::Result<()> {
    let size = remote.len();

    let bytes = BytesMut::zeroed(size);
    let result = client.prefetch(bytes, remote)?.wait()?;

    validate(&result)?;
    Ok(())
}

pub async fn latency_async<C: AsyncClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
) -> io::Result<()> {
    let iterations = 10;
    let size = remote.len();

    let mut latencies = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let bytes = BytesMut::zeroed(size);

        let start = Instant::now();
        let _ = client.prefetch(bytes, remote).await?;
        let end = Instant::now();

        latencies.push(end - start);
    }

    print_latency(&latencies);
    Ok(())
}

pub async fn throughput_async<C: AsyncClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
) -> io::Result<()> {
    let iterations = 10;
    let size = remote.len();

    let bytes = BytesMut::zeroed(size * iterations);
    let mut futures = Vec::with_capacity(iterations);

    let start = Instant::now();
    for bytes in chunks_mut_exact(bytes, size) {
        futures.push(client.prefetch(bytes, remote));
    }
    futures::future::join_all(futures).await;
    let end = Instant::now();

    print_throughput(size as f64 * iterations as f64, end - start);
    Ok(())
}

pub async fn validate_async<C: AsyncClient>(
    client: &mut C,
    remote: RemoteMemorySlice,
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
