use application::client::{
    AsyncClient, BlockingClient, CopyClient, CopyThreadedClient, IdealClient, NaiveClient,
    NonBlockingClient, PipelineAsyncClient, PipelineClient, PipelineThreadedClient,
};
use application::server::Server;
use application::{GB, KI_B, MI_B, client};
use client::ClientConfig;
use ibverbs::Context;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::{io, thread, time};
use tokio::runtime;
use tracing::{Level, info};

const REQUEST_SIZE: usize = 512 * MI_B;
const REQUEST_COUNT: usize = 100;

#[derive(clap::Parser, Debug)]
#[command(version, about = "Start an RDMA server or connect as a client.")]
struct Args {
    #[arg(requires = "mode")]
    server: Option<IpAddr>,
    #[arg(long, value_enum, requires = "server")]
    mode: Option<Mode>,

    #[arg(short, long, default_value_t = 4096)]
    size_kb: usize,

    #[arg(short, long, default_value_t = 2)]
    qp_count: usize,

    #[arg(short, long, default_value_t = 18515)]
    port: u16,

    #[arg(long, default_value_t = Level::TRACE)]
    log: Level,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Mode {
    Ideal,
    Copy,
    CopyThreaded,
    Naive,
    Pipeline,
    PipelineThreaded,
    PipelineAsync,
}

fn main() -> io::Result<()> {
    let args: Args = clap::Parser::parse();

    tracing_subscriber::fmt()
        .with_max_level(args.log)
        .with_thread_ids(true)
        .compact()
        .init();

    run(args)
}

fn run(args: Args) -> io::Result<()> {
    let ctx = ibverbs::devices()?
        .iter()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No RDMA devices found"))?
        .open()?;

    let listen_addr = format!("0.0.0.0:{}", args.port);
    match args.server {
        None => run_server(ctx, listen_addr),
        Some(addr) => {
            let connect_addr = SocketAddr::new(addr, args.port);
            let cfg = ClientConfig {
                server_addr: connect_addr,
                mr_size: args.size_kb * KI_B,
                qp_count: args.qp_count,
            };
            match args.mode.unwrap() {
                Mode::Naive => run_client_blocking::<NaiveClient>(ctx, cfg),
                Mode::Copy => run_client_blocking::<CopyClient>(ctx, cfg),
                Mode::CopyThreaded => run_client_non_blocking::<CopyThreadedClient>(ctx, cfg),
                Mode::Ideal => run_client_blocking::<IdealClient>(ctx, cfg),
                Mode::Pipeline => run_client_blocking::<PipelineClient>(ctx, cfg),
                Mode::PipelineThreaded => {
                    run_client_non_blocking::<PipelineThreadedClient>(ctx, cfg)
                }
                Mode::PipelineAsync => {
                    runtime::Runtime::new()?
                        .block_on(async_run_client::<PipelineAsyncClient>(ctx, cfg))
                }
            }
        }
    }
}

fn run_server(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<()> {
    let mut server = Server::new(ctx, addr)?;
    info!("Server started. Serving requests...");

    server.serve()
}

fn run_client_blocking<C: BlockingClient>(ctx: Context, cfg: ClientConfig) -> io::Result<()> {
    let mut client = C::new(ctx, cfg)?;
    info!("Client started. Sending requests...");

    let start = time::Instant::now();
    let mut result = vec![0u8; REQUEST_SIZE * REQUEST_COUNT];
    for chunk in result.chunks_exact_mut(REQUEST_SIZE) {
        info!("Sending");
        let _ = client.request(chunk)?;
        info!("Received");
    }

    let duration = start.elapsed();
    validate(result);
    info!(
        "{} GB/s",
        (REQUEST_COUNT * REQUEST_SIZE) as f64 / duration.as_secs_f64() / GB as f64
    );

    Ok(())
}

fn run_client_non_blocking<C: NonBlockingClient>(
    ctx: Context,
    cfg: ClientConfig,
) -> io::Result<()> {
    let mut client = C::new(ctx, cfg)?;
    info!("Client started. Sending requests...");

    let start = time::Instant::now();
    let mut handles = Vec::new();
    let mut result = vec![0u8; REQUEST_SIZE * REQUEST_COUNT];
    for chunk in result.chunks_exact_mut(REQUEST_SIZE) {
        info!("Sending");
        handles.push(client.request(chunk)?);
    }
    for handle in handles {
        handle.wait();
        info!("Received");
    }

    let duration = start.elapsed();
    validate(result);
    info!(
        "{} GB/s",
        (REQUEST_COUNT * REQUEST_SIZE) as f64 / duration.as_secs_f64() / GB as f64
    );

    Ok(())
}

async fn async_run_client<C: AsyncClient>(ctx: Context, cfg: ClientConfig) -> io::Result<()> {
    let mut client = C::new(ctx, cfg).await?;
    info!("Client started. Sending requests...");

    let start = time::Instant::now();

    let mut handles = Vec::new();
    let mut result = vec![0u8; REQUEST_SIZE * REQUEST_COUNT];
    for chunk in result.chunks_mut(REQUEST_SIZE) {
        info!("Sending");
        handles.push(client.request(chunk).await?);
    }
    for handle in handles {
        handle.wait();
        info!("Received");
    }

    let duration = start.elapsed();

    validate(result);
    info!(
        "{} GB/s",
        (REQUEST_COUNT * REQUEST_SIZE) as f64 / duration.as_secs_f64() / GB as f64
    );

    Ok(())
}

fn validate(data: Vec<u8>) {
    thread::scope(|s| {
        for chunk in data.chunks_exact(REQUEST_SIZE) {
            s.spawn(move || {
                for i in 0..chunk.len() {
                    assert_eq!(chunk[i], i as u8);
                }
            });
        }
    });
}
