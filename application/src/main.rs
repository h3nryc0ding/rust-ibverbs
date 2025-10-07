use application::client::{
    AsyncClient, BlockingClient, CopyAsyncClient, CopyClient, CopyThreadedClient, IdealClient,
    NaiveAsyncClient, NaiveClient, NaiveThreadedClient, NonBlockingClient, PipelineAsyncClient,
    PipelineClient, PipelineThreadedClient,
};
use application::server::Server;
use application::{GB, KI_B, MI_B, chunks_mut_exact, client};
use bytes::{Bytes, BytesMut};
use client::ClientConfig;
use ibverbs::Context;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::{io, thread, time};
use tokio::runtime;
use tracing::{Level, debug, info};

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
    CopyAsync,
    CopyThreaded,
    Naive,
    NaiveAsync,
    NaiveThreaded,
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
        .into_iter()
        .next()
        .ok_or_else(|| io::Error::new(io::ErrorKind::NotFound, "No RDMA devices found"))?
        .open()?;

    if let Some(server_ip) = args.server {
        run_client(ctx, server_ip, args)
    } else {
        let listen_addr = format!("0.0.0.0:{}", args.port);
        run_server(ctx, listen_addr)
    }
}

fn run_client(ctx: Context, server_ip: IpAddr, args: Args) -> io::Result<()> {
    let cfg = ClientConfig {
        server_addr: SocketAddr::new(server_ip, args.port),
        mr_size: args.size_kb * KI_B,
        qp_count: args.qp_count,
    };

    match args.mode.unwrap() {
        Mode::Ideal => run_client_blocking::<IdealClient>(ctx, cfg),
        Mode::Naive => run_client_blocking::<NaiveClient>(ctx, cfg),
        Mode::NaiveAsync => {
            runtime::Runtime::new()?.block_on(async_run_client::<NaiveAsyncClient>(ctx, cfg))
        }
        Mode::NaiveThreaded => run_client_non_blocking::<NaiveThreadedClient>(ctx, cfg),
        Mode::Copy => run_client_blocking::<CopyClient>(ctx, cfg),
        Mode::CopyAsync => {
            runtime::Runtime::new()?.block_on(async_run_client::<CopyAsyncClient>(ctx, cfg))
        }
        Mode::CopyThreaded => run_client_non_blocking::<CopyThreadedClient>(ctx, cfg),
        Mode::Pipeline => run_client_blocking::<PipelineClient>(ctx, cfg),
        Mode::PipelineAsync => {
            runtime::Runtime::new()?.block_on(async_run_client::<PipelineAsyncClient>(ctx, cfg))
        }
        Mode::PipelineThreaded => run_client_non_blocking::<PipelineThreadedClient>(ctx, cfg),
    }
}

fn run_server(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<()> {
    let mut server = Server::new(ctx, addr)?;
    info!("Server started. Serving requests...");
    server.serve()
}

fn run_test<F>(test_body: F) -> io::Result<()>
where
    F: FnOnce(BytesMut) -> io::Result<Vec<Bytes>>,
{
    info!("Client started. Sending requests...");
    let bytes = BytesMut::zeroed(REQUEST_SIZE * REQUEST_COUNT);

    let start = time::Instant::now();
    let results = test_body(bytes)?;
    let duration = start.elapsed();

    validate(results);

    info!(
        "Throughput: {:.2} GB/s",
        (REQUEST_COUNT * REQUEST_SIZE) as f64 / duration.as_secs_f64() / GB as f64
    );
    Ok(())
}

async fn run_test_async<F, Fut>(test_body: F) -> io::Result<()>
where
    F: FnOnce(BytesMut) -> Fut,
    Fut: Future<Output = io::Result<Vec<Bytes>>>,
{
    info!("Client started. Sending requests...");
    let bytes = BytesMut::zeroed(REQUEST_SIZE * REQUEST_COUNT);
    let start = time::Instant::now();

    let results = test_body(bytes).await?;
    let duration = start.elapsed();

    validate(results);

    info!(
        "Throughput: {:.2} GB/s",
        (REQUEST_COUNT * REQUEST_SIZE) as f64 / duration.as_secs_f64() / GB as f64
    );
    Ok(())
}

fn run_client_blocking<C: BlockingClient>(ctx: Context, cfg: ClientConfig) -> io::Result<()> {
    let mut client = C::new(ctx, cfg)?;
    run_test(|bytes| {
        let results: Vec<BytesMut> = chunks_mut_exact(bytes, REQUEST_SIZE)
            .map(|chunk| {
                debug!("Sending request");
                let res = client.request(chunk);
                debug!("Received response");
                res
            })
            .collect::<io::Result<_>>()?;
        Ok(results.into_iter().map(|b| b.freeze()).collect())
    })
}

fn run_client_non_blocking<C: NonBlockingClient>(
    ctx: Context,
    cfg: ClientConfig,
) -> io::Result<()> {
    let mut client = C::new(ctx, cfg)?;
    run_test(|bytes| {
        let handles: Vec<_> = chunks_mut_exact(bytes, REQUEST_SIZE)
            .map(|chunk| {
                debug!("Sending request");
                let handle = client.request(chunk);
                debug!("Sent request");
                handle
            })
            .collect::<io::Result<_>>()?;
        info!("All {} requests sent.", handles.len());

        let results: Vec<BytesMut> = handles
            .into_iter()
            .map(|handle| {
                debug!("Waiting for response");
                let res = handle.wait();
                debug!("Received response");
                res
            })
            .collect::<io::Result<_>>()?;
        info!("All {} responses received.", results.len());
        Ok(results.into_iter().map(|b| b.freeze()).collect())
    })
}

async fn async_run_client<C: AsyncClient>(ctx: Context, cfg: ClientConfig) -> io::Result<()> {
    let mut client = C::new(ctx, cfg).await?;
    run_test_async(|bytes| async {
        let mut handles = Vec::with_capacity(REQUEST_COUNT);
        for chunk in chunks_mut_exact(bytes, REQUEST_SIZE) {
            debug!("Sending request");
            handles.push(client.request(chunk).await?);
            debug!("Sent request");
        }
        info!("All {} requests sent.", handles.len());

        let results: Vec<BytesMut> = handles
            .into_iter()
            .map(|handle| {
                debug!("Waiting for response");
                let res = handle.wait();
                debug!("Received response");
                res
            })
            .collect::<io::Result<_>>()?;
        info!("All {} responses received.", results.len());
        Ok(results.into_iter().map(|b| b.freeze()).collect())
    })
    .await
}

fn validate(result: Vec<Bytes>) {
    thread::scope(|s| {
        for request in result {
            s.spawn(move || {
                request
                    .iter()
                    .enumerate()
                    .for_each(|(index, chunk)| assert_eq!(index as u8, *chunk))
            });
        }
    });
}
