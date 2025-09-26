use application::client::{
    Client, CopyClient, CopyThreadedClient, IdealClient, IdealThreadedClient, NaiveClient,
    PipelineClient, PipelineThreadedClient,
};
use application::server::Server;
use application::{GB, OPTIMAL_MR_SIZE};
use ibverbs::Context;
use std::net::{IpAddr, ToSocketAddrs};
use std::{io, time};
use tracing::{Level, info, info_span};

const MIN_REQUEST_SIZE: usize = OPTIMAL_MR_SIZE;
const MAX_REQUEST_SIZE: usize = 1 * GB;

#[derive(clap::Parser, Debug)]
#[command(version, about = "Start an RDMA server or connect as a client.")]
struct Args {
    server: Option<IpAddr>,
    #[arg(long, value_enum, required_if_eq("server", "Some"))]
    mode: Option<Mode>,

    #[arg(short, long, default_value_t = 18515)]
    port: u16,

    #[arg(long, default_value_t = Level::TRACE)]
    log: Level,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Mode {
    Ideal,
    IdealThreaded,
    Copy,
    CopyThreaded,
    Naive,
    Pipeline,
    PipelineThreaded,
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

    match args.server {
        None => run_server(ctx, format!("0.0.0.0:{}", args.port)),
        Some(addr) => match args.mode.unwrap() {
            Mode::Naive => run_client::<NaiveClient>(ctx, format!("{}:{}", addr, args.port)),
            Mode::Copy => run_client::<CopyClient>(ctx, format!("{}:{}", addr, args.port)),
            Mode::CopyThreaded => {
                run_client::<CopyThreadedClient>(ctx, format!("{}:{}", addr, args.port))
            }
            Mode::Ideal => run_client::<IdealClient>(ctx, format!("{}:{}", addr, args.port)),
            Mode::IdealThreaded => {
                run_client::<IdealThreadedClient>(ctx, format!("{}:{}", addr, args.port))
            }
            Mode::Pipeline => run_client::<PipelineClient>(ctx, format!("{}:{}", addr, args.port)),
            Mode::PipelineThreaded => {
                run_client::<PipelineThreadedClient>(ctx, format!("{}:{}", addr, args.port))
            }
        },
    }
}

fn run_server(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<()> {
    let mut server = Server::new(ctx, addr)?;
    info!("Server started. Serving requests...");

    server.serve()
}

fn run_client<C: Client>(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<()> {
    let mut client = C::new(ctx, addr)?;
    info!("Client started. Sending requests...");

    let start = time::Instant::now();
    let mut received = 0;
    let mut result = vec![0u8; MAX_REQUEST_SIZE];
    for size in (MIN_REQUEST_SIZE..=MAX_REQUEST_SIZE).step_by(OPTIMAL_MR_SIZE) {
        let span = info_span!("request", request_id = size);
        let _enter = span.enter();

        info!("Sending");
        let _ = client.request(&mut result[0..size])?;
        info!("Received");
        received += size;
    }
    let duration = start.elapsed();
    let received_gb = received as f64 / 2f64.powi(30);

    println!(
        "Received {} GiB in {:?} ({} GiB/s)",
        received_gb,
        duration,
        received_gb / duration.as_secs_f64()
    );

    Ok(())
}
