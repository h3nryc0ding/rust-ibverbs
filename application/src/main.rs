use application::MB;
use application::client::{BaseClient, Client, CopyClient, SimpleClient};
use application::server::Server;
use ibverbs::Context;
use std::net::{IpAddr, ToSocketAddrs};
use std::{io, time};
use tracing::{Level, info, info_span};

const REQUEST_COUNT: usize = 256;

#[derive(clap::Parser, Debug)]
#[command(version, about = "Start an RDMA server or connect as a client.")]
struct Args {
    server: Option<IpAddr>,
    #[arg(short, long, default_value_t = 18515)]
    port: u16,

    #[arg(long, default_value_t = Level::TRACE)]
    log: Level,

    #[arg(long, value_enum, default_value_t = Mode::Simple)]
    mode: Mode,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Mode {
    Simple,
    Copy,
}

fn main() -> io::Result<()> {
    let args: Args = clap::Parser::parse();

    tracing_subscriber::fmt()
        .with_max_level(args.log)
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
        Some(addr) => match args.mode {
            Mode::Simple => run_client::<SimpleClient>(ctx, format!("{}:{}", addr, args.port)),
            Mode::Copy => run_client::<CopyClient>(ctx, format!("{}:{}", addr, args.port)),
        },
    }
}

fn run_server(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<()> {
    let mut server = Server::new(ctx, addr)?;
    info!("Server started. Serving requests...");

    server.serve()
}

fn run_client<C: Client>(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<()> {
    let base = BaseClient::new(ctx, addr)?;
    let mut client = C::new(base)?;
    info!("Client started. Sending requests...");

    let start = time::Instant::now();
    let mut received = 0;
    for size in 1..REQUEST_COUNT {
        let span = info_span!("request", request_id = size);
        let _enter = span.enter();

        info!("Sending");
        let res = client.request(size * MB)?;
        info!("Received");
        received += res.len();
    }
    let duration = start.elapsed();
    let received_gb = received as f64 / 2f64.powi(30);

    info!(
        "Received {} GiB in {:?} ({} GiB/s)",
        received_gb,
        duration,
        received_gb / duration.as_secs_f64()
    );

    Ok(())
}
