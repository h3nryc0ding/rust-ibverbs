use application::transfer::{Client, Protocol, SendRecvProtocol, SendRecvReadProtocol, Server};
use std::net::{IpAddr, ToSocketAddrs};
use std::{io, time};
use tracing::{Level, info, info_span};

#[derive(clap::Parser, Debug)]
#[command(version, about = "Start an RDMA server or connect as a client.")]
struct Args {
    server: Option<IpAddr>,
    #[arg(short, long, default_value_t = 18515)]
    port: u16,
    #[arg(short, value_enum, default_value_t = TransferProtocol::SendRecv)]
    protocol: TransferProtocol,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum TransferProtocol {
    SendRecv,
    SendRecvRead,
}

fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .compact()
        .init();

    let args: Args = clap::Parser::parse();

    match args.protocol {
        TransferProtocol::SendRecv => run::<SendRecvProtocol>(args.server, args.port),
        TransferProtocol::SendRecvRead => run::<SendRecvReadProtocol>(args.server, args.port),
    }
}

fn run<P: Protocol>(addr: Option<IpAddr>, port: u16) -> io::Result<()> {
    match addr {
        None => run_server::<P>(format!("0.0.0.0:{}", port)),
        Some(addr) => run_client::<P>(format!("{}:{}", addr, port)),
    }
}

fn run_server<P: Protocol>(addr: impl ToSocketAddrs) -> io::Result<()> {
    let mut server = P::Server::new(addr)?;
    info!("Server started. Serving requests...");

    server.serve()
}

fn run_client<P: Protocol>(addr: impl ToSocketAddrs) -> io::Result<()> {
    const REQUESTS: usize = 128;

    let mut client = P::Client::new(addr)?;
    info!("Client started. Sending requests...");

    let start = time::Instant::now();
    let mut received = 0;
    for i in 0..REQUESTS {
        let span = info_span!("request", request_id = i);
        let _enter = span.enter();

        let req = i as u8;
        info!("Sending request: {}", req);
        let res = client.request(req)?;
        info!("Received response: {:?}", res);

        received += res.len()
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
