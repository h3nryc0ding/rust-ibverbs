use application::memory::Provider;
use application::memory::jit::JitProvider;
use application::memory::pooled::PooledProvider;
use application::transfer::{Client, Protocol, SendRecvProtocol, SendRecvReadProtocol, Server};
use std::net::{IpAddr, ToSocketAddrs};
use std::{io, thread, time};
use tracing::{Level, info, info_span};

#[derive(clap::Parser, Debug)]
#[command(version, about = "Start an RDMA server or connect as a client.")]
struct Args {
    server: Option<IpAddr>,
    #[arg(short, long, default_value_t = 18515)]
    port: u16,
    #[arg(short, value_enum, default_value_t = TransferProtocol::SendRecvRead)]
    transfer_protocol: TransferProtocol,
    #[arg(short, value_enum, default_value_t = MemoryProvider::Pooled)]
    memory_provider: MemoryProvider,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum TransferProtocol {
    SendRecv,
    SendRecvRead,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum MemoryProvider {
    Jit,
    Pooled,
}

fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .compact()
        .init();

    let args = clap::Parser::parse();
    dispatch(args)
}

fn dispatch(args: Args) -> io::Result<()> {
    match args.transfer_protocol {
        TransferProtocol::SendRecv => dispatch_provider::<SendRecvProtocol>(args),
        TransferProtocol::SendRecvRead => dispatch_provider::<SendRecvReadProtocol>(args),
    }
}

fn dispatch_provider<P: Protocol>(args: Args) -> io::Result<()> {
    match args.memory_provider {
        MemoryProvider::Jit => execute::<P, JitProvider>(args),
        MemoryProvider::Pooled => execute::<P, PooledProvider>(args),
    }
}

fn execute<P: Protocol, M: Provider>(args: Args) -> io::Result<()> {
    match args.server {
        None => run_server::<P, M>(format!("0.0.0.0:{}", args.port)),
        Some(addr) => run_client::<P, M>(format!("{}:{}", addr, args.port)),
    }
}

fn run_server<P: Protocol, M: Provider>(addr: impl ToSocketAddrs) -> io::Result<()> {
    let mut server: <P>::Server<M> = Server::new(addr)?;
    info!("Server started. Serving requests...");

    server.serve()
}

fn run_client<P: Protocol, M: Provider>(addr: impl ToSocketAddrs) -> io::Result<()> {
    const REQUESTS: usize = 64;

    let mut client: <P>::Client<M> = Client::new(addr)?;
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
        received += res.len();

        thread::spawn(move || {
            let xor = res.iter().fold(0, |acc, &b| acc ^ b);
            info!("Checksum: {}", xor);
        });
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
