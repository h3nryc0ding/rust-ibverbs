use application::memory::Provider;
use application::memory::ideal::IdealProvider;
use application::memory::jit::JitProvider;
use application::memory::pooled::PooledProvider;
use application::transfer::{Client, Protocol, SendRecvProtocol, SendRecvReadProtocol, Server};
use application::{REQUEST_COUNT, REQUEST_SIZE_SEED};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::{Arc, Mutex};
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
    Ideal,
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
        MemoryProvider::Ideal => execute::<P, IdealProvider>(args),
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
    let mut client: <P>::Client<M> = Client::new(addr)?;
    info!("Client started. Sending requests...");

    let start = time::Instant::now();
    let mut received = 0;
    let rng = Arc::new(Mutex::new(ChaCha8Rng::seed_from_u64(REQUEST_SIZE_SEED)));
    for i in 0..REQUEST_COUNT {
        let span = info_span!("request", request_id = i);
        let _enter = span.enter();

        let req = i as u8;
        info!("Sending request: {}", req);
        let res = client.request(req)?;
        info!("Received response: {:?}", res);
        received += res.len();

        let rng = Arc::clone(&rng);
        thread::spawn(move || {
            let mut rng = rng.lock().unwrap();

            let expected_size = rng.random_range(0..1 * 2usize.pow(30));
            let expected: Vec<u8> = (0..expected_size).map(|i| (i % 256) as u8).collect();
            let expected_xor = expected.iter().fold(0u8, |acc, &x| acc ^ x);

            assert_eq!(expected_size, res.len());
            assert_eq!(expected_xor, res.iter().fold(0u8, |acc, &x| acc ^ x));
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
