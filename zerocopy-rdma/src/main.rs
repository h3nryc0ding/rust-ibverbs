use std::net::{IpAddr, Ipv6Addr, SocketAddr};
use tokio::net::TcpStream;
use tokio::{io, net};
use zerocopy::transfer::{ReadWriteProtocol, SendRecvProtocol};
use zerocopy::{client, server, transfer};

#[derive(clap::Parser, Debug)]
#[command(version, about = "Start an RDMA server or connect as a client.")]
struct Args {
    /// The IP address to connect to (client mode).
    /// If not provided, runs in server mode.
    server: Option<IpAddr>,

    /// The TCP port for the initial handshake.
    #[arg(short, long, default_value_t = 18515)]
    port: u16,

    /// The RDMA protocol to use.
    #[arg(long, value_enum, default_value_t = Protocol::SendRecv)]
    protocol: Protocol,
}

#[derive(clap::ValueEnum, Clone, Debug)]
enum Protocol {
    SendRecv,
    ReadWrite,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args: Args = clap::Parser::parse();

    match args.protocol {
        Protocol::SendRecv => run::<SendRecvProtocol>(args).await,
        Protocol::ReadWrite => run::<ReadWriteProtocol>(args).await,
    }
}

async fn run<P: transfer::Protocol>(args: Args) -> io::Result<()> {
    let devices = ibverbs::devices()?;
    let device = devices.get(0).expect("No RDMA devices found");

    if let Some(server) = args.server {
        let address = SocketAddr::new(server, args.port);
        let stream = TcpStream::connect(address).await?;
        client::run::<P>(device, stream).await
    } else {
        let address = SocketAddr::new(IpAddr::from(Ipv6Addr::UNSPECIFIED), args.port);
        let listener = net::TcpListener::bind(address).await?;
        server::run::<P>(device, listener).await
    }
}
