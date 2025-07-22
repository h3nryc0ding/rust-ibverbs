use std::{io, net};
use zerocopy::{client, server};

#[derive(clap::Parser, Debug)]
#[command(version, about = "Start an RDMA server or connect as a client.")]
struct Args {
    /// The IP address to connect to (client mode).
    /// If not provided, runs in server mode.
    server: Option<net::IpAddr>,
    /// The TCP port for the initial handshake.
    #[arg(short, long, default_value_t = 18515)]
    port: u16,
}

fn main() -> io::Result<()> {
    let args: Args = clap::Parser::parse();

    let ctx = ibverbs::devices()?
        .iter()
        .next()
        .expect("no rdma device available")
        .open()?;

    if let Some(server) = args.server {
        let address = net::SocketAddr::new(server, args.port);
        client::run(ctx, address)
    } else {
        let address =
            net::SocketAddr::new(net::IpAddr::from(net::Ipv6Addr::UNSPECIFIED), args.port);
        server::run(ctx, address)
    }
}
