use application::args::{latency_blocking, throughput_blocking, validate_blocking};
use application::client::BaseClient;
use application::client::naive::blocking::Client;
use application::{MI_B, args};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: args::Args,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    let client = BaseClient::new(args.default.addr)?;
    let remotes = client.remotes();

    let mut client = Client::new(client)?;
    let remote = remotes[0].slice(0..512 * MI_B);

    if args.default.latency {
        latency_blocking(&mut client, remote)?;
    }

    if args.default.throughput {
        throughput_blocking(&mut client, remote)?;
    }

    if args.default.validate {
        validate_blocking(&mut client, remote)?;
    }

    Ok(())
}
