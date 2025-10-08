use application::args::{latency_blocking, throughput_blocking};
use application::client::BaseClient;
use application::client::ideal::{Client, Config};
use application::{MI_B, args};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: args::Args,

    #[arg(long, default_value_t = 4 * MI_B)]
    mr_size: usize,

    #[arg(long, default_value_t = 32)]
    mr_count: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    let client = BaseClient::new(args.default.addr)?;
    let remotes = client.remotes();

    let config = Config::from(&args);
    let mut client = Client::new(client, config)?;
    let remote = remotes[0].slice(0..512 * MI_B);

    if args.default.latency {
        latency_blocking(&mut client, remote)?;
    }

    if args.default.throughput {
        throughput_blocking(&mut client, remote)?;
    }

    Ok(())
}

impl From<&CLI> for Config {
    fn from(value: &CLI) -> Self {
        Self {
            mr_count: value.mr_count,
            mr_size: value.mr_size,
        }
    }
}
