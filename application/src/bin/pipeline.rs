use application::args::{latency_blocking, throughput_blocking, validate_blocking};
use application::client::BaseClient;
use application::client::pipeline::blocking::{Client, Config};
use application::{KI_B, MI_B, args};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: args::Args,

    #[arg(long, default_value_t = 4 * KI_B)]
    chunk_size: usize,
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

    if args.default.validate {
        validate_blocking(&mut client, remote)?;
    }

    Ok(())
}

impl From<&CLI> for Config {
    fn from(value: &CLI) -> Self {
        Self {
            chunk_size: value.chunk_size,
        }
    }
}
