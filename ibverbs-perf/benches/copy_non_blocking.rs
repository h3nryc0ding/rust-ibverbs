use application::KI_B;
use application::bench::{BaseCLI, non_blocking};
use application::client::{NonBlockingClient, copy};
use clap::Parser;
use copy::threaded::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,

    #[arg(long, default_value_t = 256 * KI_B)]
    mr_size: usize,

    #[arg(long, default_value_t = 1)]
    concurrency: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    non_blocking(&args.base, |base, size| {
        Client::new(
            base,
            Config {
                mr_size: args.mr_size,
                mr_count: args.base.inflight(size).max(4 * KI_B),
                concurrency: args.concurrency,
            },
        )
    })
}
