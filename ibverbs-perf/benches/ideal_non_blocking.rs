use application::KI_B;
use application::bench::{BaseCLI, non_blocking};
use application::client::{NonBlockingClient, ideal};
use clap::Parser;
use ideal::threaded::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,

    #[arg(long, default_value_t = 512 * KI_B)]
    chunk_size: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    non_blocking(&args.base, |base, size| {
        Client::new(
            base,
            Config {
                mr_size: args.chunk_size,
                mr_count: args.base.inflight(size).max(4 * KI_B),
            },
        )
    })
}
