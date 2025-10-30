use application::MI_B;
use application::bench::{BaseCLI, blocking};
use application::client::{BlockingClient, pipeline};
use clap::Parser;
use pipeline::blocking::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,

    #[arg(long, default_value_t = 2 * MI_B)]
    chunk_size: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    blocking(&args.base, |base, size| {
        Client::new(
            base,
            Config {
                chunk_size: args.chunk_size.min(size),
            },
        )
    })
}
