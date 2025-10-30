use application::MI_B;
use application::bench::{BaseCLI, non_blocking};
use application::client::{NonBlockingClient, pipeline};
use clap::Parser;
use pipeline::threaded::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,

    #[arg(long, default_value_t = 2 * MI_B)]
    chunk_size: usize,

    #[arg(long, default_value_t = 1)]
    concurrency_reg: usize,

    #[arg(long, default_value_t = 1)]
    concurrency_dereg: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    non_blocking(&args.base, |base, size| {
        Client::new(
            base,
            Config {
                chunk_size: args.chunk_size.min(size),
                concurrency_reg: args.concurrency_reg,
                concurrency_dereg: args.concurrency_dereg,
            },
        )
    })
}
