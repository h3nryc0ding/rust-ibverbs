use application::bench::{BaseCLI, non_blocking};
use application::client::{NonBlockingClient, naive};
use clap::Parser;
use naive::threaded::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,

    #[arg(long, default_value_t = 1)]
    concurrency_reg: usize,

    #[arg(long, default_value_t = 1)]
    concurrency_dereg: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    non_blocking(&args.base, |base, _| {
        Client::new(
            base,
            Config {
                concurrency_reg: args.concurrency_reg,
                concurrency_dereg: args.concurrency_dereg,
            },
        )
    })
}
