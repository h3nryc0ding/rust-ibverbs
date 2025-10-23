use application::bench::{BaseCLI, blocking};
use application::client::{BlockingClient, pipeline};
use application::{KI_B, MI_B, sequence_multiplied};
use clap::Parser;
use pipeline::blocking::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,

    #[arg(long, default_value_t = 4 * KI_B)]
    chunk_size_min: usize,

    #[arg(long, default_value_t = 64 * MI_B)]
    chunk_size_max: usize,

    #[arg(long, default_value_t = 2)]
    chunk_size_multiplier: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    for chunk_size in sequence_multiplied(
        args.chunk_size_min,
        args.chunk_size_max,
        args.chunk_size_multiplier,
    ) {
        blocking(&args.base, |base, _size| {
            Client::new(base, Config { chunk_size })
        })?;
    }

    Ok(())
}
