use application::bench::{BaseCLI, non_blocking};
use application::client::{NonBlockingClient, pipeline};
use application::{KI_B, MI_B, sequence_multiplied};
use clap::Parser;
use pipeline::threaded::{Client, Config};
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

    #[arg(long, default_value_t = 1)]
    threads_min: usize,

    #[arg(long, default_value_t = 9)]
    threads_max: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    for threads in args.threads_min..args.threads_max {
        for chunk_size in sequence_multiplied(
            args.chunk_size_min,
            args.chunk_size_max,
            args.chunk_size_multiplier,
        ) {
            non_blocking(&args.base, |base, _size| {
                Client::new(
                    base,
                    Config {
                        chunk_size,
                        concurrency_reg: threads,
                        concurrency_dereg: 1,
                    },
                )
            })?;
        }
    }

    Ok(())
}
