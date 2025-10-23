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
    threads_min: usize,

    #[arg(long, default_value_t = 9)]
    threads_max: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    for threads in args.threads_min..args.threads_max {
        {
            non_blocking(&args.base, |base, _size| {
                Client::new(
                    base,
                    Config {
                        concurrency_reg: threads,
                        concurrency_dereg: 1,
                    },
                )
            })?;
        }
    }

    Ok(())
}
