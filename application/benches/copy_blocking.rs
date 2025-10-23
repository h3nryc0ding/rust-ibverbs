use application::bench::{BaseCLI, blocking};
use application::client::{BlockingClient, copy};
use application::{KI_B, MI_B, sequence_multiplied};
use clap::Parser;
use copy::blocking::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,

    #[arg(long, default_value_t = 4 * KI_B)]
    mr_size_min: usize,

    #[arg(long, default_value_t = 64 * MI_B)]
    mr_size_max: usize,

    #[arg(long, default_value_t = 2)]
    mr_size_multiplier: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    for mr_size in sequence_multiplied(args.mr_size_min, args.mr_size_max, args.mr_size_multiplier)
    {
        blocking(&args.base, |base, size| {
            Client::new(
                base,
                Config {
                    mr_size,
                    mr_count: size.div_ceil(mr_size),
                },
            )
        })?
    }

    Ok(())
}
