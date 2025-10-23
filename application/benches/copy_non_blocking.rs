use application::bench::{BaseCLI, non_blocking};
use application::client::{NonBlockingClient, copy};
use application::{KI_B, MI_B, sequence_multiplied};
use clap::Parser;
use copy::threaded::{Client, Config};
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

    #[arg(long, default_value_t = 4)]
    mr_count_min: usize,

    #[arg(long, default_value_t = 512)]
    mr_count_max: usize,

    #[arg(long, default_value_t = 2)]
    mr_count_multiplier: usize,

    #[arg(long, default_value_t = 1)]
    threads_min: usize,

    #[arg(long, default_value_t = 9)]
    threads_max: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    for threads in args.threads_min..args.threads_max {
        for mr_size in
            sequence_multiplied(args.mr_size_min, args.mr_size_max, args.mr_size_multiplier)
        {
            for mr_count in sequence_multiplied(
                args.mr_count_min,
                args.mr_count_max,
                args.mr_count_multiplier,
            ) {
                non_blocking(&args.base, |base, size| {
                    Client::new(
                        base,
                        Config {
                            mr_size,
                            mr_count,
                            concurrency: threads,
                        },
                    )
                })?
            }
        }
    }

    Ok(())
}
