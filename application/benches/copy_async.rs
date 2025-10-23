use application::bench::{BaseCLI, r#async};
use application::client::{AsyncClient, BlockingClient, copy};
use application::{KI_B, MI_B, sequence_multiplied};
use clap::Parser;
use copy::r#async::{Client, Config};
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
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = CLI::parse();

    for mr_size in sequence_multiplied(args.mr_size_min, args.mr_size_max, args.mr_size_multiplier)
    {
        for mr_count in sequence_multiplied(
            args.mr_count_min,
            args.mr_count_max,
            args.mr_count_multiplier,
        ) {
            r#async(&args.base, |base, size| {
                Client::new(base, Config { mr_size, mr_count })
            })
            .await?;
        }
    }

    Ok(())
}
