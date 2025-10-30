use application::MI_B;
use application::bench::{BaseCLI, non_blocking};
use application::client::{NonBlockingClient, pipeline};
use clap::Parser;
use pipeline::r#async::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,

    #[arg(long, default_value_t = 2 * MI_B)]
    chunk_size: usize,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = CLI::parse();

    non_blocking(&args.base, |base, size| {
        Client::new(
            base,
            Config {
                chunk_size: args.chunk_size.min(size),
            },
        )
    })
}
