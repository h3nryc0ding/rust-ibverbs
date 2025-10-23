use application::bench::{BaseCLI, r#async};
use application::client::{AsyncClient, BlockingClient, naive};
use application::{KI_B, MI_B, sequence_multiplied};
use clap::Parser;
use naive::r#async::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = CLI::parse();

    r#async(&args.base, |base, size| Client::new(base, Config {})).await
}
