use application::bench::{BaseCLI, non_blocking};
use application::client::{NonBlockingClient, naive};
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

    non_blocking(&args.base, |base, _| Client::new(base, Config))
}
