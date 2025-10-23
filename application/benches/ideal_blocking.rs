use application::bench::{BaseCLI, blocking};
use application::client::BlockingClient;
use application::client::ideal::blocking::{Client, Config};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    base: BaseCLI,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    blocking(&args.base, |base, size| {
        Client::new(
            base,
            Config {
                mr_size: size,
                mr_count: 1,
            },
        )
    })
}
