use application::args::{DefaultCLI, bench_blocking};
use application::client::naive;
use clap::Parser;
use naive::blocking::{Client, Config};
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: DefaultCLI,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    let config = Config::from(&args);
    bench_blocking::<Client>(&args.default, config)
}

impl From<&CLI> for Config {
    fn from(_: &CLI) -> Self {
        Self {}
    }
}
