use application::args::{DefaultCLI, bench_non_blocking};
use application::client::naive::threaded::{Client, Config};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: DefaultCLI,

    #[arg(long, default_value_t = 8)]
    concurrency_reg: usize,

    #[arg(long, default_value_t = 2)]
    concurrency_dereg: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    let config = Config::from(&args);
    bench_non_blocking::<Client>(&args.default, config)
}

impl From<&CLI> for Config {
    fn from(value: &CLI) -> Self {
        Self {
            concurrency_reg: value.concurrency_reg,
            concurrency_dereg: value.concurrency_dereg,
        }
    }
}
