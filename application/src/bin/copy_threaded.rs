use application::MI_B;
use application::args::{DefaultCLI, bench_non_blocking};
use application::client::copy::threaded::{Client, Config};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: DefaultCLI,

    #[arg(long, default_value_t = 4 * MI_B)]
    mr_size: usize,

    #[arg(long, default_value_t = 32)]
    mr_count: usize,

    #[arg(long, default_value_t = 4)]
    concurrency: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    let config = Config::from(&args);
    bench_non_blocking::<Client>(&args.default, config)
}

impl From<&CLI> for Config {
    fn from(value: &CLI) -> Self {
        Self {
            mr_size: value.mr_size,
            mr_count: value.mr_count,
            concurrency: value.concurrency,
        }
    }
}
