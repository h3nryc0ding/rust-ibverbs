use application::KI_B;
use application::args::{DefaultCLI, bench_blocking};
use application::client::pipeline::blocking::{Client, Config};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: DefaultCLI,

    #[arg(long, default_value_t = 4 * KI_B)]
    chunk_size: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    let config = Config::from(&args);
    bench_blocking::<Client>(&args.default, config)
}

impl From<&CLI> for Config {
    fn from(value: &CLI) -> Self {
        Self {
            chunk_size: value.chunk_size,
        }
    }
}
