use application::MI_B;
use application::bench::{DefaultCLI, bench_async};
use application::client::pipeline::r#async::{Client, Config};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: DefaultCLI,

    #[arg(long, default_value_t = 4 * MI_B)]
    chunk_size: usize,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = CLI::parse();

    let config = Config::from(&args);
    bench_async::<Client>(&args.default, config).await
}

impl From<&CLI> for Config {
    fn from(value: &CLI) -> Self {
        Self {
            chunk_size: value.chunk_size,
        }
    }
}
