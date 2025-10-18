use application::bench::{DefaultCLI, bench_async};
use application::client::naive::r#async::{Client, Config};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: DefaultCLI,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = CLI::parse();

    let config = Config::from(&args);
    bench_async::<Client>(&args.default, config).await
}

impl From<&CLI> for Config {
    fn from(_: &CLI) -> Self {
        Self {}
    }
}
