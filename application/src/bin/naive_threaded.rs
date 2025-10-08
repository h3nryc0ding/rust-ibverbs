use application::args::bench_threaded;
use application::client::BaseClient;
use application::client::naive::threaded::{Client, Config};
use application::{MI_B, args};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: args::Args,

    #[arg(long, default_value_t = 8)]
    concurrency_reg: usize,

    #[arg(long, default_value_t = 2)]
    concurrency_dereg: usize,
}

fn main() -> io::Result<()> {
    let args = CLI::parse();

    let client = BaseClient::new(args.default.addr)?;
    let remotes = client.remotes();

    let config = Config::from(&args);
    let mut client = Client::new(client, config)?;
    let remote = remotes[0].slice(0..512 * MI_B);

    bench_threaded(&mut client, &remote, &args.default)
}

impl From<&CLI> for Config {
    fn from(value: &CLI) -> Self {
        Self {
            concurrency_reg: value.concurrency_reg,
            concurrency_dereg: value.concurrency_dereg,
        }
    }
}
