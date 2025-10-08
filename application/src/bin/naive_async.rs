use application::args::bench_async;
use application::client::BaseClient;
use application::client::naive::r#async::Client;
use application::{MI_B, args};
use clap::Parser;
use std::io;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[command(flatten)]
    default: args::Args,

    #[arg(long, default_value_t = 4 * MI_B)]
    mr_size: usize,

    #[arg(long, default_value_t = 32)]
    mr_count: usize,
}

#[tokio::main]
async fn main() -> io::Result<()> {
    let args = CLI::parse();

    let client = BaseClient::new(args.default.addr)?;
    let remotes = client.remotes();

    let mut client = Client::new(client).await?;
    let remote = remotes[0].slice(0..512 * MI_B);

    bench_async(&mut client, &remote, &args.default).await
}
