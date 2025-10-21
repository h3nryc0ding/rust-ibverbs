use application::GI_B;
use application::server::{Config, Server};
use clap::Parser;
use std::io;
use application::bench::init_tracing;

#[derive(Debug, Parser)]
#[command(author, version, about)]
pub struct CLI {
    #[arg(long, default_value_t = 4 * GI_B)]
    size: usize,
}

fn main() -> io::Result<()> {
    init_tracing();
    let args = CLI::parse();

    let config = Config::from(args);
    let mut server = Server::new(config)?;

    server.serve()
}

impl From<CLI> for Config {
    fn from(value: CLI) -> Self {
        Self { size: value.size }
    }
}
