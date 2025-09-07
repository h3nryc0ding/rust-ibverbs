use application::server::Server;
use std::io;
use tracing::{info, Level};

fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .compact()
        .init();

    let mut server = Server::new("0.0.0.0:18515")?;
    info!("Server started. Serving requests...");

    server.serve()
}
