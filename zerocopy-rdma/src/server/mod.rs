mod server;

use std::{io, net};

pub fn run(ctx: ibverbs::Context, socket: net::SocketAddr) -> io::Result<()> {
    let server = server::Initialized::new(ctx)?;
    println!("Server successfully initialized, listening on {}", socket);

    let mut server = server.accept(socket)?;
    println!("Server accepted connection from {}", socket);

    println!("Server is now serving requests...");
    server.serve()
}
