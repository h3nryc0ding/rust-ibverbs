use application::server::Server;
use std::io;

fn main() -> io::Result<()> {
    let mut server = Server::new("0.0.0.0:18515")?;
    println!("Server started. Serving requests...");

    server.serve()
}
