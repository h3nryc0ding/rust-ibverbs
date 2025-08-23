use crate::transfer::{Protocol, Server};
use tokio::io;
use tokio::net::TcpListener;

pub async fn run<P: Protocol>(dev: ibverbs::Device<'_>, listener: TcpListener) -> io::Result<()> {
    println!("Server is listening for connections...");
    loop {
        let (mut stream, addr) = listener.accept().await?;
        println!("Accepted new connection from {}", addr);

        let ctx = dev.open()?;
        let mut server = P::Server::new(ctx, &mut stream).await?;
        println!("Server session for {} is now running.", addr);

        server.serve().await?;
    }
}
