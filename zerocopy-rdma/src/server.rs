use crate::transfer::{Protocol, Server};
use tokio::net::TcpListener;
use tokio::{io, task};

pub async fn run<'dev, P: Protocol>(
    dev: ibverbs::Device<'dev>,
    listener: TcpListener,
) -> io::Result<()>
where
    P::Server: Sync + Send,
{
    println!("Server is listening for connections...");
    loop {
        let (stream, addr) = listener.accept().await?;
        println!("Accepted new connection from {}", addr);

        let ctx = dev.open()?;
        task::spawn(async move {
            let mut server = P::Server::new(ctx, stream).await.unwrap();
            println!("Server session for {} is now running.", addr);
            server.serve().await.unwrap();
        });
    }
}
