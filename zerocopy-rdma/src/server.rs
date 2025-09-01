use crate::transfer::{Protocol, Server};
use tokio::net::TcpListener;
use tokio::{io, task};
use tracing::{instrument, span};

#[instrument(skip_all, fields(socket = %listener.local_addr().unwrap()))]
pub async fn run<'dev, P: Protocol>(
    dev: ibverbs::Device<'dev>,
    listener: TcpListener,
) -> io::Result<()>
where
    P::Server: Sync + Send,
{
    loop {
        let (stream, addr) = listener.accept().await?;

        let ctx = dev.open()?;
        task::spawn(async move {
            let _ = span!(tracing::Level::INFO, "client", %addr);
            let mut server = P::Server::new(ctx, stream).await.unwrap();
            server.serve().await.unwrap();
        });
    }
}
