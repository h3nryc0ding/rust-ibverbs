use crate::protocol::QueryRequest;
use crate::transfer::{CLIENT_RECORDS, Client, Protocol, SERVER_RECORDS};
use futures::{StreamExt, stream};
use tokio::net::TcpStream;
use tokio::{io, task, time};
use tracing::{Instrument, debug, debug_span};

const REQUESTS: usize = 1_000;
const CONCURRENT_REQUESTS: usize = 5;

pub async fn run<P: Protocol>(dev: ibverbs::Device<'_>, mut stream: TcpStream) -> io::Result<()>
where
    P::Client: Clone,
{
    let ctx = dev.open()?;
    let client = P::Client::new(ctx, &mut stream).await?;

    stream::iter(0..REQUESTS)
        .map(|id| {
            let count = id % CLIENT_RECORDS + 1;
            let offset = id % (SERVER_RECORDS - count);
            let req = QueryRequest { offset };
            let mut client = client.clone();

            let span = debug_span!("", request_id = id);
            async move {
                debug!("Sending");
                let res = client.request(req).await;
                debug!("Received");
                (id, offset, count, res)
            }
            .instrument(span)
        })
        .buffer_unordered(CONCURRENT_REQUESTS)
        .for_each_concurrent(None, |(id, offset, count, res)| async move {
            match res {
                Ok(bytes) => task::spawn_blocking(move || {
                    debug_span!("verify", request_id = id, res = ?bytes).in_scope(|| {
                        debug!("count: {}", bytes.len() == count);
                        debug!("offset: {}", bytes.len() > 0 && bytes[0] == offset as u8);
                    });
                })
                .await
                .unwrap(),
                Err(e) => panic!("Request failed: {}", e),
            }
        })
        .await;

    Ok(())
}
