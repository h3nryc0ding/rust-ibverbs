use crate::protocol::QueryRequest;
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol, RECORDS};
use futures::{StreamExt, stream};
use tokio::io;
use tokio::net::TcpStream;

const REQUESTS: usize = 10;
const CONCURRENT_REQUESTS: usize = 2;

pub async fn run<P: Protocol>(dev: ibverbs::Device<'_>, mut stream: TcpStream) -> io::Result<()>
where
    P::Client: Clone,
{
    let ctx = dev.open()?;
    let client = P::Client::new(ctx, &mut stream).await?;

    stream::iter(0..REQUESTS)
        .map(|i| QueryRequest {
            offset: i,
            count: RECORDS / 2,
        })
        .map(|r| {
            let mut client = client.clone();
            println!("Client cloned");
            async move {
                println!("Requesting records {} to {}", r.offset, r.offset + r.count);
                client.request(r).await
            }
        })
        .buffer_unordered(CONCURRENT_REQUESTS)
        .for_each_concurrent(None, |res| async {
            match res {
                Ok(records) => {
                    println!("Received {} records", records.len());
                    assert!(records.iter().all(MockRecord::validate))
                },
                Err(e) => eprintln!("Request failed: {}", e),
            }
        })
        .await;

    Ok(())
}
