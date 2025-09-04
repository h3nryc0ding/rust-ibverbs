use crate::protocol::QueryRequest;
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol, RECORDS};
use futures::{StreamExt, stream};
use tokio::net::TcpStream;
use tokio::{io, task, time};
use tracing::{Instrument, debug, debug_span};

const REQUESTS: usize = 200;
const CONCURRENT_REQUESTS: usize = 5;

pub async fn run<P: Protocol>(dev: ibverbs::Device<'_>, mut stream: TcpStream) -> io::Result<()>
where
    P::Client: Clone,
{
    let ctx = dev.open()?;
    let client = P::Client::new(ctx, &mut stream).await?;

    let start = time::Instant::now();

    stream::iter(0..REQUESTS)
        .map(|i| {
            let req = QueryRequest {
                offset: i,
                count: RECORDS / 2,
            };
            let mut client = client.clone();

            let span = debug_span!("", request_id = i);
            async move {
                debug!("Sending");
                let res = client.request(req).await;
                debug!("Received");
                (i, res)
            }
            .instrument(span)
        })
        .buffer_unordered(CONCURRENT_REQUESTS)
        .for_each_concurrent(None, |(i, res)| async move {
            match res {
                Ok(records) => task::spawn_blocking(move || {
                    assert_eq!(records[0].id, i);
                    assert!(records.iter().all(MockRecord::validate));
                })
                .await
                .unwrap(),
                Err(e) => panic!("Request failed: {}", e),
            }
        })
        .await;

    let duration = start.elapsed();
    let records = REQUESTS * (RECORDS / 2);
    let transferred = records * size_of::<MockRecord>();

    println!(
        "Transferred {} records in {:.2?} ({:.2} GiB/s)",
        records,
        duration,
        transferred as f64 / duration.as_secs_f64() / (1024.0 * 1024.0 * 1024.0)
    );

    Ok(())
}
