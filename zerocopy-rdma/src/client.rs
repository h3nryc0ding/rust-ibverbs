use crate::protocol::QueryRequest;
use crate::record::MockRecord;
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

    let start = time::Instant::now();

    stream::iter(0..REQUESTS)
        .map(|id| {
            let count = id % CLIENT_RECORDS + 1;
            let offset = id % (SERVER_RECORDS - count);
            let req = QueryRequest { offset, count };
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
                Ok(records) => task::spawn_blocking(move || {
                    debug_span!("verify", request_id = id).in_scope(|| {
                        debug!("offset: {}", records[0].id == offset);
                        debug!("count: {}", records.len() == count);
                        debug!("checksum: {}", records.iter().all(MockRecord::validate));
                    });
                })
                .await
                .unwrap(),
                Err(e) => panic!("Request failed: {}", e),
            }
        })
        .await;

    let duration = start.elapsed();
    let records = REQUESTS * (CLIENT_RECORDS / 2);
    let transferred = records * size_of::<MockRecord>();

    println!(
        "Transferred {} records in {:.2?} ({:.2} GiB/s)",
        records,
        duration,
        transferred as f64 / duration.as_secs_f64() / (1024.0 * 1024.0 * 1024.0)
    );

    Ok(())
}
