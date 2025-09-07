use application::client::Client;
use std::{io, time};
use tracing::{Level, info, info_span};

const REQUESTS: usize = 64;

fn main() -> io::Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(Level::TRACE)
        .compact()
        .init();

    let mut client = Client::new("127.0.0.1:18515")?;
    info!("Client started. Sending requests...");

    let start = time::Instant::now();
    let mut received = 0;
    for i in 0..REQUESTS {
        let span = info_span!("request", request_id = i);
        let _enter = span.enter();

        let req = i as u8;
        info!("Sending request: {}", req);
        let res = client.request(req)?;
        info!("Received response: {:?}", res);

        received += res.len()
    }
    let duration = start.elapsed();
    let received_gb = received as f64 / 2f64.powi(30);

    info!(
        "Received {} GiB in {:?} ({} GiB/s)",
        received_gb,
        duration,
        received_gb / duration.as_secs_f64()
    );

    Ok(())
}
