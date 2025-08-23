use crate::protocol::QueryRequest;
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol};
use tokio::io;
use tokio::net::TcpStream;

const REQUESTS: usize = 100_000;

pub async fn run<P: Protocol>(dev: ibverbs::Device<'_>, mut stream: TcpStream) -> io::Result<()> {
    let ctx = dev.open()?;
    let mut client = P::Client::new(ctx, &mut stream).await?;

    for i in 0..REQUESTS {
        let req = QueryRequest {
            offset: i,
            count: i + 1,
        };
        let res = client.request(req).await?;
        assert_eq!(res[0].id, i);
        assert!(res.iter().all(MockRecord::validate));
        println!("Received {} records in request {i}", res.len());
    }
    Ok(())
}
