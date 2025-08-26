use crate::protocol::QueryRequest;
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol};
use tokio::net::TcpStream;
use tokio::io;

const REQUESTS: usize = 100;

pub async fn run<P: Protocol>(dev: ibverbs::Device<'_>, mut stream: TcpStream) -> io::Result<()> {
    let ctx = dev.open()?;
    let mut client = P::Client::new(ctx, &mut stream).await?;

    for i in 0..REQUESTS {
        let req = QueryRequest {
            offset: i,
            count: 512 * 1024,
        };
        let res = client.request(req).await?;
        validate(i, &res);
    }
    Ok(())
}

fn validate(id: usize, records: &[MockRecord]) {
    assert_eq!(records[0].id, id);
    for record in records {
        assert!(MockRecord::validate(record));
    }
}
