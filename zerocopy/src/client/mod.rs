mod client;

use std::{io, net};

pub fn run(ctx: ibverbs::Context, socket: net::SocketAddr) -> io::Result<()> {
    let client = client::Initialized::new(ctx)?;
    println!("Client successfully initialized, connecting to {}", socket);

    let mut client = client.connect(socket)?;
    println!("Connected to server at {}", socket);

    let req = b"hello world";
    println!("Sending request: {}", String::from_utf8_lossy(req));
    let res = client.request(&req[..])?;
    println!("Received response: {}", String::from_utf8_lossy(&res));

    Ok(())
}
