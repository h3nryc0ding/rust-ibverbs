mod client;

use crate::protocol;
use std::{io, net};

pub fn run(ctx: ibverbs::Context, socket: net::SocketAddr) -> io::Result<()> {
    let client = client::Initialized::new(ctx)?;
    println!("Client successfully initialized, connecting to {}", socket);

    let mut client = client.connect(socket)?;
    println!("Connected to server at {}", socket);

    let requests = ["Hello World", "Foo Bar Baz", "Lorem Impsum"];

    for &request in requests.iter() {
        let req = protocol::EchoPacket::new(request);
        println!("Sending request: {}", &req.as_str());
        let res = client.request(&req)?;
        println!("Received response: {}", res.as_str());
    }

    Ok(())
}
