mod client;

use crate::protocol::EchoPacket;
use std::{io, net};

pub fn run(ctx: ibverbs::Context, socket: net::SocketAddr) -> io::Result<()> {
    let client = client::Initialized::new(ctx)?;
    println!("Client successfully initialized, connecting to {}", socket);

    let mut client = client.connect(socket)?;
    println!("Connected to server at {}", socket);

    for i in 0..usize::MAX {
        let req = EchoPacket::new(&format!("{}", i));
        println!("Sending request: {}", &req.as_str());
        let res = client.request(&req)?;
        println!("Sending response: {}", res.as_str());
    }

    Ok(())
}
