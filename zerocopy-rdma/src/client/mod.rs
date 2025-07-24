mod client;

use crate::protocol::DataPacket;
use std::{io, net};

pub fn run(ctx: ibverbs::Context, socket: net::SocketAddr) -> io::Result<()> {
    let client = client::Initialized::new(ctx)?;
    println!("Client successfully initialized, connecting to {}", socket);

    let mut client = client.connect(socket)?;
    println!("Connected to server at {}", socket);

    for i in 0..usize::MAX {
        let req = DataPacket::new(&format!("{}", i));
        if i % 10_000 == 0 {
            println!("Sending request: {}", &req.as_str())
        };
        let res = client.request(&req)?;
        if i % 10_000 == 0 {
            println!("Sending response: {}", res.as_str());
        }
    }

    Ok(())
}
