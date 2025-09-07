use application::client::Client;
use std::io;

fn main() -> io::Result<()> {
    let mut client = Client::new("127.0.0.1:18515")?;
    println!("Client started. Sending requests...");

    for i in 0..1024 {
        let req = i as u8;
        println!("Sending request: {:?}", req);
        let res = client.request(req)?;
        println!("Received response: {:?}", res);
    }

    Ok(())
}
