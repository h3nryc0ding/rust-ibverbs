use crate::rdma::connection;
use crate::utils::json::{read_json, write_json};
use ibverbs::Context;
use std::{io, net};

pub struct Initialized(connection::Initialized);

impl Initialized {
    pub fn new(ctx: Context) -> io::Result<Initialized> {
        let connection = connection::Initialized::new(ctx)?;
        Ok(Initialized(connection))
    }

    pub fn connect(self, socket: net::SocketAddr) -> io::Result<Connected> {
        let mut stream = net::TcpStream::connect(socket)?;

        let local = self.0.endpoints()?;
        write_json(&mut stream, &local)?;
        let remote = read_json(&mut stream)?;

        let connection = self.0.connect(remote)?;
        Ok(Connected(connection))
    }
}

pub struct Connected(connection::Connected);

impl Connected {
    pub fn request(&mut self, req: &[u8]) -> io::Result<&[u8]> {
        todo!()
    }
}
