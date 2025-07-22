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

    pub fn accept(self, socket: net::SocketAddr) -> io::Result<Connected> {
        let listener = net::TcpListener::bind(socket)?;
        let (mut stream, _) = listener.accept()?;

        let local = self.0.endpoints()?;
        let remote = read_json(&mut stream)?;
        write_json(&mut stream, &local)?;

        let connection = self.0.connect(remote)?;
        Ok(Connected(connection))
    }
}

pub struct Connected(connection::Connected);

impl Connected {
    pub fn serve(&mut self) -> io::Result<()> {
        todo!()
    }
}
