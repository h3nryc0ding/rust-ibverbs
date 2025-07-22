use crate::protocol::EchoPacket;
use crate::rdma::{connection, utils};
use crate::utils::json::{read_json, write_json};
use bytemuck::{Zeroable, bytes_of, from_bytes};
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
        loop {
            // 1. Poll for incoming request
            let request = from_bytes::<EchoPacket>(self.0.mr_recv.inner());
            if *request == EchoPacket::zeroed() {
                continue;
            }

            // 2. Process the request
            self.0.mr_send.inner().copy_from_slice(bytes_of(request));

            // 3. Write send MR to client
            let local = self.0.mr_send.slice(request.bounds());
            let remote = self.0.remote_mr.slice(request.bounds());
            self.0.qp.post_write(&[local], remote, 0, None)?;

            // 4. Wait for write completion
            utils::await_completions::<1>(&mut self.0.cq)?;

            // 5. Clear receive MR
            self.0.mr_recv.inner().fill(0);
        }
    }
}
