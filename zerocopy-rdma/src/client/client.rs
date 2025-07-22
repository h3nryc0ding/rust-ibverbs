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
    pub fn request(&mut self, req: &EchoPacket) -> io::Result<&EchoPacket> {
        // 0. Reset receive MR
        self.0.mr_recv.inner().fill(0);

        // 1. Set send MR to request
        self.0.mr_send.inner().copy_from_slice(bytes_of(req));

        // 2. Write send MR to server
        let local = self.0.mr_send.slice(req.bounds());
        let remote = self.0.remote_mr.slice(req.bounds());
        self.0.qp.post_write(&[local], remote, 0, None)?;

        // 3. Wait for write completion
        utils::await_completions::<1>(&mut self.0.cq)?;

        // 4. Poll receive MR
        while *from_bytes::<EchoPacket>(self.0.mr_recv.inner()) == EchoPacket::zeroed() {}

        Ok(from_bytes(self.0.mr_recv.inner()))
    }
}
