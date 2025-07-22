use crate::protocol::EchoPacket;
use crate::rdma::{connection, utils};
use crate::utils::json::{read_json, write_json};
use bytemuck::from_bytes_mut;
use ibverbs::Context;
use std::{hint, io, net, ptr};

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
        Ok(Connected { connection })
    }
}

pub struct Connected {
    connection: connection::Connected,
}

impl Connected {
    pub fn serve(&mut self) -> io::Result<()> {
        let con = &mut self.connection;

        loop {
            let recv_pkt = from_bytes_mut::<EchoPacket>(con.mr_recv.inner());
            let send_pkt = from_bytes_mut::<EchoPacket>(con.mr_send.inner());

            let recv_ptr = recv_pkt as *const EchoPacket;
            while unsafe { ptr::read_volatile(recv_ptr) }.is_reset() {
                hint::spin_loop();
            }

            *send_pkt = *recv_pkt;
            recv_pkt.reset();

            let bounds = send_pkt.bounds();
            let local = con.mr_send.slice(&bounds);
            let remote = con.remote_mr.slice(&bounds);
            con.qp.post_write(&[local], remote, 0, None)?;

            utils::await_completions::<1>(&mut con.cq)?;
        }
    }
}
