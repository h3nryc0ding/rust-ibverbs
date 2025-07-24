use crate::protocol;
use crate::protocol::{BoundedPacket, DataPacket, MetaPacket};
use crate::rdma::{connection, utils};
use crate::utils::json::{read_json, write_json};
use ibverbs::Context;
use std::{hint, io, net, ptr};

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
        Ok(Connected { connection })
    }
}

pub struct Connected {
    connection: connection::Connected,
}

impl Connected {
    pub fn request(&mut self, req: &DataPacket) -> io::Result<&DataPacket> {
        let con = &mut self.connection;

        let (recv_data_pkt, recv_meta_pkt, send_data_pkt, send_meta_pkt) = unsafe {
            // SAFETY: MRs are valid for the lifetime of the request
            (
                &mut *(con.mr_recv_data.inner().as_mut_ptr() as *mut DataPacket),
                &mut *(con.mr_recv_meta.inner().as_mut_ptr() as *mut MetaPacket),
                &mut *(con.mr_send_data.inner().as_mut_ptr() as *mut DataPacket),
                &mut *(con.mr_send_meta.inner().as_mut_ptr() as *mut MetaPacket),
            )
        };
        recv_meta_pkt.status = protocol::Status::Empty;
        send_meta_pkt.status = protocol::Status::Ready;
        *send_data_pkt = req.clone();

        let bounds = send_data_pkt.bounds();
        let local = con.mr_send_data.slice(&bounds);
        let remote = con.remote_mr_data.slice(&bounds);
        con.qp.post_write(&[local], remote, 0, None)?;
        utils::await_completions::<1>(&mut con.cq)?;

        let bounds = send_meta_pkt.bounds();
        let local = con.mr_send_meta.slice(&bounds);
        let remote = con.remote_mr_meta.slice(&bounds);
        con.qp.post_write(&[local], remote, 0, None)?;
        utils::await_completions::<1>(&mut con.cq)?;

        while unsafe { ptr::read_volatile(recv_meta_pkt) }.is_empty() {
            hint::spin_loop();
        }

        Ok(recv_data_pkt)
    }
}
