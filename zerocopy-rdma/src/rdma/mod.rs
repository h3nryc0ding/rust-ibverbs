use crate::utils::json::{read_json, write_json};
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, ibv_qp_type};
use std::{io, net};

pub mod utils;

pub struct Connection {
    pub ctx: Context,
    pub pd: ProtectionDomain,
    pub cq: CompletionQueue,
    pub qp: QueuePair,
}

impl Connection {
    pub fn connect(ctx: Context, addr: net::SocketAddr) -> io::Result<Self> {
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(128, 1337)?;

        let qp = pd
            .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)?
            .allow_remote_rw()
            .build()?;

        let local = qp.endpoint()?;
        let remote = {
            let mut stream = net::TcpStream::connect(addr)?;
            write_json(&mut stream, &local)?;
            read_json(&mut stream)?
        };
        let qp = qp.handshake(remote)?;
        Ok(Self { ctx, pd, cq, qp })
    }

    pub fn accept(ctx: Context, addr: net::SocketAddr) -> io::Result<Self> {
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(128, 1337)?;

        let qp = pd
            .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)?
            .allow_remote_rw()
            .build()?;

        let local = qp.endpoint()?;
        let remote = {
            let listener = net::TcpListener::bind(addr)?;
            let (mut stream, _) = listener.accept()?;
            write_json(&mut stream, &local)?;
            read_json(&mut stream)?
        };
        let qp = qp.handshake(remote)?;
        Ok(Self { ctx, pd, cq, qp })
    }
}
