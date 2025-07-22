use ibverbs::{
    CompletionQueue, Context, MemoryRegion, PreparedQueuePair, ProtectionDomain, QueuePair,
    QueuePairEndpoint, RemoteMemoryRegion, ibv_qp_type,
};
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Serialize, Deserialize)]
pub struct Endpoints {
    qp: QueuePairEndpoint,
    mr: RemoteMemoryRegion,
}

pub struct Initialized {
    ctx: Context,
    cq: CompletionQueue,
    pd: ProtectionDomain,
    qp: PreparedQueuePair,
    mr_send: MemoryRegion<Vec<u8>>,
    mr_recv: MemoryRegion<Vec<u8>>,
}

impl Initialized {
    pub fn new(ctx: Context) -> io::Result<Initialized> {
        let cq = ctx.create_cq(64, 0)?;
        let pd = ctx.alloc_pd()?;
        let qp = pd
            .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)?
            .allow_remote_rw()
            .build()?;
        let mr_send = pd.allocate(136)?;
        let mr_recv = pd.allocate(136)?;
        Ok(Initialized {
            ctx,
            cq,
            pd,
            qp,
            mr_send,
            mr_recv,
        })
    }

    pub fn endpoints(&self) -> io::Result<Endpoints> {
        Ok(Endpoints {
            qp: self.qp.endpoint()?,
            mr: self.mr_recv.remote(),
        })
    }

    pub fn connect(self, endpoints: Endpoints) -> io::Result<Connected> {
        let qp = self.qp.handshake(endpoints.qp)?;
        Ok(Connected {
            ctx: self.ctx,
            cq: self.cq,
            pd: self.pd,
            qp,
            mr_send: self.mr_send,
            mr_recv: self.mr_recv,
            remote_mr: endpoints.mr,
        })
    }
}

pub struct Connected {
    pub ctx: Context,
    pub cq: CompletionQueue,
    pub pd: ProtectionDomain,
    pub qp: QueuePair,
    pub mr_send: MemoryRegion<Vec<u8>>,
    pub mr_recv: MemoryRegion<Vec<u8>>,
    pub remote_mr: RemoteMemoryRegion,
}
