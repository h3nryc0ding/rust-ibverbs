use crate::protocol::{DataPacket, MetaPacket};
use ibverbs::{
    CompletionQueue, Context, MemoryRegion, PreparedQueuePair, ProtectionDomain, QueuePair,
    QueuePairEndpoint, RemoteMemoryRegion, ibv_qp_type,
};
use serde::{Deserialize, Serialize};
use std::io;

#[derive(Serialize, Deserialize)]
pub struct Endpoints {
    qp: QueuePairEndpoint,
    mr_data: RemoteMemoryRegion,
    mr_meta: RemoteMemoryRegion,
}

pub struct Initialized {
    ctx: Context,
    cq: CompletionQueue,
    pd: ProtectionDomain,
    qp: PreparedQueuePair,
    mr_send_data: MemoryRegion<Vec<u8>>,
    mr_send_meta: MemoryRegion<Vec<u8>>,
    mr_recv_data: MemoryRegion<Vec<u8>>,
    mr_recv_meta: MemoryRegion<Vec<u8>>,
}

impl Initialized {
    pub fn new(ctx: Context) -> io::Result<Initialized> {
        let cq = ctx.create_cq(64, 0)?;
        let pd = ctx.alloc_pd()?;
        let qp = pd
            .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)?
            .allow_remote_rw()
            .build()?;
        let mr_send_data = pd.allocate(size_of::<DataPacket>() + 1)?;
        let mr_send_meta = pd.allocate(size_of::<MetaPacket>())?;
        let mr_recv_data = pd.allocate(size_of::<DataPacket>())?;
        let mr_recv_meta = pd.allocate(size_of::<MetaPacket>())?;
        Ok(Initialized {
            ctx,
            cq,
            pd,
            qp,
            mr_send_data,
            mr_send_meta,
            mr_recv_data,
            mr_recv_meta,
        })
    }

    pub fn endpoints(&self) -> io::Result<Endpoints> {
        Ok(Endpoints {
            qp: self.qp.endpoint()?,
            mr_data: self.mr_recv_data.remote(),
            mr_meta: self.mr_recv_meta.remote(),
        })
    }

    pub fn connect(self, endpoints: Endpoints) -> io::Result<Connected> {
        let qp = self.qp.handshake(endpoints.qp)?;
        Ok(Connected {
            ctx: self.ctx,
            cq: self.cq,
            pd: self.pd,
            qp,
            mr_send_data: self.mr_send_data,
            mr_send_meta: self.mr_send_meta,
            mr_recv_data: self.mr_recv_data,
            mr_recv_meta: self.mr_recv_meta,
            remote_mr_data: endpoints.mr_data,
            remote_mr_meta: endpoints.mr_meta,
        })
    }
}

pub struct Connected {
    pub ctx: Context,
    pub cq: CompletionQueue,
    pub pd: ProtectionDomain,
    pub qp: QueuePair,
    pub mr_send_data: MemoryRegion<Vec<u8>>,
    pub mr_send_meta: MemoryRegion<Vec<u8>>,
    pub mr_recv_data: MemoryRegion<Vec<u8>>,
    pub mr_recv_meta: MemoryRegion<Vec<u8>>,
    pub remote_mr_data: RemoteMemoryRegion,
    pub remote_mr_meta: RemoteMemoryRegion,
}
