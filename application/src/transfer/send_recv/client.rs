use crate::memory::jit::JitProvider;
use crate::memory::pooled::PooledProvider;
use crate::memory::{MemoryHandle, MemoryProvider};
use crate::transfer::send_recv::{ClientMeta, ServerMeta};
use crate::transfer::{Client, await_completions};
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, ibv_qp_type};
use std::io;
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use tracing::instrument;

pub struct SendRecvClient {
    ctx: Context,
    pd: Arc<ProtectionDomain>,
    cq: CompletionQueue,
    qp: QueuePair,

    id: u64,
    jit_provider: JitProvider,
    pooled_provider: PooledProvider,
}

impl Client for SendRecvClient {
    fn new<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let ctx = ibverbs::devices()?
            .get(0)
            .expect("No IB devices found")
            .open()?;
        let pd = Arc::new(ctx.alloc_pd()?);
        let cq = ctx.create_cq(4096, 0)?;
        let qp = {
            let qpb = pd
                .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)?
                .allow_remote_rw()
                .build()?;
            let local = qpb.endpoint()?;
            let data = serde_json::to_vec(&local)?;
            let mut buf = [0u8; 1024];
            let n = {
                let mut stream = TcpStream::connect(addr)?;
                stream.write_all(&data)?;
                stream.read(&mut buf)?
            };
            let remote = serde_json::from_slice(&buf[..n])?;
            qpb.handshake(remote)?
        };

        let id = 0;
        let jit_provider = JitProvider::new(pd.clone());
        let pooled_provider = PooledProvider::new(pd.clone());
        Ok(Self {
            ctx,
            pd,
            cq,
            qp,
            id,
            jit_provider,
            pooled_provider,
        })
    }

    #[instrument(skip(self), ret, err)]
    fn request(&mut self, req: u8) -> io::Result<MemoryHandle> {
        let id = self.id;

        let s_met: MemoryHandle<ServerMeta> = self.pooled_provider.allocate(1)?;
        let local = s_met.slice(..);
        unsafe {
            self.qp.post_receive(&[local], id)?;
        }
        let mut c_req: MemoryHandle<u8> = self.pooled_provider.allocate::<u8>(1)?;
        c_req[0] = req;
        let local = c_req.slice(..);
        unsafe {
            self.qp.post_send(&[local], id, None)?;
        }
        await_completions::<2>(&mut self.cq)?;
        let size = s_met[0].size;

        let mut c_meta: MemoryHandle<ClientMeta> = self.pooled_provider.allocate(1)?;
        let s_res: MemoryHandle<u8> = self.jit_provider.allocate(size)?;
        {
            c_meta[0].addr = s_res.remote().addr as usize;
            c_meta[0].rkey = s_res.remote().rkey as usize;
        }
        let local = s_res.slice(..size);
        unsafe {
            self.qp.post_receive(&[local], id)?;
        }
        let local = c_meta.slice(..);
        unsafe {
            self.qp.post_send(&[local], id, None)?;
        }

        await_completions::<2>(&mut self.cq)?;

        Ok(s_res)
    }
}
