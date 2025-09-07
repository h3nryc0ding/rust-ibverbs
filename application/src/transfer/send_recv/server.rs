use crate::memory::jit::JitProvider;
use crate::memory::pooled::PooledProvider;
use crate::memory::{MemoryHandle, MemoryProvider};
use crate::transfer::send_recv::{ClientMeta, ServerMeta};
use crate::transfer::{SERVER_SIZE, Server, await_completions};
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, ibv_qp_type};
use rand::Rng;
use rand_chacha::ChaCha8Rng;
use rand_chacha::rand_core::SeedableRng;
use std::io;
use std::io::{Read, Write};
use std::net::{TcpListener, ToSocketAddrs};
use std::sync::Arc;

pub struct SendRecvServer {
    ctx: Context,
    pd: Arc<ProtectionDomain>,
    cq: CompletionQueue,
    qp: QueuePair,

    jit_provider: JitProvider,
    pooled_provider: PooledProvider,

    s_res: MemoryHandle<u8>,
}

impl Server for SendRecvServer {
    fn new<A: ToSocketAddrs>(addr: A) -> io::Result<Self> {
        let ctx = ibverbs::devices()?
            .get(0)
            .expect("No IB devices found")
            .open()?;
        let pd = Arc::new(ctx.alloc_pd()?);
        let cq = ctx.create_cq(4096, 0)?;

        let jit_provider = JitProvider::new(pd.clone());
        let pooled_provider = PooledProvider::new(pd.clone());
        let s_res: MemoryHandle<u8> = pooled_provider.allocate(SERVER_SIZE)?;

        let qp = {
            let qpb = pd
                .create_qp(&cq, &cq, ibv_qp_type::IBV_QPT_RC)?
                .allow_remote_rw()
                .build()?;
            let local = qpb.endpoint()?;
            let data = serde_json::to_vec(&local)?;
            let mut buf = [0u8; 1024];
            let n = {
                let listener = TcpListener::bind(addr).unwrap();
                let (mut stream, _) = listener.accept()?;
                stream.write_all(&data)?;
                stream.read(&mut buf)?
            };
            let remote = serde_json::from_slice(&buf[..n])?;
            qpb.handshake(remote)?
        };

        Ok(Self {
            ctx,
            pd,
            cq,
            qp,
            jit_provider,
            pooled_provider,
            s_res,
        })
    }

    fn serve(&mut self) -> io::Result<()> {
        let mut id = 0;
        let mut rng = ChaCha8Rng::seed_from_u64(1337);
        loop {
            let size = rng.random_range(0..SERVER_SIZE);

            let c_req: MemoryHandle<u8> = self.pooled_provider.allocate(1)?;
            let local = c_req.slice(..);
            unsafe { self.qp.post_receive(&[local], id)? }
            await_completions::<1>(&mut self.cq)?;

            let mut s_met: MemoryHandle<ServerMeta> = self.pooled_provider.allocate(1)?;
            s_met[0].size = size;

            let c_met: MemoryHandle<ClientMeta> = self.pooled_provider.allocate(1)?;
            let local = c_met.slice(..);
            unsafe { self.qp.post_receive(&[local], id)? }
            let local = s_met.slice(..);
            unsafe { self.qp.post_send(&[local], id, None)? }
            await_completions::<2>(&mut self.cq)?;

            let local = self.s_res.slice(..size);
            unsafe { self.qp.post_send(&[local], id, None)? }
            await_completions::<1>(&mut self.cq)?;

            id += 1;
        }
    }
}
