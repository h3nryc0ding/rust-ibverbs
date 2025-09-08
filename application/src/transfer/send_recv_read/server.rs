use crate::memory::{MemoryHandle, Provider};
use crate::transfer::send_recv_read::ServerMeta;
use crate::transfer::{SERVER_SIZE, SIZE_SEED, Server, await_completions, handshake, synchronize};
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair};
use rand::{Rng, SeedableRng};
use rand_chacha::ChaCha8Rng;
use std::io;
use std::net::{TcpListener, ToSocketAddrs};
use std::sync::Arc;

pub struct SendRecvReadServer<M: Provider> {
    ctx: Context,
    pd: Arc<ProtectionDomain>,
    cq: CompletionQueue,
    qp: QueuePair,

    provider: M,
    s_res: MemoryHandle<u8>,
}

impl<M: Provider> Server<M> for SendRecvReadServer<M> {
    fn new(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let ctx = ibverbs::devices()?
            .get(0)
            .expect("No IB devices found")
            .open()?;

        let listener = TcpListener::bind(addr)?;
        let (mut stream, _) = listener.accept()?;
        let (pd, cq, qp) = handshake(&ctx, &mut stream)?;
        let pd = Arc::new(pd);

        let provider = M::new(pd.clone())?;
        let s_res: MemoryHandle<u8> = provider.allocate(SERVER_SIZE)?;

        synchronize(&mut stream)?;
        Ok(Self {
            ctx,
            pd,
            cq,
            qp,
            provider,
            s_res,
        })
    }

    fn serve(&mut self) -> io::Result<()> {
        let mut id = 0;
        let mut rng = ChaCha8Rng::seed_from_u64(SIZE_SEED);
        loop {
            let size = rng.random_range(0..SERVER_SIZE);

            let c_req: MemoryHandle<u8> = self.provider.allocate(1)?;
            let local = c_req.slice(..);
            unsafe { self.qp.post_receive(&[local], id)? }
            await_completions::<1>(&mut self.cq)?;

            let mut s_met: MemoryHandle<ServerMeta> = self.provider.allocate(1)?;
            s_met[0] = ServerMeta::from(self.s_res.remote().slice(0..size));

            let local = s_met.slice(..);
            unsafe { self.qp.post_send(&[local], id + 1, None)? }
            await_completions::<1>(&mut self.cq)?;

            id += 3;
        }
    }
}
