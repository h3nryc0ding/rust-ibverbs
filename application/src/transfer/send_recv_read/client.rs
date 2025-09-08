use crate::memory::{MemoryHandle, Provider};
use crate::transfer::send_recv_read::ServerMeta;
use crate::transfer::{Client, await_completions, handshake, synchronize};
use ibverbs::{CompletionQueue, Context, ProtectionDomain, QueuePair, RemoteMemorySlice};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::Arc;
use tracing::instrument;

pub struct SendRecvReadClient<M: Provider> {
    ctx: Context,
    pd: Arc<ProtectionDomain>,
    cq: CompletionQueue,
    qp: QueuePair,

    id: u64,
    provider: M,
}

impl<M: Provider> Client<M> for SendRecvReadClient<M> {
    fn new(addr: impl ToSocketAddrs) -> io::Result<Self> {
        let ctx = ibverbs::devices()?
            .get(0)
            .expect("No IB devices found")
            .open()?;
        let mut stream = TcpStream::connect(addr)?;
        let (pd, cq, qp) = handshake(&ctx, &mut stream)?;
        let pd = Arc::new(pd);

        let id = 0;
        let provider = M::new(pd.clone())?;

        synchronize(&mut stream)?;
        Ok(Self {
            ctx,
            pd,
            cq,
            qp,
            id,
            provider,
        })
    }

    #[instrument(skip(self), ret, err)]
    fn request(&mut self, req: u8) -> io::Result<MemoryHandle> {
        let id = self.id;

        let s_met: MemoryHandle<ServerMeta> = self.provider.allocate(1)?;
        let local = s_met.slice(..);
        unsafe {
            self.qp.post_receive(&[local], id)?;
        }
        let mut c_req: MemoryHandle<u8> = self.provider.allocate::<u8>(1)?;
        c_req[0] = req;
        let local = c_req.slice(..);
        unsafe {
            self.qp.post_send(&[local], id + 1, None)?;
        }
        await_completions::<2>(&mut self.cq)?;

        let remote = RemoteMemorySlice::from(&s_met[0]);
        let size = remote.len();
        let s_res: MemoryHandle<u8> = self.provider.allocate(size)?;
        let local = s_res.slice(..size);
        unsafe {
            self.qp.post_read(&[local], remote, id + 2)?;
        }
        await_completions::<1>(&mut self.cq)?;

        self.id += 3;
        Ok(s_res)
    }
}
