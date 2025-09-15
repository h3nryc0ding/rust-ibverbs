use crate::{BINCODE_CONFIG, MB};
use bincode::serde::{decode_from_std_read, encode_into_std_write};
use ibverbs::ibv_qp_type::IBV_QPT_RC;
use ibverbs::{
    CompletionQueue, Context, MemoryRegion, ProtectionDomain, QueuePair, RemoteMemorySlice, ibv_wc,
};
use std::cmp::min;
use std::collections::{HashMap, VecDeque};
use std::io;
use std::net::{TcpStream, ToSocketAddrs};
use std::sync::mpsc;
use tracing::{instrument, trace};

pub struct Client {
    ctx: Context,
    pd: ProtectionDomain,
    cq: CompletionQueue,
    qp: QueuePair,

    remote: RemoteMemorySlice,
}

impl Client {
    pub fn new(ctx: Context, addr: impl ToSocketAddrs) -> io::Result<Self> {
        let mut stream = TcpStream::connect(addr)?;
        let pd = ctx.alloc_pd()?;
        let cq = ctx.create_cq(1024, 0)?;

        let qp = {
            let pqp = pd
                .create_qp(&cq, &cq, IBV_QPT_RC)?
                .allow_remote_rw()
                .build()?;
            let remote = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
            trace!("Server remote endpoint: {:?}", remote);
            let local = pqp.endpoint()?;
            trace!("Client local endpoint: {:?}", local);
            encode_into_std_write(&local, &mut stream, BINCODE_CONFIG).unwrap();
            pqp.handshake(remote)?
        };

        let remote = decode_from_std_read(&mut stream, BINCODE_CONFIG).unwrap();
        trace!("Server remote slice: {:?}", remote);

        Ok(Self {
            ctx,
            pd,
            cq,
            qp,
            remote,
        })
    }

    #[instrument(skip(self), err)]
    pub fn simple_request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        let local = self.pd.allocate(size)?;
        unsafe {
            self.qp
                .post_read(&[local.slice_local(..)], self.remote, 0)?
        };

        let mut completed = false;
        let mut completions = [ibv_wc::default(); 1];
        while !completed {
            for completion in self.cq.poll(&mut completions)? {
                if let Some((e, _)) = completion.error() {
                    panic!("wc error: {:?}", e)
                }
                completed = true;
            }
        }

        local.deregister()
    }

    #[instrument(skip(self), err)]
    pub fn split_request(&mut self, size: usize) -> io::Result<Box<[u8]>> {
        static MR_SIZE: usize = 4 * MB;
        let mut result: Vec<u8> = Vec::with_capacity(size);
        unsafe { result.set_len(size) };
        let result_ptr = result.as_mut_ptr();

        let chunks = (size + MR_SIZE - 1) / MR_SIZE;
        let mut outstanding_mrs = HashMap::new();
        let mut reg_queue = VecDeque::new();
        let mut post_queue = VecDeque::new();

        for i in 0..chunks {
            let start = i * MR_SIZE;
            let end = min(start + MR_SIZE, size);
            let len = end - start;
            let ptr = unsafe { result_ptr.add(start) };
            reg_queue.push_back((i as u64, ptr, len));
        }

        while !reg_queue.is_empty() || !post_queue.is_empty() || !outstanding_mrs.is_empty() {
            if let Some((id, ptr, len)) = reg_queue.pop_front() {
                match unsafe { self.pd.register_unchecked::<u8>(ptr, len) } {
                    Ok(mr) => {
                        trace!("registered mr id={} ptr={:?} len={}", id, ptr, len);
                        post_queue.push_back((id, mr))
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                        trace!("registration OOM, retrying later");
                        reg_queue.push_back((id, ptr, len));
                    }
                    Err(e) => panic!("register error: {:?}", e),
                }
            }

            if let Some((id, mr)) = post_queue.pop_front() {
                let local = mr.slice_local(..);
                match unsafe { self.qp.post_read(&[local], self.remote, id) } {
                    Ok(_) => {
                        trace!("posted read id={} len={}", id, local.len());
                        outstanding_mrs.insert(id, mr);
                    }
                    Err(e) if e.kind() == io::ErrorKind::OutOfMemory => {
                        trace!("post_read OOM, retrying later");
                        post_queue.push_back((id, mr));
                    }
                    Err(e) => panic!("post_read error: {:?}", e),
                }
            }

            let mut completions = [ibv_wc::default(); 16];
            for completion in self.cq.poll(&mut completions)? {
                if let Some((e, _)) = completion.error() {
                    panic!("wc error: {:?}", e)
                }
                let id = completion.wr_id();
                if let Some(mr) = outstanding_mrs.remove(&id) {
                    let ptr = mr.addr() as *mut u8;
                    let len = mr.len();
                    mr.deregister()?;
                    trace!("registered mr id={} ptr={:?} len={}", &id, ptr, len);
                } else {
                    panic!("unknown wr_id: {}", id);
                }
            }
        }

        Ok(result.into_boxed_slice())
    }
}
