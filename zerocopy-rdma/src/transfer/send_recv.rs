use crate::protocol::QueryRequest;
use crate::rdma::Connection;
use crate::rdma::utils::await_completions;
use crate::record::MockRecord;
use crate::transfer::{Client, Protocol, RECORDS, SendRecvProtocol, Server};
use bytemuck::Zeroable;
use ibverbs::MemoryRegion;
use std::io;

pub struct SendRecvClient {
    c: Connection,
    recv: MemoryRegion<Vec<MockRecord>>,
    send: MemoryRegion<Vec<QueryRequest>>,
}

impl Client for SendRecvClient {
    fn new(c: Connection) -> io::Result<Self> {
        let recv_data = (0..RECORDS as u64).map(MockRecord::new).collect();
        let recv = c.pd.register(recv_data)?;
        let send_data = vec![QueryRequest::zeroed(); 1];
        let send = c.pd.register(send_data)?;
        println!("Client setup completed");
        Ok(Self { c, recv, send })
    }

    fn request(&mut self, r: QueryRequest) -> io::Result<Vec<MockRecord>> {
        let c = &mut self.c;
        let send = &mut self.send.inner()[0];

        *send = r;

        let local_send = self.recv.slice(&(0..RECORDS * size_of::<MockRecord>()));
        unsafe { c.qp.post_receive(&[local_send], 0)? }

        let local_recv = self.send.slice(&(0..size_of::<QueryRequest>()));
        unsafe { c.qp.post_send(&[local_recv], 0)? }
        await_completions::<2>(&mut c.cq)?;

        Ok(self.recv.inner().clone())
    }
}

pub struct SendRecvServer {
    c: Connection,
    recv: MemoryRegion<Vec<QueryRequest>>,
    send: MemoryRegion<Vec<MockRecord>>,
}

impl Server for SendRecvServer {
    fn new(c: Connection) -> io::Result<Self> {
        let send_data = (0..RECORDS as u64).map(MockRecord::new).collect();
        let send = c.pd.register(send_data)?;
        let recv_data = vec![QueryRequest::zeroed(); 1];
        let recv = c.pd.register(recv_data)?;
        println!("Server setup completed");
        Ok(Self { c, recv, send })
    }

    fn serve(&mut self) -> io::Result<()> {
        let c = &mut self.c;
        loop {
            let local_recv = self.recv.slice(&(0..size_of::<QueryRequest>()));
            unsafe { c.qp.post_receive(&[local_recv], 0)? }
            await_completions::<1>(&mut c.cq)?;

            let offset = self.recv.inner()[0].offset as usize;
            let limit = self.recv.inner()[0].limit as usize;

            let local_send = self.send.slice(
                &(offset * size_of::<MockRecord>()..(offset + limit) * size_of::<MockRecord>()),
            );
            unsafe { c.qp.post_send(&[local_send], 0)? }
            await_completions::<1>(&mut c.cq)?;
        }
    }
}

impl Protocol for SendRecvProtocol {
    type Client = SendRecvClient;
    type Server = SendRecvServer;
}
