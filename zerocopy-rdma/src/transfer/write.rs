use ibverbs::{Context, MemoryRegion, QueuePair, RemoteMemoryRegion};
use std::io;
use std::sync::Arc;
use tokio::net::TcpStream;
use tokio::sync::Mutex;

type QueryRequest = [u8; 256]; // example request
type MetaResponse = [u8; size_of::<u64>()]; // size of QueryResponse
type MetaRequest = [u8; size_of::<RemoteMemoryRegion>()]; // location to write
type QueryResponse = &'static [u8]; // example response

#[derive(Clone)]
pub struct WriteClient {
    qp: Arc<Mutex<QueuePair>>,

    send_req: Arc<Mutex<MemoryRegion<QueryRequest>>>,
    req_remote: RemoteMemoryRegion,
    send_meta: Arc<Mutex<MemoryRegion<MetaResponse>>>,
    meta_remote: RemoteMemoryRegion,
    recv_meta: Arc<Mutex<MemoryRegion<MetaRequest>>>,
    recv_data: Arc<Mutex<MemoryRegion<QueryResponse>>>,
}

impl WriteClient {
    async fn new(ctx: Context, stream: &mut TcpStream) -> io::Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn request(&mut self, req: QueryRequest) -> io::Result<QueryResponse> {
        let mut qp = self.qp.lock().await;
        // 1. Client sends request to server
        let mut send_req = self.send_req.lock().await;
        send_req.copy_from_slice(&req);
        let local = send_req.slice(0..size_of::<QueryRequest>());
        let remote = self.req_remote.slice(0..size_of::<QueryRequest>());

        qp.post_write(&[local], remote, 0, None)?;
        // 2. Client waits for meta response from server
        // 3. Client allocates memory region
        // 4. Client sends memory region info to server
        // 5. Client waits for server to write data

        todo!()
    }
}

#[derive(Clone)]
pub struct WriteServer;

impl WriteServer {
    async fn new(ctx: Context, stream: TcpStream) -> io::Result<Self>
    where
        Self: Sized,
    {
        todo!()
    }

    async fn serve(&mut self) -> io::Result<()> {
        // 1. Server waits for request from client
        // 2. Server sends meta response to client
        // 3. Server waits for memory region info from client
        // 4. Server writes data to client's memory region
        todo!()
    }
}
