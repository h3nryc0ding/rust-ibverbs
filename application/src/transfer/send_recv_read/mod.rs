use crate::memory::Provider;
use crate::transfer::Protocol;
use crate::transfer::send_recv_read::client::SendRecvReadClient;
use crate::transfer::send_recv_read::server::SendRecvReadServer;
use ibverbs::RemoteMemorySlice;

mod client;
mod server;

pub struct SendRecvReadProtocol;

impl Protocol for SendRecvReadProtocol {
    type Client<M: Provider> = SendRecvReadClient<M>;
    type Server<M: Provider> = SendRecvReadServer<M>;
}

#[repr(C)]
#[derive(Debug, Default, Clone)]
struct ServerMeta(RemoteMemorySlice);

impl From<RemoteMemorySlice> for ServerMeta {
    fn from(value: RemoteMemorySlice) -> Self {
        Self(value)
    }
}

impl From<&ServerMeta> for RemoteMemorySlice {
    fn from(value: &ServerMeta) -> Self {
        value.0
    }
}
