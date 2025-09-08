use crate::memory::Provider;
use crate::transfer::Protocol;
use crate::transfer::send_recv::client::SendRecvClient;
use crate::transfer::send_recv::server::SendRecvServer;

pub mod client;
pub mod server;

pub struct SendRecvProtocol;

impl Protocol for SendRecvProtocol {
    type Client<M: Provider> = SendRecvClient<M>;
    type Server<M: Provider> = SendRecvServer<M>;
}

#[repr(C)]
#[derive(Debug, Default, Clone)]
struct ServerMeta {
    size: u64,
}
