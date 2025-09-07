use crate::transfer::Protocol;
use crate::transfer::send_recv::client::SendRecvClient;
use crate::transfer::send_recv::server::SendRecvServer;

pub mod client;
pub mod server;

pub struct SendRecvProtocol;

impl Protocol for SendRecvProtocol {
    type Client = SendRecvClient;
    type Server = SendRecvServer;
}

#[repr(C)]
#[derive(Debug, Default, Clone)]
pub struct ServerMeta {
    size: usize,
}

#[repr(C)]
#[derive(Debug, Default, Clone)]
pub struct ClientMeta {
    addr: usize,
    rkey: usize,
}
