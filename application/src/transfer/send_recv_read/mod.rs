use crate::transfer::Protocol;
use crate::transfer::send_recv_read::client::SendRecvReadClient;
use crate::transfer::send_recv_read::server::SendRecvReadServer;

mod client;
mod server;

pub struct SendRecvReadProtocol;

impl Protocol for SendRecvReadProtocol {
    type Client = SendRecvReadClient;
    type Server = SendRecvReadServer;
}
