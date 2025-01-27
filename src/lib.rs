// "error: error running utp stream event loop: rx channel closed" - detect if it's ok
//
// TODO: congestion control
//
// TODO: extensions
// - selective ACKs
// - remote close reasons (qBittorrent sends those)
//
// TODO: proper closure
// - when client dropped, send all data in TX, then send FIN
// - poll_close and/or poll_shutdown impl
//
// TODO: tests (e.g. nagle etc)
//
// TODO: built-in connection timeouts?
//
// TODO: detect MTU at least through this crate https://github.com/mozilla/mtu/

mod constants;
mod message;
mod packet_pool;
pub mod raw;
mod socket;
// mod stream;
mod assembled_rx;
mod congestion;
mod rtte;
mod seq_nr;
mod stream;
mod stream_tx;
#[cfg(test)]
mod test_util;
mod traits;
mod utils;

pub use socket::UtpSocket;
pub use stream::{UtpStream, UtpStreamUdp};
pub use traits::Transport;
