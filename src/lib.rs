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

mod constants;
mod message;
mod packet_pool;
pub mod raw;
mod socket;
// mod stream;
mod assembled_rx;
mod congestion;
mod rtte;
mod stream;
mod stream_tx;
mod utils;

pub use socket::UtpSocket;
pub use stream::UtpStream;
