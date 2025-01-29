// TODO: flow control
// - it's crucial to bound the amount of data the remote is allowed to send us.
// - for librqbit, it should be ok, but for a general library definitely not
//
// TODO: Initial SYN - keep resending it with increasing delay. This would require refactoring SYN
// into the VirtualSocket state machine (like smoltcp), cause otherwise it'll get nasty.
//
// TODO: packet pool is inefficient as:
// - the packet sizes vary greatly
// - detecting MTU might work only for outgoing hosts, but not for incoming hosts
//
// TODO: memory management. Think about a data structure that will let the UDP incoming packet
// get written straight out to use Rx buffer.

// TODO: LEDBAT congestion control
//
// TODO: extensions
// - selective ACKs
//
// TODO: built-in connection timeouts?

mod assembled_rx;
mod congestion;
mod constants;
mod message;
mod packet_pool;
pub mod raw;
mod rtte;
mod seq_nr;
mod socket;
mod stream;
mod stream_tx;

#[cfg(test)]
mod test_util;
mod traits;
mod utils;
mod write_canary_file;

pub use socket::{UtpSocket, UtpSocketUdp};
pub use stream::{UtpStream, UtpStreamUdp};
pub use traits::Transport;
