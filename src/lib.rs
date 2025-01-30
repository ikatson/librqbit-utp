// TODO: uTP socket death
// if main socket dies
// - scream about it
// - test: error all subsequent accept() and connect() attempts
// - test: drop all outstanding accepts() and connects()

// TODO: flow control
// - test it
//   - ensure the user gets all neded messages even if it was blocked for a while
// - test: ensure the user gets a fin
//
// TODO: Initial SYN - keep resending it with increasing delay. This would probably require refactoring SYN
// into the VirtualSocket state machine (like smoltcp), cause otherwise it'll get nasty.
// Although we could just send it in a loop with connect() like every second or so no problem;
// With jitter though! Not to flood all at the same time.
//
// TODO: LEDBAT congestion control
//
// TODO: extensions
// - selective ACKs on sending side
//
// TODO: memory management. Think about a data structure that will let the UDP incoming packet
// get written straight out to use Rx buffer.//
//
// TODO: built-in connection timeouts?

mod congestion;
mod constants;
mod message;
mod out_of_order_rx;
pub mod raw;
mod rtte;
mod seq_nr;
mod socket;
mod stream_dispatch;
mod stream_tx;

mod smoltcp_assembler;
mod stream;
mod stream_rx;
#[cfg(test)]
mod test_util;
mod traits;
mod utils;

pub use socket::{CongestionConfig, CongestionControllerKind, SocketOpts, UtpSocket, UtpSocketUdp};
pub use stream::{UtpStream, UtpStreamUdp};
pub use traits::Transport;
