// TODO: in case the other end doesn't answer we need to stop the dispatcher. Otherwise it'll leak memory.
// currently, only if we send the whole tx queue we check if it's dead or not.

// RTT, RTO, congestion
// Recent RFCs that we might try to implement exactly to make everyting super explicit
// RFC 9293 https://datatracker.ietf.org/doc/html/rfc9293 (TCP) (2022, standard)
// RFC 6298 https://datatracker.ietf.org/doc/html/rfc6298 (2011, draft) - RTO calculation, references RTT calculation
// RFC 5681 https://datatracker.ietf.org/doc/html/rfc5681 (2009, draft) - congestion control, fast retransmit, fast recovery
//
// rfc8985 (draft, 2021) - RACK algorithm

// (partly done) TODO: I don't like the timer logic - it's too complex and thus probably buggy. Rewrite it.

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
// TODO: built-in connection timeouts?

#[macro_use]
mod macros;

mod congestion;
mod constants;
mod message;
pub mod raw;
mod rtte;
mod seq_nr;
mod socket;
mod spawn_utils;
mod stream;
mod stream_dispatch;
mod stream_rx;
mod stream_tx;
mod stream_tx_segments;
#[cfg(test)]
mod test_util;
mod traits;
mod utils;

pub use socket::{CongestionConfig, CongestionControllerKind, SocketOpts, UtpSocket, UtpSocketUdp};
pub use stream::{UtpStream, UtpStreamUdp};
pub use traits::Transport;

type Payload = Vec<u8>;
