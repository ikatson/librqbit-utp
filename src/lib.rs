// RTT, RTO, congestion
// Recent RFCs that we might try to implement exactly to make everyting super explicit
// RFC 9293 https://datatracker.ietf.org/doc/html/rfc9293 (TCP) (2022, standard)
// RFC 6298 https://datatracker.ietf.org/doc/html/rfc6298 (2011, draft) - RTO calculation, references RTT calculation
// RFC 5681 https://datatracker.ietf.org/doc/html/rfc5681 (2009, draft) - congestion control, fast retransmit, fast recovery
//
// rfc8985 (draft, 2021) - RACK algorithm

// TODO: LEDBAT congestion control

#[macro_use]
mod macros;

mod congestion;
mod constants;
#[cfg(test)]
mod e2e_tests;
mod message;
mod metrics;
pub mod mtu;
pub mod raw;
mod recovery;
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
pub use stream::UtpStream;
pub use traits::Transport;

type Payload = Vec<u8>;
