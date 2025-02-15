pub mod cubic;
pub mod tracing;

use std::time::Instant;

use crate::rtte::RttEstimator;

#[allow(unused_variables)]
pub trait CongestionController: Send + Sync + core::fmt::Debug {
    /// Returns the number of bytes that can be sent.
    fn window(&self) -> usize;

    /// Increase the window on ACK
    fn on_ack(&mut self, now: Instant, len: usize, rtt: &RttEstimator);

    // NOT fast retransmit
    // flight_size per rfc5681
    fn on_rto_timeout(&mut self, now: Instant);

    fn on_congestion_event(&mut self, now: Instant);

    fn set_remote_window(&mut self, win: usize);
}
