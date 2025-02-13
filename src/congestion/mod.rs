pub mod cubic;
pub mod tracing;

use std::time::Instant;

use crate::rtte::RttEstimator;

#[allow(unused_variables)]
pub trait CongestionController: Send + Sync + core::fmt::Debug {
    /// Returns the number of bytes that can be sent.
    fn window(&self) -> usize;

    /// Set the remote window size.
    fn set_remote_window(&mut self, remote_window: usize);

    fn on_ack(&mut self, now: Instant, len: usize, rtt: &RttEstimator);

    fn on_retransmit(&mut self, now: Instant);

    fn on_duplicate_ack(&mut self, now: Instant);

    fn pre_transmit(&mut self, now: Instant);

    // fn post_transmit(&mut self, now: Instant, len: usize) {}

    /// Set the maximum segment size.
    fn set_mss(&mut self, mss: usize);
}
