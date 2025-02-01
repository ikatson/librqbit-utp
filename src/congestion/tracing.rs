use std::time::Instant;

use tracing::{trace, Level};

use crate::{rtte::RttEstimator, utils::log_before_and_after_if_changed};

use super::CongestionController;

#[derive(Debug)]
pub struct TracingController<I> {
    inner: I,
}

impl<I> TracingController<I> {
    pub fn new(inner: I) -> Self {
        Self { inner }
    }
}

impl<I> CongestionController for TracingController<I>
where
    I: CongestionController + PartialEq + Copy + 'static,
{
    fn window(&self) -> usize {
        self.inner.window()
    }

    fn set_remote_window(&mut self, remote_window: usize) {
        trace!(remote_window, "set_remote_window");
        self.inner.set_remote_window(remote_window);
    }

    fn on_ack(&mut self, now: Instant, len: usize, rtt: &RttEstimator) {
        log_before_and_after_if_changed(
            "on_ack:cwnd",
            self,
            |s| s.inner,
            |s| s.inner.on_ack(now, len, rtt),
            |_, _| Level::TRACE,
        )
    }

    fn on_retransmit(&mut self, now: Instant) {
        log_before_and_after_if_changed(
            "on_retransmit:cwnd",
            self,
            |s| s.inner,
            |s| s.inner.on_retransmit(now),
            |_, _| Level::TRACE,
        )
    }

    fn on_duplicate_ack(&mut self, now: Instant) {
        log_before_and_after_if_changed(
            "on_duplicate_ack:cwnd",
            self,
            |s| s.inner,
            |s| s.inner.on_duplicate_ack(now),
            |_, _| Level::TRACE,
        )
    }

    fn pre_transmit(&mut self, now: Instant) {
        log_before_and_after_if_changed(
            "pre_transmit:cwnd",
            self,
            |s| s.inner,
            |s| s.inner.pre_transmit(now),
            |_, _| Level::TRACE,
        )
    }

    fn set_mss(&mut self, mss: usize) {
        self.inner.set_mss(mss);
    }
}
