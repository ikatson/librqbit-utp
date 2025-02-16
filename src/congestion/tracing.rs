use std::time::Instant;

use crate::{constants::CONGESTION_TRACING_LOG_LEVEL, rtte::RttEstimator};

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

    fn on_ack(&mut self, now: Instant, len: usize, rtt: &RttEstimator) {
        log_every_ms_if_changed!(
            500,
            CONGESTION_TRACING_LOG_LEVEL,
            "on_ack",
            self,
            |s| s.inner,
            |s| s.inner.on_ack(now, len, rtt)
        );
    }

    fn on_rto_timeout(&mut self, now: Instant) {
        log_if_changed!(
            CONGESTION_TRACING_LOG_LEVEL,
            "on_rto_timeout",
            self,
            |s| s.inner,
            |s| s.inner.on_rto_timeout(now)
        );
    }

    fn on_enter_fast_retransmit(&mut self, now: Instant) {
        log_if_changed!(
            CONGESTION_TRACING_LOG_LEVEL,
            "on_enter_fast_retransmit",
            self,
            |s| s.inner,
            |s| s.inner.on_enter_fast_retransmit(now)
        );
    }

    fn set_remote_window(&mut self, win: usize) {
        self.inner.set_remote_window(win);
    }

    fn sshthresh(&self) -> usize {
        self.inner.sshthresh()
    }

    fn on_recovered(&mut self, recovery_cwnd_bytes: usize) {
        log_if_changed!(
            CONGESTION_TRACING_LOG_LEVEL,
            "on_recovered",
            self,
            |s| s.inner,
            |s| s.inner.on_recovered(recovery_cwnd_bytes)
        );
    }

    fn smss(&self) -> usize {
        self.inner.smss()
    }
}
