use std::{
    f64,
    time::{Duration, Instant},
};

use crate::rtte::RttEstimator;

use super::CongestionController;

// Constants for the Cubic congestion control algorithm.
// See RFC 8312.
const BETA_CUBIC: f64 = 0.7;
const C: f64 = 0.4;

#[derive(Clone, Copy)]
pub struct Cubic {
    // All wnd units are in mss, as per CUBIC
    cwnd: f64,
    ssthresh: f64,

    k: f64, // CUBIC: time required to get to w_max
    w_max: f64,

    mss: usize,
    last_congestion_event: Instant,

    // Remote window. Limits the window size
    rwnd: f64,
}

impl core::fmt::Debug for Cubic {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "cwnd={},sshthresh:{}",
            self.window(),
            (self.ssthresh * self.mss as f64) as usize
        )
    }
}

impl PartialEq for Cubic {
    fn eq(&self, other: &Self) -> bool {
        self.cwnd == other.cwnd && self.ssthresh == other.ssthresh
    }
}

impl Cubic {
    pub fn new(now: Instant, mss: usize) -> Cubic {
        Cubic {
            cwnd: 2.,
            ssthresh: f64::INFINITY,

            k: 0.,
            w_max: 0.,
            last_congestion_event: now,

            rwnd: 0.,
            mss,
        }
    }
}

impl CongestionController for Cubic {
    fn window(&self) -> usize {
        (self.cwnd * self.mss as f64) as usize
    }

    fn on_rto_timeout(&mut self, _now: Instant) {
        // CUBIC https://datatracker.ietf.org/doc/html/rfc8312#section-4.7
        self.ssthresh = (self.cwnd * BETA_CUBIC).max(2.);
        self.cwnd = 1.
    }

    fn on_triple_duplicate_ack(&mut self, now: Instant) {
        self.w_max = self.cwnd;
        self.k = calc_k(self.w_max);
        self.ssthresh = (self.cwnd * BETA_CUBIC).max(2.);
        self.cwnd *= BETA_CUBIC;
        self.last_congestion_event = now;
    }

    fn on_ack(&mut self, now: Instant, len: usize, rtte: &RttEstimator) {
        if len == 0 {
            return;
        }

        if self.cwnd < self.ssthresh {
            // Slow start
            self.cwnd += len as f64 / self.mss as f64;
        } else {
            // During congestion avoidance, window is computed using CUBIC. By the time we get here,
            //
            let t = now - self.last_congestion_event;
            let w_cubic_v = w_cubic(t, self.k, self.w_max);
            let rtt = rtte.roundtrip_time_estimate();
            let w_est_v = w_est(t, rtt, self.w_max);

            if w_cubic_v < w_est_v {
                // TCP friendly region
                self.cwnd = w_est_v;
            } else {
                // Concave and convex regions
                self.cwnd += (w_cubic(t + rtt, self.k, self.w_max) - self.cwnd) / self.cwnd;
            }
        }

        self.cwnd = self.cwnd.min(self.rwnd)
    }

    fn set_remote_window(&mut self, win: usize) {
        self.rwnd = win as f64 / self.mss as f64
    }
}

// K is the number of seconds required to get back to w_max.
fn calc_k(w_max_in_mss_units: f64) -> f64 {
    const FACTOR: f64 = (1. - BETA_CUBIC) / C;
    (w_max_in_mss_units * FACTOR).cbrt()
}

fn w_cubic(t: Duration, k: f64, w_max_in_mss_units: f64) -> f64 {
    C * (t.as_secs_f64() - k).powf(3.) + w_max_in_mss_units
}

fn w_est(t: Duration, rtt: Duration, w_max_in_mss_units: f64) -> f64 {
    // [3*(1-beta_cubic)/(1+beta_cubic)]
    const FACTOR: f64 = 3. * (1. - BETA_CUBIC) / (1. + BETA_CUBIC); // 0.52
    w_max_in_mss_units * BETA_CUBIC + FACTOR * (t.as_secs_f64() / rtt.as_secs_f64())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use approx::assert_abs_diff_eq;

    use crate::congestion::cubic::{calc_k, w_cubic, BETA_CUBIC};

    #[test]
    fn test_w_cubic_zero() {
        for w_max_mss in 0usize..=1000 {
            let w_max_mss = w_max_mss as f64;
            let k = calc_k(w_max_mss);
            assert_abs_diff_eq!(
                w_cubic(Duration::from_secs(0), k, w_max_mss),
                w_max_mss * BETA_CUBIC,
                epsilon = 0.01f64
            );
        }
    }
}
