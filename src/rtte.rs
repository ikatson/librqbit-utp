use std::time::{Duration, Instant};

use tracing::trace;

// Conservative initial RTT estimate.
const RTTE_INITIAL_RTT: u32 = 300;
const RTTE_INITIAL_DEV: u32 = 100;

// Minimum "safety margin" for the RTO that kicks in when the
// variance gets very low.
const RTTE_MIN_MARGIN: u32 = 5;

const RTTE_MIN_RTO: u32 = 10;
const RTTE_MAX_RTO: u32 = 10000;

type UtpSeqNumber = u16;

#[derive(Debug, Clone, Copy)]
pub struct RttEstimator {
    // Using u32 instead of Duration to save space (Duration is i64)
    rtt: u32,
    deviation: u32,
    timestamp: Option<(Instant, UtpSeqNumber)>,
    max_seq_sent: Option<UtpSeqNumber>,
    rto_count: u8,
}

impl Default for RttEstimator {
    fn default() -> Self {
        Self {
            rtt: RTTE_INITIAL_RTT,
            deviation: RTTE_INITIAL_DEV,
            timestamp: None,
            max_seq_sent: None,
            rto_count: 0,
        }
    }
}

impl RttEstimator {
    pub fn retransmission_timeout(&self) -> Duration {
        let margin = RTTE_MIN_MARGIN.max(self.deviation * 4);
        let ms = (self.rtt + margin).clamp(RTTE_MIN_RTO, RTTE_MAX_RTO);
        Duration::from_millis(ms as u64)
    }

    fn sample(&mut self, new_rtt: u32) {
        // "Congestion Avoidance and Control", Van Jacobson, Michael J. Karels, 1988
        self.rtt = (self.rtt * 7 + new_rtt + 7) / 8;
        let diff = (self.rtt as i32 - new_rtt as i32).unsigned_abs();
        self.deviation = (self.deviation * 3 + diff + 3) / 4;

        self.rto_count = 0;

        let rto = self.retransmission_timeout().as_millis();
        trace!(
            "rtte: sample={:?} rtt={:?} dev={:?} rto={:?}",
            new_rtt,
            self.rtt,
            self.deviation,
            rto
        );
    }

    pub fn on_send(&mut self, timestamp: Instant, seq: UtpSeqNumber) {
        if self
            .max_seq_sent
            .map(|max_seq_sent| seq > max_seq_sent)
            .unwrap_or(true)
        {
            self.max_seq_sent = Some(seq);
            if self.timestamp.is_none() {
                self.timestamp = Some((timestamp, seq));
                trace!("rtte: sampling at seq={:?}", seq);
            }
        }
    }

    pub fn on_ack(&mut self, timestamp: Instant, seq: UtpSeqNumber) {
        if let Some((sent_timestamp, sent_seq)) = self.timestamp {
            if seq >= sent_seq {
                self.sample((timestamp - sent_timestamp).as_millis() as u32);
                self.timestamp = None;
            }
        }
    }

    pub fn on_retransmit(&mut self) {
        if self.timestamp.is_some() {
            trace!("rtte: abort sampling due to retransmit");
        }
        self.timestamp = None;
        self.rto_count = self.rto_count.saturating_add(1);
        if self.rto_count >= 3 {
            // This happens in 2 scenarios:
            // - The RTT is higher than the initial estimate
            // - The network conditions change, suddenly making the RTT much higher
            // In these cases, the estimator can get stuck, because it can't sample because
            // all packets sent would incur a retransmit. To avoid this, force an estimate
            // increase if we see 3 consecutive retransmissions without any successful sample.
            self.rto_count = 0;
            self.rtt = RTTE_MAX_RTO.min(self.rtt * 2);
            let rto = self.retransmission_timeout().as_millis();
            trace!(
                "rtte: too many retransmissions, increasing: rtt={:?} dev={:?} rto={:?}",
                self.rtt,
                self.deviation,
                rto
            );
        }
    }
}
