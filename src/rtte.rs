use std::time::{Duration, Instant};

use crate::seq_nr::SeqNr;

// rfc6298: 2.1. Until a round-trip time (RTT) measurement has been made for a
// segment sent between the sender and receiver, the sender SHOULD
// set RTO <- 1 second
const RTTE_INITIAL_RTT: Duration = Duration::from_millis(300);

// rfc6298 2.4
const RTTE_MIN_RTO: Duration = Duration::from_secs(1);
// rfc6298 2.4
const RTTE_MAX_RTO: Duration = Duration::from_secs(60);

fn clamp(rto: Duration) -> Duration {
    rto.clamp(RTTE_MIN_RTO, RTTE_MAX_RTO)
}

const K: u32 = 4;

#[derive(Debug, Clone, Copy)]
enum RttState {
    Initial {
        rto: Duration,
    },
    Subsequent {
        rto: Duration,
        srtt: Duration,
        rttvar: Duration,
    },
}

#[derive(Debug, Clone, Copy)]
pub struct RttEstimator {
    state: RttState,

    #[cfg(test)]
    forced_timeout: Option<Duration>,
}

impl Default for RttEstimator {
    fn default() -> Self {
        Self {
            state: RttState::Initial {
                rto: RTTE_INITIAL_RTT,
            },

            #[cfg(test)]
            forced_timeout: None,
        }
    }
}

impl RttEstimator {
    #[cfg(test)]
    pub fn force_timeout(&mut self, duration: Duration) {
        self.forced_timeout = Some(duration);
    }

    pub fn retransmission_timeout(&self) -> Duration {
        #[cfg(test)]
        if let Some(t) = self.forced_timeout {
            return t;
        }

        match &self.state {
            RttState::Initial { rto } => *rto,
            RttState::Subsequent { rto, .. } => *rto,
        }
    }

    fn sample(&mut self, new_rtt: Duration) {
        match &mut self.state {
            RttState::Initial { .. } => {
                let srtt = new_rtt;
                let rttvar = new_rtt / 2;

                let rto = clamp(srtt + rttvar * K); // SRTT + max (G, K*RTTVAR);
                self.state = RttState::Subsequent { rto, srtt, rttvar }
            }
            RttState::Subsequent { rto, srtt, rttvar } => {
                // RTTVAR <- (1 - beta) * RTTVAR + beta * |SRTT - R'| = rttvar * 3 / 4 + (srtt - new_rtt).abs() / 4
                // SRTT <- (1 - alpha) * SRTT + alpha * R' = srtt * 7 / 8 + new_rtt / 8 = (srtt * 7 + new_rtt) / 8
                // alpha = 1/8
                // beta = 1/4

                *rttvar = *rttvar * 3 / 4 + srtt.abs_diff(new_rtt) / 4;
                *srtt = (*srtt * 7 + new_rtt) / 8;
                *rto = clamp(*srtt + K * *rttvar);
            }
        }
    }

    pub fn on_send(&mut self, _timestamp: Instant, _seq: SeqNr) {
        // Nothing, we compute that in segements
    }

    pub fn on_ack(&mut self, rtt: Duration) {
        self.sample(rtt);
    }

    pub fn on_retransmit(&mut self) {
        // rfc6298 section 5
        match &mut self.state {
            RttState::Initial { rto } => {
                *rto = clamp(*rto * 2);
            }
            RttState::Subsequent { rto, .. } => {
                *rto = clamp(*rto * 2);
            }
        };
    }
}
