use std::time::Duration;

// rfc6298: 2.1. Until a round-trip time (RTT) measurement has been made for a
// segment sent between the sender and receiver, the sender SHOULD
// set RTO <- 1 second
const RTTE_INITIAL_RTT: Duration = Duration::from_millis(300);

// rfc6298 2.4 says to set min 1s. However looks like modern OSes override
// this and keep it lower. Linux has 200ms. uTP BEP 29 is 500ms.
const RTTE_MIN_RTO: Duration = Duration::from_millis(200);
// rfc6298 2.4
const RTTE_MAX_RTO: Duration = Duration::from_secs(60);

// rfc6298 section 4
const CLOCK_GRANULARITY: Duration = Duration::from_millis(10);

fn clamp(rto: Duration) -> Duration {
    rto.clamp(RTTE_MIN_RTO, RTTE_MAX_RTO)
}

fn calc_rto(srtt: Duration, rttvar: Duration) -> Duration {
    clamp(srtt + (rttvar * K).max(CLOCK_GRANULARITY))
}

const K: u32 = 4;

// TODO: this was stabilized in Rust 1.81. Remove some time later.
fn duration_abs_diff(dur: Duration, other: Duration) -> Duration {
    if let Some(res) = dur.checked_sub(other) {
        res
    } else {
        other.checked_sub(dur).unwrap()
    }
}

#[derive(Clone, Copy)]
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

#[derive(Clone, Copy)]
pub struct RttEstimator {
    state: RttState,

    #[cfg(test)]
    forced_timeout: Option<Duration>,
}

impl std::fmt::Debug for RttEstimator {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "rtt:{:?},rto:{:?}",
            self.roundtrip_time(),
            self.retransmission_timeout()
        )
    }
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

impl PartialEq for RttEstimator {
    fn eq(&self, other: &Self) -> bool {
        (self.roundtrip_time(), self.retransmission_timeout())
            == (other.roundtrip_time(), other.retransmission_timeout())
    }
}

impl RttEstimator {
    #[cfg(test)]
    pub fn force_timeout(&mut self, duration: Duration) {
        self.forced_timeout = Some(duration);
    }

    pub fn roundtrip_time(&self) -> Duration {
        match self.state {
            RttState::Initial { rto } => rto,
            RttState::Subsequent { srtt, .. } => srtt,
        }
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

    pub fn sample(&mut self, new_rtt: Duration) {
        match &mut self.state {
            RttState::Initial { .. } => {
                let srtt = new_rtt;
                let rttvar = new_rtt / 2;

                let rto = calc_rto(srtt, rttvar);
                self.state = RttState::Subsequent { rto, srtt, rttvar }
            }
            RttState::Subsequent { rto, srtt, rttvar } => {
                // RTTVAR <- (1 - beta) * RTTVAR + beta * |SRTT - R'| = rttvar * 3 / 4 + (srtt - new_rtt).abs() / 4
                // SRTT <- (1 - alpha) * SRTT + alpha * R' = srtt * 7 / 8 + new_rtt / 8 = (srtt * 7 + new_rtt) / 8
                // alpha = 1/8
                // beta = 1/4

                *rttvar = *rttvar * 3 / 4 + duration_abs_diff(*srtt, new_rtt) / 4;
                *srtt = (*srtt * 7 + new_rtt) / 8;
                *rto = calc_rto(*srtt, *rttvar);
            }
        }
    }

    pub fn on_rto_timeout(&mut self) {
        // rfc6298 section 5.5
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
