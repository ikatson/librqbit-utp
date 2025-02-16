/// Implements RFC 6675 "SACK Loss Recovery Algorithm for TCP"
/// Some other parts are in user_tx_segments and stream_dispatch
use std::time::Instant;

use tracing::{debug, trace, warn};

use crate::{
    congestion::CongestionController,
    constants::{SACK_DEPTH, SACK_DUP_THRESH},
    raw::UtpHeader,
    seq_nr::SeqNr,
    stream_tx_segments::{OnAckResult, Segment, SegmentIterItem, Segments},
};

#[derive(Debug, Clone, Copy)]
pub struct RecoveryPhase {
    pub recovery_point: SeqNr,
    pub high_rxt: SeqNr,
    pub retransmit_tokens: usize,

    // During recovery we manage cwnd, not congestion controller.
    pub cwnd: usize,
}

impl RecoveryPhase {}

#[derive(Debug, Clone, Copy)]
pub enum Recovery {
    IgnoringUntilRecoveryPoint { recovery_point: SeqNr },
    CountingDuplicates { dup_acks: u8 },
    Recovery(RecoveryPhase),
}

impl Recovery {
    pub fn is_recovery(&self) -> bool {
        matches!(self, Recovery::Recovery { .. })
    }

    pub fn recovery_cwnd(&self) -> Option<usize> {
        match self {
            Recovery::Recovery(rec) => Some(rec.cwnd),
            _ => None,
        }
    }

    pub fn on_ack(
        &mut self,
        header: &UtpHeader,
        on_ack_result: &OnAckResult,
        tx_segs: &mut Segments,
        last_sent_seq_nr: SeqNr,
        congestion_controller: &mut dyn CongestionController,
        now: Instant,
    ) {
        match self {
            Recovery::IgnoringUntilRecoveryPoint { recovery_point } => {
                if header.ack_nr >= *recovery_point {
                    *self = Recovery::CountingDuplicates { dup_acks: 0 }
                }
            }
            Recovery::CountingDuplicates { dup_acks } => {
                if !on_ack_result.is_duplicate() {
                    *dup_acks = 0;
                    return;
                }

                *dup_acks += 1;

                let high_ack = match tx_segs.first_seq_nr() {
                    Some(s) => s - 1,
                    None => {
                        // Everything got ACKed.
                        *dup_acks = 0;
                        return;
                    }
                };

                let should_enter_recovery = *dup_acks as usize >= SACK_DUP_THRESH;

                debug!(
                    should_enter_recovery,
                    dup_acks,
                    ?high_ack,
                    high_data = ?last_sent_seq_nr,
                    "counting duplicate acks"
                );

                if !should_enter_recovery {
                    return;
                }

                congestion_controller.on_enter_fast_retransmit(now);

                let rec = RecoveryPhase {
                    recovery_point: last_sent_seq_nr,

                    // These 2 will allow us to fast retransmit the first missing packet right away
                    high_rxt: high_ack,
                    retransmit_tokens: 1,

                    cwnd: congestion_controller.sshthresh(),
                };

                // TODO: we need to inform congestion controller
                // decrease sshthresh, maybe cwnd too?

                debug!(?rec.recovery_point, "entered recovery");
                *self = Recovery::Recovery(rec);
            }
            Recovery::Recovery(rec) => {
                if header.ack_nr >= rec.recovery_point {
                    // TODO: we need to inform congestion controller
                    // Set cwnd to either (1) to min (ssthresh, max(FlightSize, SMSS) + SMSS)
                    debug!(?rec.recovery_point, ?header.ack_nr, "exited recovery");
                    congestion_controller.on_recovered(rec.cwnd);
                    *self = Recovery::CountingDuplicates { dup_acks: 0 };
                    return;
                }

                // partial ACK
                // In this case,
                //        retransmit the first unacknowledged segment.  Deflate the
                //        congestion window by the amount of new data acknowledged by the
                //        Cumulative Acknowledgment field.

                let total_acked_bytes = on_ack_result.total_acked_bytes();

                if total_acked_bytes > 0 {
                    rec.cwnd = rec.cwnd.saturating_sub(total_acked_bytes);
                    if total_acked_bytes >= congestion_controller.smss() {
                        // This will let us send new data.
                        rec.cwnd += congestion_controller.smss();

                        // This will let us retransmit one more unacked segment.
                        rec.retransmit_tokens += 1;
                    }
                }
            }
        }
    }

    pub(crate) fn on_rto_timeout(&mut self, last_sent_seq_nr: SeqNr) {
        *self = Recovery::IgnoringUntilRecoveryPoint {
            recovery_point: last_sent_seq_nr,
        }
    }
}
