/// Implements RFC 6582 "The NewReno Modification to TCP's Fast Recovery Algorithm"
/// Uses elements of RFC 6675 "SACK Loss Recovery Algorithm for TCP" for duplicate counting.
use std::time::Instant;

use tracing::debug;

use crate::{
    congestion::CongestionController,
    constants::SACK_DUP_THRESH,
    metrics::METRICS,
    raw::UtpHeader,
    seq_nr::SeqNr,
    stream_tx_segments::{OnAckResult, Segments},
};

#[derive(Debug, Clone, Copy)]
pub struct RecoveryPhase {
    pub recovery_point: SeqNr,
    pub high_rxt: SeqNr,
    pub retransmit_tokens: usize,

    pub total_retransmitted_segments: usize,

    // During recovery we manage cwnd, not congestion controller.
    // After recovery is done, this will be used to configure
    // congestion controller.
    pub cwnd: usize,
}

impl RecoveryPhase {}

#[derive(Debug, Clone, Copy)]
pub enum Recovery {
    IgnoringUntilRecoveryPoint { recovery_point: SeqNr },
    CountingDuplicates { dup_acks: u8 },
    Recovering(RecoveryPhase),
}

impl Recovery {
    pub fn recovery_cwnd(&self) -> Option<usize> {
        match self {
            Recovery::Recovering(rec) => Some(rec.cwnd),
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
                    debug!(?recovery_point, ?header.ack_nr, "exiting IgnoringUntilRecoveryPoint");
                    *self = Recovery::CountingDuplicates { dup_acks: 0 }
                }
            }
            Recovery::CountingDuplicates { dup_acks } => {
                // HighAck is the last sequence number fully consumed by receiver.
                let high_ack = match tx_segs.first_seq_nr() {
                    Some(s) => s - 1,
                    None => {
                        // The queue is empty, don't count ACKs.
                        *dup_acks = 0;
                        return;
                    }
                };
                if header.ack_nr <= high_ack {
                    // Ignore the useless ACK.
                    return;
                }

                // This is pretty strong, for this heuristic to work the receiver MUST support SACK.
                // We can probably just assume that. If it doesn't work, we'll need to track if we have
                // ever seen a SACK before at least once.
                if !on_ack_result.is_duplicate() || header.extensions.selective_ack.is_none() {
                    *dup_acks = 0;
                    return;
                }

                METRICS.duplicate_acks_received.increment(1);
                *dup_acks += 1;

                let should_enter_recovery = *dup_acks as usize >= SACK_DUP_THRESH;

                debug!(
                    should_enter_recovery,
                    dup_acks,
                    ?high_ack,
                    high_data = ?last_sent_seq_nr,
                    ?congestion_controller,
                    "counting duplicate acks"
                );

                if !should_enter_recovery {
                    return;
                }

                congestion_controller.on_enter_fast_retransmit(now);

                // This is a simplified version of rfc6675. How many burst packets we are allowed to retransmit.
                // It must be at least 1 for fast retransmit (as per rfc5681) to trigger.
                let retransmit_tokens = header
                    .extensions
                    .selective_ack
                    .as_ref()
                    .map(|sack| sack.as_bitslice().count_ones())
                    .unwrap_or(0)
                    .max(1);

                let rec = RecoveryPhase {
                    recovery_point: last_sent_seq_nr,

                    high_rxt: high_ack,
                    retransmit_tokens,

                    total_retransmitted_segments: 0,

                    // This is already reduced sshthresh.
                    cwnd: congestion_controller.sshthresh(),
                };

                debug!(?rec.recovery_point, ?retransmit_tokens, ?congestion_controller, "entered recovery");
                *self = Recovery::Recovering(rec);
            }
            Recovery::Recovering(rec) => {
                if header.ack_nr >= rec.recovery_point {
                    // From rfc6582 NewReno "Full Acknowledgements" section.
                    // On recover we set cwnd very conservatively not to cause a sudden burst of traffic.
                    // It will very quickly reach sshthresh.
                    let mss = congestion_controller.smss();
                    let cwnd = congestion_controller
                        .sshthresh()
                        .min(tx_segs.calc_flight_size().max(mss) + mss);
                    let sshthresh = rec.cwnd;
                    congestion_controller.on_recovered(cwnd, sshthresh);

                    debug!(?rec.recovery_point, ?header.ack_nr, prev_cwnd=rec.cwnd, ?congestion_controller, "exited recovery");
                    *self = Recovery::CountingDuplicates { dup_acks: 0 };
                    return;
                }

                rec.retransmit_tokens += on_ack_result.total_acked_segments();
            }
        }
    }

    pub(crate) fn on_rto_timeout(&mut self, last_sent_seq_nr: SeqNr) {
        debug!(
            recovery_point = ?last_sent_seq_nr,
            "on_rto_timeout: ignoring duplicate acks until"
        );
        *self = Recovery::IgnoringUntilRecoveryPoint {
            recovery_point: last_sent_seq_nr,
        }
    }
}
