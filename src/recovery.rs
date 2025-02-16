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

    pub total_retransmitted_segments: usize,

    // During recovery we manage cwnd, not congestion controller.
    pub cwnd: usize,

    restore_cwnd: usize,
    restore_sshthresh: usize,
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
                    debug!(?recovery_point, ?header.ack_nr, "exiting IgnoringUntilRecoveryPoint");
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
                    ?congestion_controller,
                    "counting duplicate acks"
                );

                if !should_enter_recovery {
                    return;
                }

                let restore_cwnd = congestion_controller.window();
                let restore_sshthresh = congestion_controller.sshthresh();

                congestion_controller.on_enter_fast_retransmit(now);

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

                    cwnd: congestion_controller.sshthresh(),
                    restore_cwnd,
                    restore_sshthresh,
                };

                debug!(?rec.recovery_point, ?retransmit_tokens, ?congestion_controller, "entered recovery");
                *self = Recovery::Recovery(rec);
            }
            Recovery::Recovery(rec) => {
                // Heuristic. If there's still selective ACK data, DO NOT exit recovery.
                if header.ack_nr >= rec.recovery_point && header.extensions.selective_ack.is_some()
                {
                    debug!("moving recovery point further, still not fully recovered");
                    rec.recovery_point = last_sent_seq_nr;
                }

                if header.ack_nr >= rec.recovery_point {
                    if rec.total_retransmitted_segments > 0 {
                        // Recovery actually did something
                        // let mss = congestion_controller.smss();
                        // let cwnd = congestion_controller
                        //     .sshthresh()
                        //     .min(tx_segs.calc_flight_size().max(mss) + mss);
                        let sshthresh = rec.cwnd;
                        // congestion_controller.on_recovered(cwnd, sshthresh);
                        congestion_controller.on_recovered(sshthresh, sshthresh);
                    } else {
                        debug!("recovery was not necessary, restoring previous values");
                        // The ACKs repaired themselves without us doing anything. Restore previous values.
                        congestion_controller.on_recovered(rec.restore_cwnd, rec.restore_sshthresh);
                    }

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
