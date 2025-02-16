/// Implements RFC 6582 "The NewReno Modification to TCP's Fast Recovery Algorithm"
/// Uses elements of RFC 6675 "SACK Loss Recovery Algorithm for TCP" for duplicate counting.
use std::time::Instant;

use tracing::{debug, trace};

use crate::{
    congestion::CongestionController,
    constants::SACK_DUP_THRESH,
    metrics::METRICS,
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
    stream_tx_segments::{OnAckResult, Segments},
};

#[derive(Debug, Clone, Copy)]
pub struct Recovering {
    recovery_point: SeqNr,

    // The packet sender may modify these so they are pub.
    pub high_rxt: SeqNr,
    pub retransmit_tokens: usize,

    // During recovery we manage cwnd, not congestion controller.
    // After recovery is done, this will be used to configure
    // congestion controller.
    cwnd: usize,
}

impl Recovering {
    pub fn recovery_point(&self) -> SeqNr {
        self.recovery_point
    }
}

impl Recovering {}

#[derive(Debug, Clone, Copy)]
enum RecoveryPhase {
    IgnoringUntilRecoveryPoint { recovery_point: SeqNr },
    CountingDuplicates { dup_acks: u8 },
    Recovering(Recovering),
}

#[derive(Debug, Clone, Copy)]
pub struct Recovery {
    receiver_supports_sack: bool,

    // Tracks if an ACK is a window update before it switches to SACK mode.
    last_window: Option<u32>,

    phase: RecoveryPhase,
}

impl Default for Recovery {
    fn default() -> Self {
        Self {
            receiver_supports_sack: false,
            last_window: None,
            phase: RecoveryPhase::CountingDuplicates { dup_acks: 0 },
        }
    }
}

impl Recovery {
    pub fn recovery_cwnd(&self) -> Option<usize> {
        match &self.phase {
            RecoveryPhase::Recovering(rec) => Some(rec.cwnd),
            _ => None,
        }
    }

    pub fn recovering_mut(&mut self) -> Option<&mut Recovering> {
        match &mut self.phase {
            RecoveryPhase::Recovering(rec) => Some(rec),
            _ => None,
        }
    }

    fn count_sack_duplicates(
        header: &UtpHeader,
        _on_ack_result: &OnAckResult,
        prev_dup_acks: u8,
    ) -> u8 {
        match &header.extensions.selective_ack {
            Some(sack) => {
                // Per rfc6675, if we received at least 3 segments above high_ack, we need to enter recovery
                // right away.
                let segments_past_first = sack.as_bitslice().count_ones();
                if segments_past_first >= SACK_DUP_THRESH as usize {
                    debug!(
                        prev_dup_acks,
                        segments_past_first,
                        ?header.ack_nr,
                        "too many segments past first ACKed"
                    );
                    METRICS.duplicate_acks_received.increment(1);
                    return SACK_DUP_THRESH;
                }

                // Per rfc6675, ONLY if the SACK carries new data we +1 the counter. I.e.
                // "a segment that arrives carrying a SACK block that
                // identifies previously unacknowledged and un-SACKed octets between
                // HighACK and HighData".
                //
                // However, uTP cannot convey any information past the SACK size limit
                // (usually 64 segments). So we assume it is trying to do that here, and
                // ignore actually looking at how many segments were ACKed.
                METRICS.duplicate_acks_received.increment(1);
                prev_dup_acks + 1
            }
            None => {
                // If the incoming ACK is a cumulative acknowledgment, the TCP MUST
                // reset DupAcks to zero.
                0
            }
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
        self.receiver_supports_sack |= header.extensions.selective_ack.is_some();

        match &mut self.phase {
            RecoveryPhase::IgnoringUntilRecoveryPoint { recovery_point } => {
                if header.ack_nr >= *recovery_point {
                    debug!(?recovery_point, ?header.ack_nr, "exiting IgnoringUntilRecoveryPoint");
                    self.phase = RecoveryPhase::CountingDuplicates { dup_acks: 0 }
                }
            }
            RecoveryPhase::CountingDuplicates { dup_acks } => {
                // HighAck is the last sequence number fully consumed by receiver.
                let high_ack = match tx_segs.first_seq_nr() {
                    Some(s) => s - 1,
                    None => {
                        // The queue is empty, don't count ACKs.
                        *dup_acks = 0;
                        return;
                    }
                };

                if self.receiver_supports_sack {
                    *dup_acks = Self::count_sack_duplicates(header, on_ack_result, *dup_acks);
                } else {
                    // Resort to simple duplicate ACK counting per rfc5681.
                    if header.htype != Type::ST_STATE {
                        return;
                    }
                    let is_window_update = self.last_window != Some(header.wnd_size);
                    self.last_window = Some(header.wnd_size);

                    if is_window_update {
                        return;
                    }

                    if header.ack_nr != high_ack {
                        return;
                    }

                    METRICS.duplicate_acks_received.increment(1);
                    *dup_acks += 1;

                    trace!(?header.ack_nr, ?high_ack, dup_acks, "counted duplicate ACK in non-sack mode");
                }

                let should_enter_recovery = *dup_acks >= SACK_DUP_THRESH;
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

                let rec = Recovering {
                    recovery_point: last_sent_seq_nr,

                    high_rxt: high_ack,
                    retransmit_tokens,

                    // This is already reduced sshthresh.
                    cwnd: congestion_controller.sshthresh(),
                };

                debug!(?rec.recovery_point, ?retransmit_tokens, ?high_ack, ack_nr=?header.ack_nr, ?congestion_controller, "entered recovery");
                self.phase = RecoveryPhase::Recovering(rec);
            }
            RecoveryPhase::Recovering(rec) => {
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
                    self.phase = RecoveryPhase::CountingDuplicates { dup_acks: 0 };
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
        self.phase = RecoveryPhase::IgnoringUntilRecoveryPoint {
            recovery_point: last_sent_seq_nr,
        }
    }
}
