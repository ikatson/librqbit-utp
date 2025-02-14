/// Implements RFC 6675 "SACK Loss Recovery Algorithm for TCP"
/// Some other parts are in user_tx_segments and stream_dispatch
use std::time::Instant;

use tracing::debug;

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

    // "HighRxt" is the highest sequence number which has been
    // retransmitted during the current loss recovery phase.
    pub high_rxt: SeqNr,

    // "RescueRxt" is the highest sequence number which has been
    // optimistically retransmitted to prevent stalling of the ACK clock
    // when there is loss at the end of the window and no new data is
    // available for transmission.
    pub rescue_rxt_used: bool,

    // "Pipe" is a sender's estimate of the number of bytes outstanding
    // in the network.
    pub pipe: usize,
}

pub struct NextSeg<'a> {
    pub item: SegmentIterItem<&'a mut Segment>,
    pub is_rescue: bool,
}

impl RecoveryPhase {
    // The caller must set high_rxt after successfully sending.
    pub fn next_seg<'s>(&self, tx_segs: &'s mut Segments) -> anyhow::Result<Option<NextSeg<'s>>> {
        fn erase_lifetime<'o>(s: &mut Segments) -> &'o mut Segments {
            unsafe { &mut *{ s as *mut _ } }
        }

        // Rule "0" (i.e. section 4.3) - retransmit the first segment if not yet.
        if let Some(seg) = erase_lifetime(tx_segs)
            .iter_mut()
            .next()
            .filter(|s| s.seq_nr() > self.high_rxt)
        {
            if seg.is_delivered() {
                anyhow::bail!("RecoveryPhase::next_seg: first segment in Segments is delivered, this shouldn't happen!")
            }
            return Ok(Some(NextSeg {
                item: seg,
                is_rescue: false,
            }));
        }

        let rule_1a = |seg: &SegmentIterItem<&mut Segment>| seg.seq_nr() > self.high_rxt;
        let rule_1b = |seg: &SegmentIterItem<&mut Segment>| seg.segment().has_sacks_after_it;
        let rule_1c = |seg: &SegmentIterItem<&mut Segment>| seg.segment().is_lost;

        // Rule 1.
        //
        // Safety: this is just a hack to satisfy borrow checker not coping with early returns from loops.
        for seg in erase_lifetime(tx_segs)
            .iter_mut()
            .filter(|s| !s.is_delivered())
        {
            if rule_1a(&seg) && rule_1b(&seg) && rule_1c(&seg) {
                return Ok(Some(NextSeg {
                    item: seg,
                    is_rescue: false,
                }));
            }
        }

        // Rule 2.
        if let Some(seg) = erase_lifetime(tx_segs)
            .iter_mut()
            .filter(|s| !s.is_delivered())
            .find(|s| !s.segment().is_sent())
        {
            return Ok(Some(NextSeg {
                item: seg,
                is_rescue: false,
            }));
        }

        // Rule 3.
        for seg in erase_lifetime(tx_segs)
            .iter_mut()
            .filter(|s| !s.is_delivered())
        {
            if rule_1a(&seg) && rule_1b(&seg) {
                return Ok(Some(NextSeg {
                    item: seg,
                    is_rescue: false,
                }));
            }
        }

        // Rule 4
        if !self.rescue_rxt_used {
            if let Some(seg) = tx_segs
                .iter_mut()
                .take(SACK_DEPTH)
                .filter(|s| !s.is_delivered())
                .last()
            {
                return Ok(Some(NextSeg {
                    item: seg,
                    is_rescue: true,
                }));
            }
        }
        Ok(None)
    }
}

#[derive(Debug, Clone, Copy)]
pub enum Recovery {
    CountingDuplicates { dup_acks: u8 },
    Recovery(RecoveryPhase),
}

impl Recovery {
    pub fn is_recovery(&self) -> bool {
        matches!(self, Recovery::Recovery { .. })
    }

    pub fn on_ack(
        &mut self,
        header: &UtpHeader,
        on_ack_result: &OnAckResult,
        tx_segs: &mut Segments,
        congestion_controller: &mut dyn CongestionController,
        now: Instant,
        last_consumed_remote_seq_nr: SeqNr,
        last_sent_seq_nr: SeqNr,
    ) {
        match self {
            Recovery::CountingDuplicates { dup_acks } => {
                if header.extensions.selective_ack.is_none() {
                    *dup_acks = 0;
                    return;
                }

                let is_duplicate = on_ack_result.newly_sacked_segment_count > 0;
                if !is_duplicate {
                    return;
                }

                *dup_acks += 1;

                let high_ack = last_consumed_remote_seq_nr;
                let high_data = last_sent_seq_nr;

                let is_lost = tx_segs.is_lost(high_ack + 1, SACK_DEPTH);

                let should_enter_recovery = *dup_acks as usize >= SACK_DUP_THRESH || is_lost;

                debug!(
                    should_enter_recovery,
                    first_is_lost = is_lost,
                    dup_acks,
                    ?high_ack,
                    ?high_data,
                    "counting duplicate acks"
                );

                if !should_enter_recovery {
                    return;
                }

                let high_rxt = high_ack;

                // TODO: we are skipping the part 3
                // "The TCP MAY transmit previously unsent data segments as per Limited transmit"
                congestion_controller.on_triple_duplicate_ack(now);
                let rec = RecoveryPhase {
                    recovery_point: high_data,
                    high_rxt,
                    pipe: tx_segs.calc_sack_pipe(high_rxt),
                    rescue_rxt_used: false,
                };
                debug!(
                    ?rec.recovery_point,
                    ?rec.high_rxt, rec.pipe, "entered recovery"
                );
                *self = Recovery::Recovery(rec);
            }
            Recovery::Recovery(rec) => {
                if header.ack_nr >= rec.recovery_point {
                    debug!(?rec.recovery_point, ?header.ack_nr, "exited recovery");
                    *self = Recovery::CountingDuplicates { dup_acks: 0 };
                    return;
                }
                rec.pipe = tx_segs.calc_sack_pipe(rec.high_rxt);
                debug!(rec.pipe, "recovery: updated pipe");
            }
        }
    }
}
