// uTP doesn't resegment, so we pre-segment TX once.
// We also store the delivered state of the segments here.
//
// When an ACK arrives this tells us how much (if any) bytes we can
// remove from the actual TX (bytes) that are stored in struct UserTx.
//
// The actual data is kept in UserTx, this only contains metadata about segments we created, sent and delivered.
// Tracks RTT and other stats per segment.
//
// Main flow:
// 1. New segment(s) to be sent is created via enqueue().
// 2. They are iterated to be sent via iter_mut_for_sending(), and marked as sent (if sent).
// 3. Once they are delivered as indicated by ACK, remove_up_to_ack() is called.
//
// The last segment MAY be an MTU probe, where we try to send a segment larger than
// a known working size, in attempt to increase throughput. If it's not delivered in time,
// it's removed and will need to be requeued again.
//
// The downside is no new segments can be enqueued while an MTU probe is outstanding.
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use tracing::trace;

use crate::{constants::calc_pipe_expiry, metrics::METRICS, raw::UtpHeader, seq_nr::SeqNr};

#[derive(Clone, Copy)]
enum SentStatus {
    NotSent,
    SentTime(Instant),
    Retransmitted { count: usize, last_send_ts: Instant },
}

pub struct Segment {
    payload_size: usize,
    payload_offset_absolute: u64,
    is_delivered: bool,
    sent: SentStatus,

    is_mtu_probe: bool,

    // IsLost() from rfc6675
    is_lost: bool,
    is_expired: bool,
    has_sacks_after_it: bool,
}

pub fn rtt_min(rtt1: Option<Duration>, rtt2: Option<Duration>) -> Option<Duration> {
    match (rtt1, rtt2) {
        (None, None) => None,
        (None, Some(r)) | (Some(r), None) => Some(r),
        (Some(r1), Some(r2)) => Some(r1.min(r2)),
    }
}

impl Segment {
    fn update_rtt(&self, now: Instant, rtt: &mut Option<Duration>) {
        match self.sent {
            // This should not happen.
            SentStatus::NotSent | SentStatus::Retransmitted { .. } => {}
            SentStatus::SentTime(sent_ts) => {
                *rtt = rtt_min(*rtt, Some(now - sent_ts));
            }
        }
    }

    pub fn retransmit_count(&self) -> usize {
        match self.sent {
            SentStatus::NotSent => 0,
            SentStatus::SentTime(..) => 0,
            SentStatus::Retransmitted { count, .. } => count,
        }
    }

    pub fn send_count(&self) -> usize {
        match self.sent {
            SentStatus::NotSent => 0,
            SentStatus::SentTime(..) => 1,
            SentStatus::Retransmitted { count, .. } => count + 1,
        }
    }

    pub fn is_mtu_probe(&self) -> bool {
        self.is_mtu_probe
    }

    fn last_sent(&self) -> Option<Instant> {
        match self.sent {
            SentStatus::NotSent => None,
            SentStatus::SentTime(instant) => Some(instant),
            SentStatus::Retransmitted { last_send_ts, .. } => Some(last_send_ts),
        }
    }
}

pub struct Segments {
    segments: VecDeque<Segment>,
    // This is including unsent. So it's not FlightSize
    len_bytes: usize,

    // absolute offset in bytes since creation.
    offset: u64,

    // how many bytes were consumed since creation.
    removed_offset: u64,

    // If a SACK seen, this will set how many sgements did it cover.
    sack_depth: usize,
    last_sack_empty: bool,

    // SND.UNA - the sequence number of first unacknowledged segment.
    // If all acknowledged, this is the same as SND.NEXT.
    // Name is the same as in https://datatracker.ietf.org/doc/html/rfc9293#section-3.3.1
    snd_una: SeqNr,
}

pub struct SegmentForSending<'a> {
    segment: &'a mut Segment,
    seq_nr: SeqNr,
    payload_offset: usize,
}

impl SegmentForSending<'_> {
    pub fn is_lost(&self) -> bool {
        self.segment.is_lost
    }

    pub fn is_expired(&self) -> bool {
        self.segment.is_expired
    }

    pub fn send_count(&self) -> usize {
        self.segment.send_count()
    }

    pub fn has_sacks_after_it(&self) -> bool {
        self.segment.has_sacks_after_it
    }

    pub fn seq_nr(&self) -> SeqNr {
        self.seq_nr
    }

    pub fn is_mtu_probe(&self) -> bool {
        self.segment.is_mtu_probe()
    }

    pub fn is_delivered(&self) -> bool {
        self.segment.is_delivered
    }
    pub fn payload_size(&self) -> usize {
        self.segment.payload_size
    }
    pub fn payload_offset(&self) -> usize {
        self.payload_offset
    }

    pub fn retransmit_count(&self) -> usize {
        self.segment.retransmit_count()
    }

    pub fn on_sent(&mut self, now: Instant) {
        let ps = self.segment.payload_size as u64;
        self.segment.sent = match self.segment.sent {
            SentStatus::NotSent => {
                METRICS.send_count.increment(1);
                METRICS.sent_bytes.increment(ps);
                METRICS.sent_payload_size.record(ps as f64);
                SentStatus::SentTime(now)
            }
            SentStatus::SentTime(_) => {
                METRICS.data_retransmissions.increment(1);
                METRICS.retransmitted_bytes.increment(ps);
                SentStatus::Retransmitted {
                    count: 1,
                    last_send_ts: now,
                }
            }
            SentStatus::Retransmitted { count, .. } => {
                METRICS.data_retransmissions.increment(1);
                METRICS.retransmitted_bytes.increment(ps);
                SentStatus::Retransmitted {
                    count: count + 1,
                    last_send_ts: now,
                }
            }
        };
    }
}

#[derive(Default)]
pub struct OnAckResult {
    // How many segments we can remove from the beginning of TX queue.
    pub acked_segments_count: usize,

    // How many bytes we can remove from the beginning of TX queue.
    pub acked_bytes: usize,

    pub max_acked_payload_size: usize,

    // How many segments in TX queue were marked delivered by SACK.
    pub newly_sacked_segment_count: usize,

    pub newly_sacked_byte_count: usize,
    pub new_rtt: Option<Duration>,
}

impl OnAckResult {
    pub(crate) fn update(&mut self, other: &OnAckResult) {
        self.acked_segments_count += other.acked_segments_count;
        self.acked_bytes += other.acked_bytes;
        self.new_rtt = rtt_min(self.new_rtt, other.new_rtt);
        self.newly_sacked_segment_count += other.newly_sacked_segment_count;
        self.newly_sacked_byte_count += other.newly_sacked_byte_count;
    }
}

impl core::fmt::Debug for OnAckResult {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "acked_segments={}, bytes={}, new_rtt={:?}",
            self.acked_segments_count, self.acked_bytes, self.new_rtt
        )
    }
}

#[derive(Clone, Copy)]
pub struct Pipe {
    pub pipe: usize,
    pub recalc_timer: Option<Instant>,
}

impl std::fmt::Debug for Pipe {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "pipe={},recalc_timer={:?}",
            self.pipe,
            self.recalc_timer.map(|t| Instant::now() - t)
        )
    }
}

#[derive(Debug)]
pub enum PopExpiredProbe {
    Expired {
        rewind_to: SeqNr,
        payload_size: usize,
    },
    NotExpired,
    Empty,
}

impl Segments {
    pub fn new(snd_una: SeqNr) -> Self {
        Segments {
            segments: VecDeque::new(),
            // TODO: store total sacked (marked delivered through sack)
            len_bytes: 0,
            offset: 0,
            removed_offset: 0,
            sack_depth: 0,
            last_sack_empty: false,
            snd_una,
        }
    }

    // // Named like in rfc9293 SND.NEXT
    // pub fn next_seq_nr(&self) -> SeqNr {
    //     self.snd_una + self.segments.len() as u16
    // }

    pub fn first_seq_nr(&self) -> Option<SeqNr> {
        if self.segments.is_empty() {
            None
        } else {
            Some(self.snd_una)
        }
    }

    pub fn total_len_packets(&self) -> usize {
        self.segments.len()
    }

    #[cfg(test)]
    pub fn count_delivered_test(&self) -> usize {
        self.segments
            .iter()
            .map(|h| if h.is_delivered { 1 } else { 0 })
            .sum()
    }

    pub fn total_len_bytes(&self) -> usize {
        self.len_bytes
    }

    pub fn sack_depth(&self) -> usize {
        self.sack_depth
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    /// Enqueue a segment for sending at the end. If it's an MTU probe, mtu_probe_expiry is not None.
    /// Returns if the segment was enqueued or not.
    ///
    /// TODO: if there's an outstanding MTU probe, don't allow enqueueing
    #[must_use]
    pub fn enqueue(&mut self, payload_len: usize, is_mtu_probe: bool) -> bool {
        self.segments.push_back(Segment {
            payload_size: payload_len,
            payload_offset_absolute: self.offset,
            is_delivered: false,
            is_mtu_probe,
            sent: SentStatus::NotSent,
            is_lost: false,
            is_expired: false,
            has_sacks_after_it: false,
        });
        self.offset += payload_len as u64;
        self.len_bytes += payload_len;
        true
    }

    pub fn pop_mtu_probe(&mut self, seq_nr: SeqNr) -> bool {
        let last_segment_seq_nr = self.snd_una + self.segments.len() as u16 - 1;
        match self.segments.pop_back() {
            Some(s) if last_segment_seq_nr == seq_nr && s.is_mtu_probe && !s.is_delivered => true,
            Some(s) => {
                self.segments.push_back(s);
                false
            }
            None => false,
        }
    }

    /// Try to pop an MTU probe if it's expired.
    pub fn pop_expired_mtu_probe(
        &mut self,
        retransmit_timed_out: bool,
        max_probe_retransmissions: usize,
    ) -> PopExpiredProbe {
        match self.segments.pop_back() {
            Some(s) => match (retransmit_timed_out, s.is_mtu_probe) {
                (_, _) if s.is_delivered => {
                    self.segments.push_back(s);
                    PopExpiredProbe::Empty
                }
                (true, true) if s.retransmit_count() >= max_probe_retransmissions => {
                    PopExpiredProbe::Expired {
                        payload_size: s.payload_size,
                        rewind_to: self.snd_una + self.segments.len() as u16 - 1,
                    }
                }
                (_, true) => {
                    self.segments.push_back(s);
                    PopExpiredProbe::NotExpired
                }
                (_, false) => {
                    self.segments.push_back(s);
                    PopExpiredProbe::Empty
                }
            },
            None => PopExpiredProbe::Empty,
        }
    }

    #[allow(unused)]
    pub fn stats(&self) -> impl std::fmt::Debug {
        // this is for debugging only
        format!("headers: {}", self.segments.len())
    }

    // Returns number of removed headers, and their comibined payload size.
    pub fn remove_up_to_ack(&mut self, now: Instant, ack_header: &UtpHeader) -> OnAckResult {
        let mut removed = 0;
        let mut payload_size = 0;
        let mut newly_sacked_segment_count: usize = 0;
        let mut newly_sacked_byte_count: usize = 0;
        let mut max_acked_payload_size: usize = 0;

        let mut new_rtt: Option<Duration> = None;

        let offset = ack_header.ack_nr - self.snd_una;
        if offset >= 0 {
            let drain_count = (offset as usize + 1).min(self.segments.len());
            for segment in self.segments.drain(0..drain_count) {
                segment.update_rtt(now, &mut new_rtt);
                removed += 1;
                payload_size += segment.payload_size;
                self.snd_una += 1;
                self.len_bytes -= segment.payload_size;
                max_acked_payload_size = max_acked_payload_size.max(segment.payload_size);
            }
        }

        // If TX start matches with header ACK and it's a selective ACK, mark all segments delivered.
        match (self.first_seq_nr(), ack_header.extensions.selective_ack) {
            (Some(first_seq_nr), Some(sack)) if first_seq_nr > ack_header.ack_nr => {
                self.sack_depth = sack.len();
                self.last_sack_empty = sack.as_bitslice().not_any();

                let sack_start = ack_header.ack_nr + 2;
                let sack_start_offset = sack_start - first_seq_nr;

                let mut process_sack = |segment: &mut Segment, is_sacked: bool| {
                    if !segment.is_delivered && is_sacked {
                        segment.is_delivered = true;
                        segment.update_rtt(now, &mut new_rtt);
                        max_acked_payload_size = max_acked_payload_size.max(segment.payload_size);
                        newly_sacked_segment_count += 1;
                        newly_sacked_byte_count += segment.payload_size;
                    }
                };

                if sack_start_offset >= 0 {
                    for (segment, acked) in self
                        .segments
                        .iter_mut()
                        .skip(sack_start_offset as usize)
                        .zip(sack.iter())
                    {
                        process_sack(segment, acked);
                    }
                } else {
                    for (segment, acked) in self
                        .segments
                        .iter_mut()
                        .zip(sack.iter().skip((-sack_start_offset) as usize))
                    {
                        process_sack(segment, acked);
                    }
                }
            }
            _ => {}
        };

        // Cleanup the beginning of the queue if an older ACK ends up marking it delivered.
        while let Some(segment) = self.segments.front() {
            if !segment.is_delivered {
                break;
            }
            removed += 1;
            payload_size += segment.payload_size;
            self.len_bytes -= segment.payload_size;
            self.snd_una += 1;
            self.segments.pop_front().unwrap();
        }

        self.removed_offset += payload_size as u64;

        OnAckResult {
            acked_segments_count: removed,
            acked_bytes: payload_size,
            max_acked_payload_size,
            newly_sacked_segment_count,
            newly_sacked_byte_count,
            new_rtt,
        }
    }

    // The total payload of sent and unacked segments.
    pub fn calc_flight_size(&self, last_sent_seq_nr: SeqNr) -> usize {
        // On RTO, we rewind back last_sent_seq_nr, however we don't reset the "sent" status
        // of packets. So instead of using "is_sent", just iterate up to last_sent_seq_nr, and pretend we
        // never sent them.
        let take = (last_sent_seq_nr - self.snd_una + 1).max(0) as usize;
        self.segments
            .iter()
            .take(take)
            .map(|s| if s.is_delivered { 0 } else { s.payload_size })
            .sum()
    }

    // rfc6675: pipe is the estimate of how much outstanding data is in the network.
    // used during SACK-based recovery not to overwhelm the network.
    pub fn calc_pipe(
        &mut self,
        high_rxt: SeqNr,
        high_data: SeqNr,
        rtt: Duration,
        now: Instant,
    ) -> Pipe {
        let mut pipe = 0;
        let mut delivered_segs = 0;
        let mut recalc_timer = None;

        let take = (high_data - self.snd_una).max(0) as usize;

        // After this we don't consider the packet being in the network.
        // rtt/2 might be a bit too aggressive.
        let expiry_threshold = calc_pipe_expiry(rtt);

        for (offset, seq_nr, segment, last_sent) in self
            .segments
            .range_mut(..take)
            .enumerate()
            .rev()
            .filter_map(|(offset, seg)| {
                seg.last_sent()
                    .map(|ts| (offset, self.snd_una + offset as u16, seg, ts))
            })
        {
            if segment.is_delivered {
                delivered_segs += 1;
                continue;
            }

            segment.has_sacks_after_it = delivered_segs > 0 || self.last_sack_empty;

            if seq_nr <= high_rxt {
                pipe += segment.payload_size;
            }

            segment.is_expired = now - last_sent >= expiry_threshold;

            // If a segment is past sack depth limit, we don't know anything about it.
            segment.is_lost = if offset > self.sack_depth + 1 {
                segment.is_expired
            } else {
                // Because of the segments we don't know about, we can't fully use rfc6675, so
                // be conservative here.
                delivered_segs >= 3 || segment.is_expired
            };

            if !segment.is_lost {
                pipe += segment.payload_size;
            }

            if !segment.is_expired && !segment.is_lost {
                let expires = last_sent + expiry_threshold;
                recalc_timer = match recalc_timer {
                    Some(ts) if expires < ts => Some(expires),
                    _ => Some(expires),
                }
            }
        }

        trace!(?pipe, ?recalc_timer, ?high_rxt, high_ack=?self.snd_una, ?high_data, "calc_pipe");
        Pipe { pipe, recalc_timer }
    }

    // Iterate stored data - headers and their payload offsets (as a function to copy payload to some other buffer).
    pub fn iter_mut_for_sending(
        &mut self,
        start: Option<SeqNr>,
    ) -> impl Iterator<Item = SegmentForSending<'_>> {
        let offset = match start {
            Some(start) => (start - self.snd_una).max(0) as usize,
            None => 0,
        };
        let removed_abs = self.removed_offset;
        let snd_una = self.snd_una;
        let range = if offset >= self.segments.len() {
            self.segments.len()..
        } else {
            offset..
        };
        self.segments
            .range_mut(range)
            .enumerate()
            .map(move |(idx, seg)| SegmentForSending {
                seq_nr: snd_una + (offset + idx) as u16,
                payload_offset: seg
                    .payload_offset_absolute
                    .checked_sub(removed_abs)
                    .unwrap() as usize,
                segment: seg,
            })
            .filter(|s| !s.is_delivered())
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::raw::{UtpHeader, selective_ack::SelectiveAck};

    use super::Segments;

    fn make_segments(start_seq_nr: u16, count: u16) -> Segments {
        let mut ftx = Segments::new(start_seq_nr.into());
        for _ in 0..count {
            assert!(ftx.enqueue(1, false));
        }

        ftx
    }

    fn make_sack_header(ack_nr: u16, acked: impl IntoIterator<Item = usize>) -> UtpHeader {
        UtpHeader {
            ack_nr: ack_nr.into(),
            extensions: crate::raw::Extensions {
                selective_ack: SelectiveAck::new_test(acked),
                ..Default::default()
            },
            ..Default::default()
        }
    }

    #[test]
    fn test_remove_up_to_ack() {
        // Test Case 1: Basic setup with 4 packets starting from sequence number 3
        let now = Instant::now();
        let ftx = make_segments(3, 4);
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.1: ACK with sequence number 4, should remove first two packets
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(4, []));
        assert_eq!(ftx.total_len_bytes(), 2);
        assert_eq!(ftx.total_len_packets(), 2);
        assert_eq!(ftx.first_seq_nr().unwrap(), 5.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.2: ACK all packets, queue should be empty
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(6, []));
        assert_eq!(ftx.total_len_bytes(), 0);
        assert_eq!(ftx.total_len_packets(), 0);
        assert!(ftx.first_seq_nr().is_none());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 2: ACK with sequence number 2 (below our start sequence)
        // Should not remove any packets or mark any as delivered
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, []));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 3: ACK with sequence number 3 (matches our start sequence)
        // Should remove the first packet, leaving 3 packets
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(3, []));
        assert_eq!(ftx.total_len_bytes(), 3);
        assert_eq!(ftx.total_len_packets(), 3);
        assert_eq!(ftx.first_seq_nr().unwrap(), 4.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 4: ACK with sequence number 2 and selective ACK for next packet
        // Should mark the second packet as delivered but not remove any
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, [0]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.segments.get(1).unwrap().is_delivered);

        // Test Case 4.1 (edge case): an older selective ACK should mark packets delivered.
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(1, [1])); // means 2 and 3 are not delivered, but 4 is
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.segments.get(1).unwrap().is_delivered);

        // Test Case 5: ACK with sequence number 2 and selective ACK for second and fourth packets
        // Should mark both packets as delivered but not remove any
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, [0, 2]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 2);
        assert!(ftx.segments.get(1).unwrap().is_delivered);
        assert!(ftx.segments.get(3).unwrap().is_delivered);

        // Test Case 6: Selective ACK with gaps
        let mut ftx = make_segments(3, 6); // Create 6 packets
        ftx.remove_up_to_ack(now, &make_sack_header(2, [0, 2, 4])); // ACK 2nd, 4th, and 6th packets
        assert_eq!(ftx.total_len_bytes(), 6);
        assert_eq!(ftx.total_len_packets(), 6);
        assert_eq!(ftx.count_delivered_test(), 3);
        assert!(ftx.segments.get(1).unwrap().is_delivered);
        assert!(ftx.segments.get(3).unwrap().is_delivered);
        assert!(ftx.segments.get(5).unwrap().is_delivered);
        assert!(!ftx.segments.front().unwrap().is_delivered);
        assert!(!ftx.segments.get(2).unwrap().is_delivered);
        assert!(!ftx.segments.get(4).unwrap().is_delivered);

        // Test Case 7: selective ACK should be able to clean up the queue
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(1, [0, 1, 2])); // means 3,4,5 were received
        assert_eq!(ftx.total_len_bytes(), 1);
        assert_eq!(ftx.total_len_packets(), 1);
        assert_eq!(ftx.count_delivered_test(), 0);
        assert_eq!(ftx.first_seq_nr().unwrap(), 6.into());

        // Test Case 7.1: selective ACK should be able to clean up the queue - even further away
        let mut ftx = make_segments(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(0, [0, 1, 2, 3])); // means 3,4,5 were received
        assert_eq!(ftx.total_len_bytes(), 1);
        assert_eq!(ftx.total_len_packets(), 1);
        assert_eq!(ftx.count_delivered_test(), 0);
        assert_eq!(ftx.first_seq_nr().unwrap(), 6.into());
    }

    #[test]
    fn test_rtt() {
        // Test Case 1: Basic setup with 4 packets starting from sequence number 3
        let mut now = Instant::now();
        let mut ftx = make_segments(3, 5);
        for mut item in ftx.iter_mut_for_sending(None) {
            item.on_sent(now);
        }
        now += Duration::from_secs(1);
        let res = ftx.remove_up_to_ack(now, &make_sack_header(3, []));
        assert_eq!(res.new_rtt, Some(Duration::from_secs(1)));

        // One second later an ACK arrives for later segment (delayed ACK)
        now += Duration::from_secs(1);
        let res = ftx.remove_up_to_ack(now, &make_sack_header(5, []));
        // This inflates rtt
        assert_eq!(res.new_rtt, Some(Duration::from_secs(2)));
        assert_eq!(ftx.first_seq_nr(), Some(6.into()));

        // We retransmit the rest
        for mut item in ftx.iter_mut_for_sending(None) {
            item.on_sent(now);
        }

        // they are ACKed, but RTT should not be updated for retransmitted packets.
        now += Duration::from_secs(1);
        let res = ftx.remove_up_to_ack(now, &make_sack_header(7, []));
        assert_eq!(ftx.first_seq_nr(), None);
        assert_eq!(res.acked_segments_count, 2);
        // This inflates rtt
        assert_eq!(res.new_rtt, None);
    }

    #[test]
    fn test_iter_mut_offsets() {
        let mut segs = Segments::new(0.into());
        assert!(segs.enqueue(1, false));
        assert!(segs.enqueue(2, false));
        assert!(segs.enqueue(3, false));

        assert_eq!(
            segs.iter_mut_for_sending(None)
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(0, 0, 1), (1, 1, 2), (2, 3, 3)]
        );

        assert_eq!(
            segs.remove_up_to_ack(
                Instant::now(),
                &UtpHeader {
                    ack_nr: 0.into(),
                    ..Default::default()
                },
            )
            .acked_bytes,
            1
        );

        assert_eq!(
            segs.iter_mut_for_sending(None)
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(1, 0, 2), (2, 2, 3)]
        );

        assert!(segs.enqueue(4, false));
        assert_eq!(
            segs.iter_mut_for_sending(None)
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(1, 0, 2), (2, 2, 3), (3, 5, 4)]
        );
        assert_eq!(
            segs.remove_up_to_ack(
                Instant::now(),
                &UtpHeader {
                    ack_nr: 2.into(),
                    ..Default::default()
                },
            )
            .acked_bytes,
            5
        );

        assert_eq!(
            segs.iter_mut_for_sending(None)
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(3, 0, 4)]
        );

        assert_eq!(
            segs.iter_mut_for_sending(Some(3.into()))
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(3, 0, 4)]
        );

        assert_eq!(
            segs.iter_mut_for_sending(Some(4.into()))
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![]
        );

        // From the past
        assert_eq!(
            segs.iter_mut_for_sending(Some(0.into()))
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![(3, 0, 4)]
        );

        assert_eq!(
            segs.iter_mut_for_sending(Some(1000.into()))
                .map(|s| (s.seq_nr().0, s.payload_offset(), s.payload_size()))
                .collect::<Vec<_>>(),
            vec![]
        );
    }
}
