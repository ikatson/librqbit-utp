// uTP doesn't resegment, so we pre-segment TX once.
// We also store the delivered state of the segments here.
//
// When an ACK arrives this tells us how much (if any) bytes we can
// remove from the actual TX (bytes) that are stored in struct UserTx.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use crate::{metrics::METRICS, raw::UtpHeader, seq_nr::SeqNr};

#[derive(Clone, Copy)]
enum SentStatus {
    NotSent,
    SentTime(Instant),
    Retransmitted { count: usize },
}

struct Segment {
    payload_size: usize,
    is_delivered: bool,
    sent: SentStatus,
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
}

pub struct Segments {
    segments: VecDeque<Segment>,
    len_bytes: usize,
    capacity: usize,

    // SND.UNA - the sequence number of first unacknowledged segment.
    // If all acknowledged, this is the same as SND.NEXT.
    // Name is the same as in https://datatracker.ietf.org/doc/html/rfc9293#section-3.3.1
    snd_una: SeqNr,
}

pub struct SegmentIterItem<'a> {
    segment: &'a mut Segment,
    seq_nr: SeqNr,
    payload_offset: usize,
}

impl SegmentIterItem<'_> {
    pub fn seq_nr(&self) -> SeqNr {
        self.seq_nr
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
        match self.segment.sent {
            SentStatus::NotSent => 0,
            SentStatus::SentTime(..) => 0,
            SentStatus::Retransmitted { count } => count,
        }
    }

    pub fn on_sent(&mut self, now: Instant) {
        self.segment.sent = match self.segment.sent {
            SentStatus::NotSent => {
                METRICS.send_count.increment(1);
                METRICS.sent_bytes.increment(self.payload_size() as u64);
                SentStatus::SentTime(now)
            }
            SentStatus::SentTime(_) => {
                METRICS.retransmissions.increment(1);
                METRICS
                    .retransmitted_bytes
                    .increment(self.payload_size() as u64);
                SentStatus::Retransmitted { count: 1 }
            }
            SentStatus::Retransmitted { count } => {
                METRICS.retransmissions.increment(1);
                METRICS
                    .retransmitted_bytes
                    .increment(self.payload_size() as u64);
                SentStatus::Retransmitted { count: count + 1 }
            }
        };
    }
}

#[derive(Default)]
pub struct OnAckResult {
    pub acked_segments_count: usize,
    pub acked_bytes: usize,
    pub new_rtt: Option<Duration>,
}

impl OnAckResult {
    pub(crate) fn update(&mut self, other: &OnAckResult) {
        self.acked_segments_count += other.acked_segments_count;
        self.acked_bytes += other.acked_bytes;
        self.new_rtt = rtt_min(self.new_rtt, other.new_rtt);
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

impl Segments {
    pub fn new(snd_una: SeqNr) -> Self {
        Segments {
            segments: VecDeque::new(),
            len_bytes: 0,
            capacity: 64,
            snd_una,
        }
    }

    // Named like in rfc9293 SND.NEXT
    pub fn next_seq_nr(&self) -> SeqNr {
        self.snd_una + self.segments.len() as u16
    }

    pub fn first_seq_nr(&self) -> Option<SeqNr> {
        if self.segments.is_empty() {
            None
        } else {
            Some(self.snd_una)
        }
    }

    #[cfg(test)]
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

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    #[allow(unused)]
    pub fn is_full(&self) -> bool {
        self.segments.len() == self.capacity
    }

    #[must_use]
    pub fn enqueue(&mut self, payload_len: usize) -> bool {
        if self.segments.len() == self.capacity {
            return false;
        }
        self.segments.push_back(Segment {
            payload_size: payload_len,
            is_delivered: false,
            sent: SentStatus::NotSent,
        });
        self.len_bytes += payload_len;
        true
    }

    #[allow(unused)]
    pub fn stats(&self) -> impl std::fmt::Debug {
        // this is for debugging only
        format!("headers: {}/{}", self.segments.len(), self.capacity)
    }

    // Returns number of removed headers, and their comibined payload size.
    pub fn remove_up_to_ack(&mut self, now: Instant, ack_header: &UtpHeader) -> OnAckResult {
        let mut removed = 0;
        let mut payload_size = 0;

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
            }
        }

        // If TX start matches with header ACK and it's a selective ACK, mark all segments delivered.
        match (self.first_seq_nr(), ack_header.extensions.selective_ack) {
            (Some(first_seq_nr), Some(sack)) if first_seq_nr > ack_header.ack_nr => {
                let sack_start = ack_header.ack_nr + 2;
                let sack_start_offset = sack_start - first_seq_nr;

                if sack_start_offset >= 0 {
                    for (segment, acked) in self
                        .segments
                        .iter_mut()
                        .skip(sack_start_offset as usize)
                        .zip(sack.iter())
                    {
                        segment.is_delivered |= acked;
                        if acked {
                            segment.update_rtt(now, &mut new_rtt);
                        }
                    }
                } else {
                    for (segment, acked) in self
                        .segments
                        .iter_mut()
                        .zip(sack.iter().skip((-sack_start_offset) as usize))
                    {
                        segment.is_delivered |= acked;
                        if acked {
                            segment.update_rtt(now, &mut new_rtt);
                        }
                    }
                }

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
            }
            _ => {}
        };

        OnAckResult {
            acked_segments_count: removed,
            acked_bytes: payload_size,
            new_rtt,
        }
    }

    // Iterate stored data - headers and their payload offsets (as a function to copy payload to some other buffer).
    pub fn iter_mut(&mut self) -> impl Iterator<Item = SegmentIterItem<'_>> {
        struct State {
            seq_nr: SeqNr,
            payload_offset: usize,
        }
        self.segments.iter_mut().scan(
            State {
                seq_nr: self.snd_una,
                payload_offset: 0,
            },
            |state, segment| {
                let ps = segment.payload_size;
                let item = SegmentIterItem {
                    segment,
                    payload_offset: state.payload_offset,
                    seq_nr: state.seq_nr,
                };
                state.payload_offset += ps;
                state.seq_nr += 1;
                Some(item)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use crate::raw::{selective_ack::SelectiveAck, UtpHeader};

    use super::Segments;

    fn make_segmented_tx(start_seq_nr: u16, count: u16) -> Segments {
        let mut ftx = Segments::new(start_seq_nr.into());
        for _ in 0..count {
            assert!(ftx.enqueue(1));
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
        let ftx = make_segmented_tx(3, 4);
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.1: ACK with sequence number 4, should remove first two packets
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(4, []));
        assert_eq!(ftx.total_len_bytes(), 2);
        assert_eq!(ftx.total_len_packets(), 2);
        assert_eq!(ftx.first_seq_nr().unwrap(), 5.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.2: ACK all packets, queue should be empty
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(6, []));
        assert_eq!(ftx.total_len_bytes(), 0);
        assert_eq!(ftx.total_len_packets(), 0);
        assert!(ftx.first_seq_nr().is_none());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 2: ACK with sequence number 2 (below our start sequence)
        // Should not remove any packets or mark any as delivered
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, []));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 3: ACK with sequence number 3 (matches our start sequence)
        // Should remove the first packet, leaving 3 packets
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(3, []));
        assert_eq!(ftx.total_len_bytes(), 3);
        assert_eq!(ftx.total_len_packets(), 3);
        assert_eq!(ftx.first_seq_nr().unwrap(), 4.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 4: ACK with sequence number 2 and selective ACK for next packet
        // Should mark the second packet as delivered but not remove any
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, [0]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.segments.get(1).unwrap().is_delivered);

        // Test Case 4.1 (edge case): an older selective ACK should mark packets delivered.
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(1, [1])); // means 2 and 3 are not delivered, but 4 is
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.segments.get(1).unwrap().is_delivered);

        // Test Case 5: ACK with sequence number 2 and selective ACK for second and fourth packets
        // Should mark both packets as delivered but not remove any
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(2, [0, 2]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 2);
        assert!(ftx.segments.get(1).unwrap().is_delivered);
        assert!(ftx.segments.get(3).unwrap().is_delivered);

        // Test Case 6: Selective ACK with gaps
        let mut ftx = make_segmented_tx(3, 6); // Create 6 packets
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
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(now, &make_sack_header(1, [0, 1, 2])); // means 3,4,5 were received
        assert_eq!(ftx.total_len_bytes(), 1);
        assert_eq!(ftx.total_len_packets(), 1);
        assert_eq!(ftx.count_delivered_test(), 0);
        assert_eq!(ftx.first_seq_nr().unwrap(), 6.into());

        // Test Case 7.1: selective ACK should be able to clean up the queue - even further away
        let mut ftx = make_segmented_tx(3, 4);
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
        let mut ftx = make_segmented_tx(3, 5);
        for mut item in ftx.iter_mut() {
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
        for mut item in ftx.iter_mut() {
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
}
