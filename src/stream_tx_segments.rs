// uTP doesn't resegment, so we pre-segment TX once.
// We also store the delivered state of the segments here.
//
// When an ACK arrives this tells us how much (if any) bytes we can
// remove from the actual TX (bytes) that are stored in struct UserTx.

use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

use tracing::debug;

use crate::{raw::UtpHeader, seq_nr::SeqNr};

enum RttMeasurement {
    NotSent,
    SentTime(Instant),
    Retransmitted,
}

struct Segment {
    payload_size: usize,
    is_delivered: bool,
    rtt: RttMeasurement,
}

pub struct Segments {
    segments: VecDeque<Segment>,
    len_bytes: usize,
    capacity: usize,
    first_seq_nr: Option<SeqNr>,
}

pub struct SegmentIterItem<'a> {
    segment: &'a Segment,
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
}

pub struct OnAckResult {
    pub acked_segments_count: usize,
    pub acked_bytes: usize,
    pub new_rtt: Option<Duration>,
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
    pub fn new() -> Self {
        Segments {
            segments: VecDeque::new(),
            len_bytes: 0,
            capacity: 64,
            first_seq_nr: None,
        }
    }

    pub fn first_seq_nr(&self) -> Option<SeqNr> {
        self.first_seq_nr
    }

    pub fn last_seq_nr(&self) -> Option<SeqNr> {
        match (self.first_seq_nr, self.segments.back()) {
            (Some(first), Some(_)) => Some(first + (self.segments.len() as u16) - 1),
            (None, None) => None,
            _ => panic!("code is very bugged"),
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
    pub fn enqueue(&mut self, seq_nr: SeqNr, payload_len: usize) -> bool {
        if self.segments.len() == self.capacity {
            return false;
        }
        if let Some(last) = self.last_seq_nr() {
            if seq_nr - last != 1 {
                debug!(?seq_nr, last_seq_nr=?seq_nr, "can only enqueue incresing sequence numbers");
                return false;
            }
        } else {
            self.first_seq_nr = Some(seq_nr);
        }
        self.segments.push_back(Segment {
            payload_size: payload_len,
            is_delivered: false,
            rtt: RttMeasurement::NotSent,
        });
        self.len_bytes += payload_len;
        true
    }

    #[allow(unused)]
    pub fn stats(&self) -> impl std::fmt::Debug {
        // this is for debugging only
        format!("headers: {}/{}", self.segments.len(), self.capacity)
    }

    fn pop_front(&mut self) -> Option<Segment> {
        let frag = self.segments.pop_front()?;
        self.len_bytes -= frag.payload_size;
        if self.segments.is_empty() {
            self.first_seq_nr = None;
        } else {
            // if this fails, it's a big bug
            self.first_seq_nr = Some(self.first_seq_nr.unwrap() + 1);
        }
        Some(frag)
    }

    // Returns number of removed headers, and their comibined payload size.
    pub fn remove_up_to_ack(&mut self, ack_header: &UtpHeader) -> OnAckResult {
        let mut removed = 0;
        let mut payload_size = 0;

        while let Some(seq_nr) = self.first_seq_nr {
            if ack_header.ack_nr < seq_nr {
                break;
            }
            let segment = self.pop_front().unwrap();
            removed += 1;
            payload_size += segment.payload_size;
        }

        let mut new_rtt: Option<Duration> = None;

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
                    }
                } else {
                    for (segment, acked) in self
                        .segments
                        .iter_mut()
                        .zip(sack.iter().skip((-sack_start_offset) as usize))
                    {
                        segment.is_delivered |= acked;
                    }
                }

                // Cleanup the beginning of the queue if an older ACK ends up marking it delivered.
                while let Some(segment) = self.segments.front() {
                    if !segment.is_delivered {
                        break;
                    }
                    removed += 1;
                    payload_size += segment.payload_size;
                    self.pop_front().unwrap();
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
    pub fn iter(&self) -> impl Iterator<Item = SegmentIterItem<'_>> {
        struct State {
            seq_nr: SeqNr,
            payload_offset: usize,
        }
        self.segments.iter().scan(
            State {
                seq_nr: self.first_seq_nr.unwrap_or(0.into()),
                payload_offset: 0,
            },
            |state, segment| {
                let item = SegmentIterItem {
                    segment,
                    payload_offset: state.payload_offset,
                    seq_nr: state.seq_nr,
                };
                state.payload_offset += segment.payload_size;
                state.seq_nr += 1;
                Some(item)
            },
        )
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        raw::{selective_ack::SelectiveAck, UtpHeader},
        seq_nr::SeqNr,
    };

    use super::Segments;

    fn make_segmented_tx(start_seq_nr: u16, count: u16) -> Segments {
        let mut ftx = Segments::new();

        let start: SeqNr = start_seq_nr.into();
        let end = start + count;
        for seq_nr in start.0..end.0 {
            assert!(ftx.enqueue(seq_nr.into(), 1));
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
        let ftx = make_segmented_tx(3, 4);
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.1: ACK with sequence number 4, should remove first two packets
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(4, []));
        assert_eq!(ftx.total_len_bytes(), 2);
        assert_eq!(ftx.total_len_packets(), 2);
        assert_eq!(ftx.first_seq_nr().unwrap(), 5.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.2: ACK all packets, queue should be empty
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(6, []));
        assert_eq!(ftx.total_len_bytes(), 0);
        assert_eq!(ftx.total_len_packets(), 0);
        assert!(ftx.first_seq_nr().is_none());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 2: ACK with sequence number 2 (below our start sequence)
        // Should not remove any packets or mark any as delivered
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(2, []));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 3: ACK with sequence number 3 (matches our start sequence)
        // Should remove the first packet, leaving 3 packets
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(3, []));
        assert_eq!(ftx.total_len_bytes(), 3);
        assert_eq!(ftx.total_len_packets(), 3);
        assert_eq!(ftx.first_seq_nr().unwrap(), 4.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 4: ACK with sequence number 2 and selective ACK for next packet
        // Should mark the second packet as delivered but not remove any
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(2, [0]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.segments.get(1).unwrap().is_delivered);

        // Test Case 4.1 (edge case): an older selective ACK should mark packets delivered.
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(1, [1])); // means 2 and 3 are not delivered, but 4 is
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.segments.get(1).unwrap().is_delivered);

        // Test Case 5: ACK with sequence number 2 and selective ACK for second and fourth packets
        // Should mark both packets as delivered but not remove any
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(2, [0, 2]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 2);
        assert!(ftx.segments.get(1).unwrap().is_delivered);
        assert!(ftx.segments.get(3).unwrap().is_delivered);

        // Test Case 6: Selective ACK with gaps
        let mut ftx = make_segmented_tx(3, 6); // Create 6 packets
        ftx.remove_up_to_ack(&make_sack_header(2, [0, 2, 4])); // ACK 2nd, 4th, and 6th packets
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
        ftx.remove_up_to_ack(&make_sack_header(1, [0, 1, 2])); // means 3,4,5 were received
        assert_eq!(ftx.total_len_bytes(), 1);
        assert_eq!(ftx.total_len_packets(), 1);
        assert_eq!(ftx.count_delivered_test(), 0);
        assert_eq!(ftx.first_seq_nr().unwrap(), 6.into());

        // Test Case 7.1: selective ACK should be able to clean up the queue - even further away
        let mut ftx = make_segmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(0, [0, 1, 2, 3])); // means 3,4,5 were received
        assert_eq!(ftx.total_len_bytes(), 1);
        assert_eq!(ftx.total_len_packets(), 1);
        assert_eq!(ftx.count_delivered_test(), 0);
        assert_eq!(ftx.first_seq_nr().unwrap(), 6.into());
    }
}
