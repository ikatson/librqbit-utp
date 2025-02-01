use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    sync::Arc,
    task::{ready, Poll, Waker},
};

use parking_lot::Mutex;
use tokio::io::AsyncWrite;
use tracing::trace;

use crate::{
    raw::UtpHeader,
    seq_nr::SeqNr,
    utils::{fill_buffer_from_rb, update_optional_waker},
};

struct Fragmented {
    header: UtpHeader,
    payload_size: usize,
    is_delivered: bool,
}

type RingBufferHeader = VecDeque<Fragmented>;

pub struct UserTxLocked {
    // Set when stream dies abruptly for writer to know about it.
    pub dead: bool,
    // When the writer shuts down, or both reader and writer die, the stream is closed.
    closed: bool,

    capacity: usize,
    buffer: VecDeque<u8>,

    // This is woken up by dispatcher when the buffer has space if it didn't have it previously.
    pub buffer_no_longer_full: Option<Waker>,
    // This is woken up by dispatcher when all outstanding packets where ACKed.
    pub buffer_flushed: Option<Waker>,

    // This is woken by by writer when it has put smth into the buffer.
    pub buffer_has_data: Option<Waker>,
}

impl UserTxLocked {
    pub fn truncate_front(&mut self, count: usize) {
        drop(self.buffer.drain(..count))
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    pub fn buffer(&self) -> &VecDeque<u8> {
        &self.buffer
    }

    pub fn fill_buffer_from_ring_buffer(
        &self,
        out_buf: &mut [u8],
        offset: usize,
        len: usize,
    ) -> anyhow::Result<()> {
        fill_buffer_from_rb(out_buf, &self.buffer, offset, len)
    }

    pub fn enqueue_slice(&mut self, bytes: &[u8]) -> usize {
        // I really hope that gets optimized away
        let len = (self.capacity - self.buffer.len()).min(bytes.len());
        let it = bytes.iter().copied().take(len);
        self.buffer.extend(it);
        len
    }

    pub fn is_full(&self) -> bool {
        self.buffer.len() == self.capacity
    }

    // This will send FIN (if not yet).
    pub fn mark_stream_dead(&mut self) {
        self.dead = true;
        if let Some(waker) = self.buffer_no_longer_full.take() {
            waker.wake();
        }
    }

    pub fn close(&mut self) {
        if !self.closed {
            trace!("closing writer");
            self.closed = true;
            if let Some(w) = self.buffer_has_data.take() {
                w.wake();
            }
        }
    }
}

pub struct UserTx {
    pub locked: Mutex<UserTxLocked>,
}

impl UserTx {
    pub fn new(capacity: NonZeroUsize) -> Arc<Self> {
        Arc::new(UserTx {
            locked: Mutex::new(UserTxLocked {
                buffer: VecDeque::new(),
                capacity: capacity.get(),
                buffer_no_longer_full: None,
                buffer_has_data: None,
                buffer_flushed: None,
                dead: false,
                closed: false,
            }),
        })
    }

    pub fn mark_stream_dead(&self) {
        self.locked.lock().mark_stream_dead();
    }
}

// The TX queue of the virtual socket. uTP doesn't refragemnt, so we store the original headers.
// The payloads are stored in the user TX behind a shared lock. Users write there with poll_write().
//
// TODO: this is redundant, we can store less info. Just the first seq_nr, that's it.
pub struct FragmentedTx {
    headers: RingBufferHeader,
    len_bytes: usize,
    capacity: usize,
}

pub struct TxIterItem<'a> {
    fragment: &'a Fragmented,
    payload_offset: usize,
}

impl TxIterItem<'_> {
    pub fn header(&self) -> &UtpHeader {
        &self.fragment.header
    }
    pub fn is_delivered(&self) -> bool {
        self.fragment.is_delivered
    }
    pub fn payload_size(&self) -> usize {
        self.fragment.payload_size
    }
    pub fn payload_offset(&self) -> usize {
        self.payload_offset
    }
}

impl FragmentedTx {
    pub fn new() -> Self {
        FragmentedTx {
            headers: VecDeque::new(),
            len_bytes: 0,
            capacity: 64,
        }
    }

    pub fn first_seq_nr(&self) -> Option<SeqNr> {
        Some(self.headers.front()?.header.seq_nr)
    }

    #[cfg(test)]
    pub fn total_len_packets(&self) -> usize {
        self.headers.len()
    }

    #[cfg(test)]
    pub fn count_delivered_test(&self) -> usize {
        self.headers
            .iter()
            .map(|h| if h.is_delivered { 1 } else { 0 })
            .sum()
    }

    pub fn total_len_bytes(&self) -> usize {
        self.len_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    #[allow(unused)]
    pub fn is_full(&self) -> bool {
        self.headers.len() == self.capacity
    }

    #[must_use]
    pub fn enqueue(&mut self, header: UtpHeader, payload_len: usize) -> bool {
        if self.headers.len() == self.capacity {
            return false;
        }
        self.headers.push_back(Fragmented {
            header,
            payload_size: payload_len,
            is_delivered: false,
        });
        self.len_bytes += payload_len;
        true
    }

    #[allow(unused)]
    pub fn stats(&self) -> impl std::fmt::Debug {
        // this is for debugging only
        format!("headers: {}/{}", self.headers.len(), self.capacity)
    }

    // Returns number of removed headers, and their comibined payload size.
    pub fn remove_up_to_ack(&mut self, ack_header: &UtpHeader) -> (usize, usize) {
        let mut removed = 0;
        let mut payload_size = 0;

        while let Some(fragment) = self.headers.front() {
            if ack_header.ack_nr < fragment.header.seq_nr {
                break;
            }
            let fragment = self.headers.pop_front().unwrap();
            removed += 1;
            payload_size += fragment.payload_size;
            self.len_bytes -= fragment.payload_size;
        }

        // If TX start matches with header ACK and it's a selective ACK, mark all fragments delivered.
        match (self.headers.front(), ack_header.extensions.selective_ack) {
            (Some(fragment), Some(sack)) if fragment.header.seq_nr > ack_header.ack_nr => {
                let offset = (fragment.header.seq_nr - ack_header.ack_nr) as usize;

                for (fragment, acked) in self.headers.iter_mut().skip(offset).zip(sack.iter()) {
                    fragment.is_delivered |= acked;
                }
            }
            _ => {}
        };

        (removed, payload_size)
    }

    // Iterate stored data - headers and their payloads (as a function to copy payload to some other buffer).
    pub fn iter(&self) -> impl Iterator<Item = TxIterItem<'_>> {
        self.headers.iter().scan(0, |offset, fragment| {
            let current_offset = *offset;
            *offset += fragment.payload_size;
            Some(TxIterItem {
                fragment,
                payload_offset: current_offset,
            })
        })
    }
}

pub struct UtpStreamWriteHalf {
    user_tx: Arc<UserTx>,
}

impl UtpStreamWriteHalf {
    pub fn new(user_tx: Arc<UserTx>) -> Self {
        Self { user_tx }
    }

    fn close(&mut self) {
        self.user_tx.locked.lock().close();
    }
}

impl Drop for UtpStreamWriteHalf {
    fn drop(&mut self) {
        self.close();
    }
}

impl AsyncWrite for UtpStreamWriteHalf {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let mut g = self.user_tx.locked.lock();

        if g.dead {
            return Poll::Ready(Err(std::io::Error::other("socket died")));
        }

        if g.closed {
            return Poll::Ready(Err(std::io::Error::other(
                "shutdown was initiated, can't write",
            )));
        }

        let count = g.enqueue_slice(buf);
        if count == 0 {
            assert!(g.is_full());
            update_optional_waker(&mut g.buffer_no_longer_full, cx);
            return Poll::Pending;
        }

        if let Some(w) = g.buffer_has_data.take() {
            w.wake()
        }

        Poll::Ready(Ok(count))
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut g = self.user_tx.locked.lock();

        if g.dead {
            return Poll::Ready(Err(std::io::Error::other("socket died")));
        }

        if g.buffer().is_empty() {
            return Poll::Ready(Ok(()));
        }

        update_optional_waker(&mut g.buffer_flushed, cx);

        Poll::Pending
    }

    fn poll_shutdown(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let flush_result = ready!(self.as_mut().poll_flush(cx));
        if let Err(e) = flush_result {
            return Poll::Ready(Err(e));
        }
        self.get_mut().close();
        Poll::Ready(Ok(()))
    }
}

#[cfg(test)]
mod tests {
    use crate::{
        raw::{selective_ack::SelectiveAck, UtpHeader},
        seq_nr::SeqNr,
    };

    use super::FragmentedTx;

    fn make_fragmented_tx(start_seq_nr: u16, count: u16) -> FragmentedTx {
        let mut ftx = FragmentedTx::new();

        let start: SeqNr = start_seq_nr.into();
        let end = start + count;
        for seq_nr in start.0..end.0 {
            assert!(ftx.enqueue(
                UtpHeader {
                    htype: crate::raw::Type::ST_DATA,
                    seq_nr: seq_nr.into(),
                    ..Default::default()
                },
                1,
            ));
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
    fn test_selective_ack_marking() {
        // Test Case 1: Basic setup with 4 packets starting from sequence number 3
        let mut ftx = make_fragmented_tx(3, 4);
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.1: ACK with sequence number 4, should remove first two packets
        let mut ftx = make_fragmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(4, []));
        assert_eq!(ftx.total_len_bytes(), 2);
        assert_eq!(ftx.total_len_packets(), 2);
        assert_eq!(ftx.first_seq_nr().unwrap(), 5.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 1.2: ACK all packets, queue should be empty
        let mut ftx = make_fragmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(6, []));
        assert_eq!(ftx.total_len_bytes(), 0);
        assert_eq!(ftx.total_len_packets(), 0);
        assert!(ftx.first_seq_nr().is_none());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 2: ACK with sequence number 2 (below our start sequence)
        // Should not remove any packets or mark any as delivered
        let mut ftx = make_fragmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(2, []));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 3: ACK with sequence number 3 (matches our start sequence)
        // Should remove the first packet, leaving 3 packets
        let mut ftx = make_fragmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(3, []));
        assert_eq!(ftx.total_len_bytes(), 3);
        assert_eq!(ftx.total_len_packets(), 3);
        assert_eq!(ftx.first_seq_nr().unwrap(), 4.into());
        assert_eq!(ftx.count_delivered_test(), 0);

        // Test Case 4: ACK with sequence number 2 and selective ACK for next packet
        // Should mark the second packet as delivered but not remove any
        let mut ftx = make_fragmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(2, [0]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 1);
        assert!(ftx.headers.get(1).unwrap().is_delivered);

        // Test Case 5: ACK with sequence number 2 and selective ACK for second and fourth packets
        // Should mark both packets as delivered but not remove any
        let mut ftx = make_fragmented_tx(3, 4);
        ftx.remove_up_to_ack(&make_sack_header(2, [0, 2]));
        assert_eq!(ftx.total_len_bytes(), 4);
        assert_eq!(ftx.total_len_packets(), 4);
        assert_eq!(ftx.first_seq_nr().unwrap(), 3.into());
        assert_eq!(ftx.count_delivered_test(), 2);
        assert!(ftx.headers.get(1).unwrap().is_delivered);
        assert!(ftx.headers.get(3).unwrap().is_delivered);

        // Test Case 6: Selective ACK with gaps
        let mut ftx = make_fragmented_tx(3, 6); // Create 6 packets
        ftx.remove_up_to_ack(&make_sack_header(2, [0, 2, 4])); // ACK 2nd, 4th, and 6th packets
        assert_eq!(ftx.total_len_bytes(), 6);
        assert_eq!(ftx.total_len_packets(), 6);
        assert_eq!(ftx.count_delivered_test(), 3);
        assert!(ftx.headers.get(1).unwrap().is_delivered);
        assert!(ftx.headers.get(3).unwrap().is_delivered);
        assert!(ftx.headers.get(5).unwrap().is_delivered);
        assert!(!ftx.headers.front().unwrap().is_delivered);
        assert!(!ftx.headers.get(2).unwrap().is_delivered);
        assert!(!ftx.headers.get(4).unwrap().is_delivered);
    }
}
