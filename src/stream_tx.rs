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

type RingBufferHeader = VecDeque<(UtpHeader, usize)>;

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
}

// The TX queue of the virtual socket. uTP doesn't refragemnt, so we store the original headers.
// The payloads are stored in the user TX behind a shared lock. Users write there with poll_write().
pub struct FragmentedTx {
    headers: RingBufferHeader,
    len_bytes: usize,
    capacity: usize,
}

pub struct TxIterItem<'a> {
    pub header: &'a UtpHeader,
    payload_size: usize,
    payload_offset: usize,
}

impl TxIterItem<'_> {
    pub fn header(&self) -> &UtpHeader {
        self.header
    }
    pub fn payload_size(&self) -> usize {
        self.payload_size
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
        Some(self.headers.front()?.0.seq_nr)
    }

    #[cfg(test)]
    pub fn total_len_packets(&self) -> usize {
        self.headers.len()
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
    pub fn enqueue(&mut self, h: UtpHeader, payload_size: usize) -> bool {
        if self.headers.len() == self.capacity {
            return false;
        }
        self.headers.push_back((h, payload_size));
        self.len_bytes += payload_size;
        true
    }

    #[allow(unused)]
    pub fn stats(&self) -> impl std::fmt::Debug {
        // this is for debugging only
        format!("headers: {}/{}", self.headers.len(), self.capacity)
    }

    // Returns number of removed headers, and their comibined payload size.
    pub fn remove_up_to_ack(&mut self, ack_nr: SeqNr) -> (usize, usize) {
        let mut removed = 0;
        let mut payload_size = 0;

        while let Some((header, ps)) = self.headers.pop_front() {
            if ack_nr < header.seq_nr {
                self.headers.push_front((header, ps));
                break;
            }

            removed += 1;
            payload_size += ps;
            self.len_bytes -= ps;
        }

        (removed, payload_size)
    }

    // Iterate stored data - headers and their payloads (as a function to copy payload to some other buffer).
    pub fn iter(&self) -> impl Iterator<Item = TxIterItem<'_>> {
        self.headers
            .iter()
            .scan(0, |offset, (header, payload_size)| {
                let current_offset = *offset;
                *offset += payload_size;
                Some(TxIterItem {
                    header,
                    payload_size: *payload_size,
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
