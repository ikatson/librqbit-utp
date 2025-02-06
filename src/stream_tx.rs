use std::{
    num::NonZeroUsize,
    sync::Arc,
    task::{ready, Poll, Waker},
};

use anyhow::bail;
use parking_lot::Mutex;
use ringbuf::{
    storage::Heap,
    traits::{Consumer, Observer, Producer},
};
use tokio::io::AsyncWrite;
use tracing::trace;

use crate::utils::{fill_buffer_from_slices, update_optional_waker};

pub struct UserTxLocked {
    // Set when stream dies abruptly for writer to know about it.
    pub dead: bool,
    // When the writer shuts down, or both reader and writer die, the stream is closed.
    closed: bool,

    buffer: ringbuf::LocalRb<Heap<u8>>,

    // This is woken up by dispatcher when the buffer has space if it didn't have it previously.
    pub buffer_has_space: Option<Waker>,
    // This is woken up by dispatcher when all outstanding packets where ACKed.
    pub buffer_flushed: Option<Waker>,

    // This is woken by by writer when it has put smth into the buffer.
    pub buffer_has_data: Option<Waker>,
}

impl UserTxLocked {
    pub fn truncate_front(&mut self, count: usize) -> anyhow::Result<()> {
        let skipped = self.buffer.skip(count);
        if skipped != count {
            bail!("bug: truncate_front: skipped({skipped}) != count({count})")
        }
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    // pub fn buffer(&self) -> &VecDeque<u8> {
    //     &self.buffer
    // }

    pub fn fill_buffer_from_ring_buffer(
        &self,
        out_buf: &mut [u8],
        offset: usize,
        len: usize,
    ) -> anyhow::Result<()> {
        let (first, second) = self.buffer.as_slices();
        fill_buffer_from_slices(out_buf, offset, len, first, second)
    }

    pub fn enqueue_slice(&mut self, bytes: &[u8]) -> usize {
        self.buffer.push_slice(bytes)
    }

    pub fn is_empty(&self) -> bool {
        self.buffer.is_empty()
    }

    pub fn len(&self) -> usize {
        self.buffer.occupied_len()
    }

    pub fn is_full(&self) -> bool {
        self.buffer.is_full()
    }

    // This will send FIN (if not yet).
    fn mark_stream_dead(&mut self) {
        self.dead = true;
        if let Some(waker) = self.buffer_has_space.take() {
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
                buffer: ringbuf::LocalRb::new(capacity.get()),
                buffer_has_space: None,
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

pub struct UtpStreamWriteHalf {
    user_tx: Arc<UserTx>,
    written_without_yield: u64,
}

impl UtpStreamWriteHalf {
    pub fn new(user_tx: Arc<UserTx>) -> Self {
        Self {
            user_tx,
            written_without_yield: 0,
        }
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
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        let this = &mut *self;

        // Yield sometimes to cooperate. 8192 is just an arbitrary number.
        const YIELD_EVERY: u64 = 8192;
        if this.written_without_yield > YIELD_EVERY {
            this.written_without_yield = 0;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let mut g = this.user_tx.locked.lock();

        if g.dead {
            return Poll::Ready(Err(std::io::Error::other("socket died")));
        }

        if g.closed {
            return Poll::Ready(Err(std::io::Error::other(
                "shutdown was initiated, can't write",
            )));
        }

        let count = g.enqueue_slice(buf);
        this.written_without_yield += count as u64;
        if count == 0 {
            assert!(g.is_full());
            update_optional_waker(&mut g.buffer_has_space, cx);
            this.written_without_yield = 0;
            return Poll::Pending;
        }

        if let Some(w) = g.buffer_has_data.take() {
            drop(g);
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

        if g.buffer.is_empty() {
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
