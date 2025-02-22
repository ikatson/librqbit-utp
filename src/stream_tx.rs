use std::{
    num::NonZeroUsize,
    sync::Arc,
    task::{Poll, Waker},
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
    vsock_closed: bool,
    // When the writer drops this is set to true.
    writer_dropped: bool,

    // When the writer calls shutdown, this is set to true.
    writer_shutdown: bool,

    buffer: ringbuf::LocalRb<Heap<u8>>,

    // Woken by by writer
    pub dispatcher_waker: Option<Waker>,
    pub writer_waker: Option<Waker>,
}

impl UserTxLocked {
    pub fn truncate_front(&mut self, count: usize) -> anyhow::Result<()> {
        let skipped = self.buffer.skip(count);
        if skipped != count {
            bail!("bug: truncate_front: skipped({skipped}) != count({count})")
        }
        Ok(())
    }

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

    fn mark_vsock_closed(&mut self) {
        self.vsock_closed = true;
        if let Some(waker) = self.writer_waker.take() {
            waker.wake();
        }
    }

    fn mark_writer_dropped(&mut self) -> bool {
        if !self.writer_dropped {
            trace!("closing writer");
            self.writer_dropped = true;
            if let Some(w) = self.dispatcher_waker.take() {
                w.wake();
            }
            true
        } else {
            false
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
                dispatcher_waker: None,
                writer_waker: None,
                vsock_closed: false,
                writer_dropped: false,
                writer_shutdown: false,
            }),
        })
    }

    pub fn is_writer_dropped(&self) -> bool {
        self.locked.lock().writer_dropped
    }

    pub fn is_writer_shutdown(&self) -> bool {
        self.locked.lock().writer_shutdown
    }

    pub fn mark_vsock_closed(&self) {
        self.locked.lock().mark_vsock_closed();
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
}

impl Drop for UtpStreamWriteHalf {
    fn drop(&mut self) {
        self.user_tx.locked.lock().mark_writer_dropped();
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

        if g.vsock_closed {
            return Poll::Ready(Err(std::io::Error::other("socket closed")));
        }

        if g.writer_shutdown {
            return Poll::Ready(Err(std::io::Error::other("no writing after shutdown")));
        }

        if g.writer_dropped {
            return Poll::Ready(Err(std::io::Error::other(
                "shutdown was initiated, can't write",
            )));
        }

        let count = g.enqueue_slice(buf);
        this.written_without_yield += count as u64;
        if count == 0 {
            assert!(g.is_full());
            update_optional_waker(&mut g.writer_waker, cx);
            this.written_without_yield = 0;
            return Poll::Pending;
        }

        if let Some(w) = g.dispatcher_waker.take() {
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

        if g.buffer.is_empty() {
            return Poll::Ready(Ok(()));
        }

        if g.vsock_closed {
            return Poll::Ready(Err(std::io::Error::other("socket died")));
        }

        update_optional_waker(&mut g.writer_waker, cx);

        Poll::Pending
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        let mut g = self.user_tx.locked.lock();
        if !g.buffer.is_empty() {
            if g.vsock_closed {
                return Poll::Ready(Err(std::io::Error::other("socket died")));
            }

            update_optional_waker(&mut g.writer_waker, cx);
            return Poll::Pending;
        }

        if g.vsock_closed {
            return Poll::Ready(Ok(()));
        }

        g.writer_shutdown = true;
        update_optional_waker(&mut g.writer_waker, cx);
        Poll::Pending
    }
}
