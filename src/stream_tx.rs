/// The user-facing writer side for uTP stream (UtpStreamWriteHalf).
/// The user calls AsyncWrite on it to write the data to the stream.
use std::{
    num::NonZeroUsize,
    sync::Arc,
    task::{Poll, Waker},
};

use parking_lot::{Mutex, RwLock};
use ringbuf::{
    SharedRb,
    storage::Heap,
    traits::{Consumer, Observer, Producer, Split},
};
use tokio::io::AsyncWrite;
use tracing::trace;

use crate::{Error, utils::update_optional_waker};

pub struct UserTxLocked {
    // Set when stream dies abruptly for writer to know about it.
    vsock_closed: bool,
    // When the writer drops this is set to true.
    writer_dropped: bool,

    // When the writer calls shutdown, this is set to true.
    writer_shutdown: bool,

    // Woken by by writer
    pub dispatcher_waker: Option<Waker>,
    pub writer_waker: Option<Waker>,
}

impl UserTxLocked {
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

/// The shared data between dispatcher and UtpStreamWriteHalf.
pub struct UserTx {
    pub locked: RwLock<UserTxLocked>,

    // They are shared and locked so that we can grow the buffers. Otherwise
    // producer would only go to the write half, and consumer to the vsock.
    pub producer: Mutex<Prod>,
    pub consumer: Mutex<Cons>,
}

impl UserTx {
    pub fn new(capacity: NonZeroUsize) -> Arc<Self> {
        let (prod, cons) = RingBuf::new(capacity.get()).split();
        Arc::new(UserTx {
            locked: RwLock::new(UserTxLocked {
                dispatcher_waker: None,
                writer_waker: None,
                vsock_closed: false,
                writer_dropped: false,
                writer_shutdown: false,
            }),
            producer: Mutex::new(prod),
            consumer: Mutex::new(cons),
        })
    }

    pub fn truncate_front(&self, count: usize) -> crate::Result<()> {
        let skipped = self.consumer.lock().skip(count);
        if skipped != count {
            return Err(Error::BugTruncateFront {
                skipped: try_shrink_or_neg!(skipped),
                count: try_shrink_or_neg!(count),
            });
        }
        Ok(())
    }

    /// Grow a ring buffer 2x up to max capacity.
    pub fn grow(&self, max_size: NonZeroUsize) -> Option<usize> {
        let mut cons = self.consumer.lock();

        let cap = cons.capacity().get();
        if cap >= max_size.get() {
            return None;
        }

        let mut prod = self.producer.lock();

        let new_cap = (cap * 2).min(max_size.get());
        let mut new_rb = RingBuf::new(new_cap);
        let (first, second) = cons.as_slices();
        new_rb.push_slice(first);
        new_rb.push_slice(second);

        let (new_prod, new_cons) = new_rb.split();
        *prod = new_prod;
        *cons = new_cons;
        Some(new_cap)
    }

    pub fn is_writer_dropped(&self) -> bool {
        self.locked.read().writer_dropped
    }

    pub fn is_writer_shutdown(&self) -> bool {
        self.locked.read().writer_shutdown
    }

    pub fn mark_vsock_closed(&self) {
        self.locked.write().mark_vsock_closed();
    }
}

type RingBuf = SharedRb<Heap<u8>>;
type Prod = <RingBuf as Split>::Prod;
type Cons = <RingBuf as Split>::Cons;

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
        self.user_tx.locked.write().mark_writer_dropped();
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

        let mut g = this.user_tx.locked.write();

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

        let count = this.user_tx.producer.lock().push_slice(buf);
        this.written_without_yield += count as u64;
        if count == 0 {
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
        let mut g = self.user_tx.locked.write();

        if self.user_tx.producer.lock().is_empty() {
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
        let mut g = self.user_tx.locked.write();
        if !self.user_tx.producer.lock().is_empty() {
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
