use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use anyhow::{bail, Context};
use parking_lot::Mutex;
use tokio::io::AsyncRead;
use tracing::{trace, warn};

use crate::{
    message::UtpMessage, raw::selective_ack::SelectiveAck, stream_dispatch::UserRxMessage,
    utils::update_optional_waker,
};

pub struct UtpStreamReadHalf {
    current: Option<BeingRead>,
    queue: tokio::sync::mpsc::UnboundedReceiver<UserRxMessage>,
    shared: Arc<UserRxShared>,
}

impl UtpStreamReadHalf {
    #[cfg(test)]
    pub async fn read_all_available(&mut self) -> std::io::Result<Vec<u8>> {
        use std::{future::poll_fn, task::ready};

        use futures::FutureExt;
        use tokio::io::AsyncReadExt;

        let mut buf = vec![0u8; 2 * 1024 * 1024];
        let mut offset = 0;
        poll_fn(|cx| {
            if self.queue.is_empty() {
                buf.truncate(offset);
                return Poll::Ready(Ok(()));
            }

            loop {
                let read = self.read(&mut buf[offset..]);
                tokio::pin!(read);
                match ready!(read.poll_unpin(cx)) {
                    Ok(len) => {
                        offset += len;

                        if self.queue.is_empty() {
                            return Poll::Ready(Ok(()));
                        }
                    }
                    Err(e) => {
                        return Poll::Ready(Err(e));
                    }
                }
            }
        })
        .await?;
        buf.truncate(offset);
        Ok(buf)
    }
}

// Dispatcher owns mut UserRx {shared, out_of_order_queue, tx}
// Client owns UtpStreamReadHalf {shared, rx}
//
// TODO: implement flow control

impl AsyncRead for UtpStreamReadHalf {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut written = 0usize;

        while buf.remaining() > 0 {
            // If there was a previous message we haven't read till the end, do it.
            if let Some(current) = self.current.as_mut() {
                let payload = &current.msg.payload()[current.offset..];
                if payload.is_empty() {
                    return Poll::Ready(Err(std::io::Error::other(
                        "bug in UtpStreamReadHalf: payload is empty",
                    )));
                }

                let len = buf.remaining().min(payload.len());

                buf.put_slice(&payload[..len]);
                written += len;
                current.offset += len;
                if current.offset == current.msg.payload().len() {
                    self.current = None;
                }
                continue;
            }

            match self.queue.poll_recv(cx) {
                Poll::Ready(Some(UserRxMessage::Payload(msg))) => {
                    self.current = Some(BeingRead { msg, offset: 0 })
                }
                Poll::Ready(Some(UserRxMessage::Error(msg))) => {
                    return Poll::Ready(Err(std::io::Error::other(msg)))
                }
                Poll::Ready(Some(UserRxMessage::Eof)) => return Poll::Ready(Ok(())),
                Poll::Ready(None) => {
                    // This can happen only if dispatcher died and didn't send anything to us.
                    return Poll::Ready(Err(std::io::Error::other("dispatcher dead")));
                }
                Poll::Pending => {
                    break;
                }
            };
        }

        if written > 0 {
            let mut g = self.shared.locked.lock();
            g.len_bytes -= written;
            if let Some(waker) = g.flush_waker.take() {
                waker.wake();
            }
            return Poll::Ready(Ok(()));
        }

        Poll::Pending
    }
}

struct BeingRead {
    msg: UtpMessage,
    offset: usize,
}

struct UserRxSharedLocked {
    len_bytes: usize,
    flush_waker: Option<Waker>,
}

struct UserRxShared {
    locked: Mutex<UserRxSharedLocked>,
    capacity: usize,
}

impl UserRxShared {
    #[cfg(test)]
    pub fn is_full_test(&self) -> bool {
        self.locked.lock().len_bytes >= self.capacity
    }
}

pub struct UserRx {
    shared: Arc<UserRxShared>,
    tx: tokio::sync::mpsc::UnboundedSender<UserRxMessage>,
    ooq: OutOfOrderQueue,
    max_incoming_payload: NonZeroUsize,
    last_remaining_rx_window: usize,
}

impl UserRx {
    pub fn build(
        max_rx_bytes: NonZeroUsize,
        out_of_order_max_packets: NonZeroUsize,
        max_incoming_payload: NonZeroUsize,
    ) -> (UserRx, UtpStreamReadHalf) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let shared = Arc::new(UserRxShared {
            locked: Mutex::new(UserRxSharedLocked {
                len_bytes: 0,
                flush_waker: None,
            }),
            capacity: max_rx_bytes.get(),
        });
        let read_half = UtpStreamReadHalf {
            current: None,
            queue: rx,
            shared: shared.clone(),
        };
        let out_of_order_queue = OutOfOrderQueue::new(out_of_order_max_packets);
        let write_half = UserRx {
            shared,
            tx,
            ooq: out_of_order_queue,
            max_incoming_payload,
            last_remaining_rx_window: max_rx_bytes.get(),
        };
        (write_half, read_half)
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }

    pub fn remaining_rx_window(&self) -> usize {
        if self.tx.is_closed() {
            0
        } else {
            self.last_remaining_rx_window
                .saturating_sub(self.ooq.stored_bytes())
        }
    }

    pub fn mark_stream_dead(&self) {
        // nothing here
        // keeping this function to be consistent with UserTx
    }

    pub fn flush(&mut self, cx: &mut std::task::Context<'_>) -> anyhow::Result<usize> {
        let filled_front_bytes: usize = self.ooq.filled_front_bytes();
        let mut remaining_rx_window = {
            let mut g = self.shared.locked.lock();
            let remaining_window = self.shared.capacity.saturating_sub(g.len_bytes);
            if remaining_window.saturating_sub(filled_front_bytes) < self.max_incoming_payload.get()
            {
                update_optional_waker(&mut g.flush_waker, cx);
            }
            remaining_window
        };

        // Flush as many items as possible from the beginning of out of order queue to the user RX
        let mut flushed_bytes = 0;
        let mut flushed_packets = 0;

        while let Some(len) = self.ooq.send_front_if_fits(remaining_rx_window, |msg| {
            self.tx.send(UserRxMessage::Payload(msg)).map_err(|e| {
                debug_every_ms!(200, "reader is dead, could not send UtpMesage to it");
                match e.0 {
                    UserRxMessage::Payload(msg) => msg,
                    _ => unreachable!(),
                }
            })
        }) {
            flushed_bytes += len;
            remaining_rx_window -= len;
            flushed_packets += 1;
        }

        if flushed_bytes > 0 {
            self.shared.locked.lock().len_bytes += flushed_bytes;
            trace!(
                packets = flushed_packets,
                bytes = flushed_bytes,
                "flushed from out-of-order user RX"
            );
        }

        if self.ooq.filled_front > 0 {
            trace!(
                flushed_bytes,
                flushed_packets,
                out_of_order_filled_front = self.ooq.filled_front,
                remaining_rx_window,
                "did not flush everything"
            );
        }

        self.last_remaining_rx_window = remaining_rx_window;
        Ok(flushed_bytes)
    }

    pub fn enqueue_last_message(&self, msg: UserRxMessage) {
        let _ = self.tx.send(msg);
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        self.ooq.selective_ack()
    }

    #[cfg(test)]
    pub fn len_test(&self) -> usize {
        self.shared.locked.lock().len_bytes
    }

    pub fn assembler_empty(&self) -> bool {
        self.ooq.is_empty()
    }

    #[cfg(test)]
    pub fn assembler_packets(&self) -> usize {
        self.ooq.stored_packets()
    }

    pub fn add_remove(
        &mut self,
        cx: &mut std::task::Context<'_>,
        msg: UtpMessage,
        offset: usize,
    ) -> anyhow::Result<AssemblerAddRemoveResult> {
        match self.ooq.add_remove(msg, offset)? {
            res @ AssemblerAddRemoveResult::Consumed {
                sequence_numbers, ..
            } if sequence_numbers > 0 => {
                // TODO: we shouldn't flush on every single message, but rather should do it after a certain threshold.
                self.flush(cx)?;
                Ok(res)
            }
            res => Ok(res),
        }
    }

    #[cfg(test)]
    async fn add_remove_test(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> anyhow::Result<AssemblerAddRemoveResult> {
        let mut msg = Some(msg);
        let msg = &mut msg;
        std::future::poll_fn(move |cx| {
            let res = self.add_remove(cx, msg.take().unwrap(), offset);
            Poll::Ready(res)
        })
        .await
    }

    #[cfg(test)]
    pub fn is_flush_waker_registered(&self) -> bool {
        self.shared.locked.lock().flush_waker.is_some()
    }

    #[cfg(test)]
    fn enqueue_test(&self, msg: UserRxMessage) {
        let mut g = self.shared.locked.lock();
        let win = self.shared.capacity.saturating_sub(g.len_bytes);
        let msglen = msg.len_bytes_test();
        if msglen < win {
            panic!("not enough space")
        }

        self.tx.send(msg).unwrap();
        g.len_bytes += msglen;
    }
}

pub struct OutOfOrderQueue {
    data: VecDeque<UtpMessage>,
    filled_front: usize,
    len: usize,
    len_bytes: usize,
    capacity: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AssemblerAddRemoveResult {
    Consumed {
        sequence_numbers: usize,
        bytes: usize,
    },
    Unavailable(UtpMessage),
}

impl OutOfOrderQueue {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            data: VecDeque::from(vec![Default::default(); capacity.get()]),
            filled_front: 0,
            len: 0,
            len_bytes: 0,
            capacity: capacity.get(),
        }
    }

    fn filled_front_bytes(&self) -> usize {
        self.data
            .iter()
            .take(self.filled_front)
            .map(|m| m.payload().len())
            .sum()
    }

    pub fn send_front_if_fits(
        &mut self,
        window: usize,
        send_fn: impl FnOnce(UtpMessage) -> Result<(), UtpMessage>,
    ) -> Option<usize> {
        if self.filled_front == 0 {
            return None;
        }
        if self.data[0].payload().len() > window {
            return None;
        }
        let msg = self.data.pop_front()?;
        let len = msg.payload().len();
        match send_fn(msg) {
            Ok(()) => {}
            Err(msg) => {
                self.data.push_front(msg);
                return None;
            }
        }
        self.filled_front -= 1;
        self.len -= 1;
        self.len_bytes -= len;
        self.data.push_back(Default::default());
        Some(len)
    }

    pub fn is_empty(&self) -> bool {
        self.filled_front == self.len
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    #[cfg(test)]
    fn filled_front(&self) -> usize {
        self.filled_front
    }

    #[cfg(test)]
    fn stored_packets(&self) -> usize {
        self.len
    }

    fn stored_bytes(&self) -> usize {
        self.len_bytes
    }

    #[cfg(test)]
    fn debug_string(&self, with_data: bool) -> impl std::fmt::Display + '_ {
        struct D<'a> {
            q: &'a OutOfOrderQueue,
            with_data: bool,
        }
        impl std::fmt::Display for D<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(
                    f,
                    "len={}, len_bytes={}",
                    self.q.stored_packets(),
                    self.q.stored_bytes(),
                )?;

                if !self.with_data {
                    return Ok(());
                }

                write!(f, ", queue={:?}", self.q.data)?;
                Ok(())
            }
        }
        D { q: self, with_data }
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        if self.is_empty() {
            return None;
        }

        let start = self.filled_front + 1;
        if start >= self.data.len() {
            return None;
        }
        let unacked = self
            .data
            .range(start..)
            .enumerate()
            .filter_map(|(idx, data)| {
                if data.payload().is_empty() {
                    None
                } else {
                    Some(idx)
                }
            });

        SelectiveAck::new(unacked)
    }

    pub fn add_remove(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> anyhow::Result<AssemblerAddRemoveResult> {
        if self.is_full() {
            trace!(offset, "assembler buffer full");
            return Ok(AssemblerAddRemoveResult::Unavailable(msg));
        }

        let effective_offset = offset + self.filled_front;

        if effective_offset >= self.data.len() {
            trace!(
                offset,
                self.filled_front,
                effective_offset,
                "message is past assembler's window"
            );
            return Ok(AssemblerAddRemoveResult::Unavailable(msg));
        }

        if msg.payload().is_empty() {
            warn!("empty payload unsupported");
            return Ok(AssemblerAddRemoveResult::Unavailable(msg));
        }

        let slot = self
            .data
            .get_mut(effective_offset)
            .context("bug: slot should be there")?;
        if !slot.payload().is_empty() {
            bail!("bug: slot had payload")
        }

        self.len += 1;
        self.len_bytes += msg.payload().len();
        *slot = msg;

        let range = self.filled_front..self.data.len();
        // Advance "filled" if a contiguous data range was found.
        let (consumed_segments, consumed_bytes) = self
            .data
            .range(range)
            .take_while(|msg| !msg.payload().is_empty())
            .fold((0, 0), |mut state, msg| {
                state.0 += 1;
                state.1 += msg.payload().len();
                state
            });
        self.filled_front += consumed_segments;
        Ok(AssemblerAddRemoveResult::Consumed {
            sequence_numbers: consumed_segments,
            bytes: consumed_bytes,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tokio::io::AsyncReadExt;
    use tracing::trace;

    use crate::{
        message::UtpMessage,
        stream_dispatch::UserRxMessage,
        stream_rx::{AssemblerAddRemoveResult, OutOfOrderQueue},
        test_util::setup_test_logging,
    };

    use super::{UserRx, UtpStreamReadHalf};

    fn msg(seq_nr: u16, payload: &[u8]) -> UtpMessage {
        UtpMessage::new_test(
            crate::raw::UtpHeader {
                htype: crate::raw::Type::ST_DATA,
                seq_nr: seq_nr.into(),
                ..Default::default()
            },
            payload,
        )
    }

    fn user_rx(
        capacity_bytes: usize,
        out_of_order_max_packets: usize,
    ) -> (UserRx, UtpStreamReadHalf) {
        UserRx::build(
            NonZeroUsize::new(capacity_bytes).unwrap(),
            NonZeroUsize::new(out_of_order_max_packets).unwrap(),
            // TODO
            NonZeroUsize::new(1500).unwrap(),
        )
    }

    #[test]
    fn test_asm_add_one_in_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(0, b"a"), 0).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 1
            }
        );
        assert_eq!(asm.stored_packets(), 1);
        assert_eq!(asm.stored_bytes(), 1);
        assert_eq!(asm.filled_front(), 1);
    }

    #[test]
    fn test_asm_add_one_out_of_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(100, b"a"), 1).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        assert_eq!(asm.stored_packets(), 1);
        assert_eq!(asm.stored_bytes(), 1);
        assert_eq!(asm.filled_front(), 0);
    }

    #[tokio::test]
    async fn test_asm_channel_full_asm_empty() {
        setup_test_logging();
        let (mut user_rx, _read) = user_rx(1, 2);
        let msg = msg(0, b"a");

        // fill RX
        user_rx.enqueue_test(UserRxMessage::Payload(msg.clone()));

        assert!(user_rx.shared.is_full_test());

        assert_eq!(
            user_rx.add_remove_test(msg.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 1
            }
        );
        assert_eq!(user_rx.ooq.stored_packets(), 1);
        assert_eq!(user_rx.ooq.stored_bytes(), 1);
        assert_eq!(user_rx.ooq.filled_front(), 1);
    }

    #[tokio::test]
    async fn test_asm_channel_full_asm_not_empty() {
        let (mut user_rx, _read) = user_rx(1, 2);
        let msg = msg(0, b"a");

        // fill RX
        user_rx.enqueue_test(UserRxMessage::Payload(msg.clone()));

        assert_eq!(
            user_rx.add_remove_test(msg.clone(), 1).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );

        assert_eq!(user_rx.ooq.stored_packets(), 1);
        assert_eq!(user_rx.ooq.stored_bytes(), 1);
        assert_eq!(user_rx.ooq.filled_front(), 0);

        assert_eq!(
            user_rx.add_remove_test(msg.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 2,
                bytes: 2
            }
        );
        assert_eq!(user_rx.ooq.stored_packets(), 2);
        assert_eq!(user_rx.ooq.stored_bytes(), 2);
        assert_eq!(user_rx.ooq.filled_front(), 2);
    }

    #[tokio::test]
    async fn test_asm_out_of_order() {
        setup_test_logging();

        let (mut user_rx, mut read) = user_rx(100, 3);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            user_rx.add_remove_test(msg_1.clone(), 1).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 1);

        assert_eq!(
            user_rx.add_remove_test(msg_2.clone(), 2).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));

        assert_eq!(
            user_rx.add_remove_test(msg_0.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 3,
                bytes: 14
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 0);

        let mut buf = [0u8; 1024];
        let sz = read.read(&mut buf).await.unwrap();
        assert_eq!(std::str::from_utf8(&buf[..sz]), Ok("helloworldtest"));
    }

    #[tokio::test]
    async fn test_asm_inorder() {
        setup_test_logging();
        let (mut user_rx, mut read) = user_rx(100, 3);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            user_rx.add_remove_test(msg_0.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 5
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 0);

        assert_eq!(
            user_rx.add_remove_test(msg_1.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 5
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));

        assert_eq!(
            user_rx.add_remove_test(msg_2.clone(), 0).await.unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 1,
                bytes: 4
            }
        );
        trace!(asm=%user_rx.ooq.debug_string(true));
        assert_eq!(user_rx.ooq.stored_packets(), 0);

        let mut buf = [0u8; 1024];
        let sz = read.read(&mut buf).await.unwrap();
        assert_eq!(std::str::from_utf8(&buf[..sz]), Ok("helloworldtest"));
    }

    #[test]
    fn test_asm_write_out_of_bounds() {
        setup_test_logging();

        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(3).unwrap());

        let msg_2 = msg(2, b"test");
        let msg_3 = msg(3, b"test");

        assert_eq!(
            asm.add_remove(msg_2.clone(), 2).unwrap(),
            AssemblerAddRemoveResult::Consumed {
                sequence_numbers: 0,
                bytes: 0
            }
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.stored_packets(), 1);

        // A message that is out of bounds of the assembler should be dropped.
        assert_eq!(
            asm.add_remove(msg_3.clone(), 3).unwrap(),
            AssemblerAddRemoveResult::Unavailable(msg_3)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.stored_packets(), 1);
    }
}
