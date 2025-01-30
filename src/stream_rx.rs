use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use crate::{smoltcp_assembler::Assembler, utils::update_optional_waker, Payload};
use anyhow::{bail, Context};
use parking_lot::{Mutex, MutexGuard};
use tokio::{io::AsyncRead, sync::mpsc};
use tracing::{debug, trace, warn};

use crate::{
    message::UtpMessage, raw::selective_ack::SelectiveAck, stream_dispatch::UserRxMessage,
};

pub struct UtpStreamReadHalf {
    locked: Arc<Mutex<UserRxLocked>>,
}

impl UtpStreamReadHalf {
    pub fn new(locked: Arc<Mutex<UserRxLocked>>) -> Self {
        Self { locked }
    }
}

impl AsyncRead for UtpStreamReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let mut written = false;

        let mut g = self.locked.lock();

        while buf.remaining() > 0 {
            // If there was a previous message we haven't read till the end, do it.
            if let Some(current) = g.current.as_mut() {
                let payload = &current.payload[current.offset..];
                if payload.is_empty() {
                    return Poll::Ready(Err(std::io::Error::other(
                        "bug in UtpStreamReadHalf: payload is empty",
                    )));
                }

                let len = buf.remaining().min(payload.len());

                buf.put_slice(&payload[..len]);
                written = true;
                current.offset += len;
                if current.offset == current.payload.len() {
                    g.current = None;
                }
            }

            match g.queue.pop_front() {
                Some(UserRxMessage::Payload(payload)) => {
                    g.current = Some(BeingRead { payload, offset: 0 })
                }
                Some(UserRxMessage::Error(msg)) => {
                    return Poll::Ready(Err(std::io::Error::other(msg)))
                }
                Some(UserRxMessage::Eof) => return Poll::Ready(Ok(())),

                None => break,
            };
        }

        if written {
            return Poll::Ready(Ok(()));
        }

        if g.closed {
            return Poll::Ready(Err(std::io::Error::other("virtual socket closed")));
        }

        update_optional_waker(&mut g.queue_has_data, cx);
        Poll::Pending
    }
}

struct BeingRead {
    payload: Payload,
    offset: usize,
}

impl BeingRead {
    fn remaining(&self) -> usize {
        self.payload.len() - self.offset
    }
}

pub struct UserRxLocked {
    closed: bool,
    current: Option<BeingRead>,
    queue: VecDeque<UserRxMessage>,
    queue_has_data: Option<Waker>,

    queue_len_bytes: usize,
    capacity_bytes: usize,
}

impl UserRxLocked {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            closed: false,
            current: None,
            queue: Default::default(),
            queue_has_data: None,
            queue_len_bytes: 0,
            capacity_bytes,
        }
    }

    pub fn len(&self) -> usize {
        self.queue_len_bytes + self.current.as_ref().map_or(0, |c| c.remaining())
    }

    pub fn window(&self) -> usize {
        self.capacity_bytes - self.len()
    }

    pub fn is_full(&self) -> bool {
        self.window() == 0
    }

    // Returns back the message if there's no space.
    #[must_use]
    pub fn enqueue(&mut self, msg: UserRxMessage) -> Option<UserRxMessage> {
        let len = msg.len_bytes();
        if len < self.window() {
            return Some(msg);
        }
        self.queue.push_back(msg);
        if let Some(waker) = self.queue_has_data.take() {
            waker.wake();
        }
        None
    }
}

pub struct UserRx {
    locked: Arc<Mutex<UserRxLocked>>,
    out_of_order_queue: OutOfOrderQueue,
}

impl UserRx {
    pub fn new(locked: Arc<Mutex<UserRxLocked>>, out_of_order_max_packets: NonZeroUsize) -> Self {
        Self {
            locked,
            out_of_order_queue: OutOfOrderQueue::new(out_of_order_max_packets),
        }
    }

    pub fn window(&self) -> usize {
        let w = self.locked.lock().window();
        w.saturating_sub(self.out_of_order_queue.len_bytes())
    }

    pub fn flush(&mut self) -> anyhow::Result<usize> {
        self.out_of_order_queue.flush(&mut *self.locked.lock())
    }

    pub fn enqueue_last_message(&self, msg: UserRxMessage) {
        let mut g = self.locked.lock();
        todo!()
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        self.out_of_order_queue.selective_ack()
    }

    pub fn assembler_empty(&self) -> bool {
        self.out_of_order_queue.is_empty()
    }

    pub fn assembler_packets(&self) -> usize {
        self.out_of_order_queue.len()
    }

    pub fn add_remove(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> anyhow::Result<AssemblerAddRemoveResult> {
        match self.out_of_order_queue.add_remove(msg, offset)? {
            res @ AssemblerAddRemoveResult::ConsumedSequenceNumbers(..) => {
                self.flush();
                Ok(res)
            }
            res => Ok(res),
        }
    }
}

struct OutOfOrderQueue {
    out_of_order_queue: VecDeque<Payload>,
    filled_front: usize,
    len: usize,
    len_bytes: usize,
    capacity: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AssemblerAddRemoveResult {
    ConsumedSequenceNumbers(usize),
    Unavailable(UtpMessage),
}

impl OutOfOrderQueue {
    pub fn new(capacity: NonZeroUsize) -> Self {
        Self {
            out_of_order_queue: VecDeque::from(vec![Default::default(); capacity.get()]),
            filled_front: 0,
            len: 0,
            len_bytes: 0,
            capacity: capacity.get(),
        }
    }

    pub fn len_bytes(&self) -> usize {
        self.len_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    pub fn is_full(&self) -> bool {
        self.len == self.capacity
    }

    pub fn len(&self) -> usize {
        self.len
    }

    pub fn debug_string(&self, with_data: bool) -> impl std::fmt::Display + '_ {
        struct D<'a> {
            q: &'a OutOfOrderQueue,
            with_data: bool,
        }
        impl std::fmt::Display for D<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "len={}, len_bytes={}", self.q.len(), self.q.len_bytes(),)?;

                if !self.with_data {
                    return Ok(());
                }

                write!(f, ", queue={:?}", self.q.out_of_order_queue)?;
                Ok(())
            }
        }
        D { q: self, with_data }
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        // TODO
        None
    }

    fn flush(&mut self, user_rx: &mut UserRxLocked) -> anyhow::Result<usize> {
        // Flush as many items as possible from the beginning of out of order queue to the user RX
        let mut total_bytes = 0;
        while self.filled_front > 0 && self.out_of_order_queue[0].len() > user_rx.window() {
            let msg = self
                .out_of_order_queue
                .pop_front()
                .context("bug: should have popped")?;
            self.filled_front -= 1;
            self.len -= 1;
            self.len_bytes -= msg.len();
            self.out_of_order_queue.push_back(Default::default());
            total_bytes += 1;
            user_rx
                .enqueue(UserRxMessage::Payload(msg))
                .context("bug: should have enqueued")?;
        }
        Ok(total_bytes)
    }

    fn add_remove(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> anyhow::Result<AssemblerAddRemoveResult> {
        if self.is_full() {
            trace!(offset, "assembler buffer full");
            return Ok(AssemblerAddRemoveResult::Unavailable(msg));
        }

        let effective_offset = offset + self.filled_front;

        if effective_offset > self.out_of_order_queue.len() {
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

        let (_, payload) = msg.consume();

        let slot = self
            .out_of_order_queue
            .get_mut(effective_offset)
            .context("bug: slot should be there")?;
        if !slot.is_empty() {
            bail!("bug: slot had payload")
        }

        self.len += 1;
        self.len_bytes += payload.len();
        *slot = payload;

        let range = self.filled_front..self.out_of_order_queue.len();
        // Advance "filled" if a contiguous data range was found.
        let contiguous = self
            .out_of_order_queue
            .range(range)
            .take_while(|payload| !payload.is_empty())
            .count();
        self.filled_front += contiguous;
        Ok(AssemblerAddRemoveResult::ConsumedSequenceNumbers(
            contiguous,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use tokio::sync::mpsc::channel;
    use tracing::trace;

    use crate::{
        message::UtpMessage,
        stream_dispatch::UserRxMessage,
        stream_rx::{AssemblerAddRemoveResult, OutOfOrderQueue},
        test_util::setup_test_logging,
    };

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

    #[test]
    fn test_asm_add_one_in_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(0, b""), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
        );
    }

    #[test]
    fn test_asm_add_one_out_of_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(0, b""), 1).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );
    }

    #[test]
    fn test_asm_channel_full_asm_empty() {
        let (tx, _rx) = channel(1);
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        let msg = msg(0, b"");
        // fill the channel
        tx.try_send(crate::stream_dispatch::UserRxMessage::UtpMessage(
            msg.clone(),
        ))
        .unwrap();
        assert_eq!(
            asm.add_remove(msg.clone(), 0, &tx).unwrap(),
            AssemblerAddRemoveResult::Unavailable(msg)
        );
        assert!(asm.assembler.is_empty());
    }

    #[test]
    fn test_asm_channel_full_asm_not_empty() {
        let (tx, _rx) = channel(1);
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        let msg = msg(0, b"");
        // fill the channel
        tx.try_send(crate::stream_dispatch::UserRxMessage::UtpMessage(
            msg.clone(),
        ))
        .unwrap();

        assert_eq!(
            asm.add_remove(msg.clone(), 1, &tx).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );
        assert_eq!(asm.len(), 1);

        assert_eq!(
            asm.add_remove(msg.clone(), 0, &tx).unwrap(),
            AssemblerAddRemoveResult::Unavailable(msg)
        );

        // TODO: we now drop the message in this case. Better would be to behave differently.
        // Fill in the buffer, send ACKs etc.
        assert_eq!(asm.len(), 1);
    }

    #[test]
    fn test_asm_out_of_order() {
        setup_test_logging();

        let (tx, mut rx) = channel(3);
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(3).unwrap());

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            asm.add_remove(msg_1.clone(), 1).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 1);

        assert_eq!(
            asm.add_remove(msg_2.clone(), 2).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );
        trace!(asm=%asm.debug_string(true));

        assert_eq!(
            asm.add_remove(msg_0.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(3)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 0);

        assert_eq!(
            rx.try_recv().unwrap(),
            UserRxMessage::Payload(msg_0.into_payload())
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            UserRxMessage::Payload(msg_1.into_payload())
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            UserRxMessage::Payload(msg_2.into_payload())
        );
    }

    #[test]
    fn test_asm_inorder() {
        setup_test_logging();
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(3).unwrap());

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            asm.add_remove(msg_0.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 0);

        assert_eq!(
            asm.add_remove(msg_1.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
        );
        trace!(asm=%asm.debug_string(true));

        assert_eq!(
            asm.add_remove(msg_2.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 0);

        assert_eq!(
            rx.try_recv().unwrap(),
            UserRxMessage::Payload(msg_0.into_payload())
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            UserRxMessage::Payload(msg_1.into_payload())
        );
        assert_eq!(
            rx.try_recv().unwrap(),
            UserRxMessage::Payload(msg_2.into_payload())
        );
    }

    #[test]
    fn test_asm_write_out_of_bounds() {
        setup_test_logging();

        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(3).unwrap());

        let msg_2 = msg(2, b"test");
        let msg_3 = msg(3, b"test");

        assert_eq!(
            asm.add_remove(msg_2.clone(), 2).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 1);

        // A message that is out of bounds of the assembler should be dropped.
        assert_eq!(
            asm.add_remove(msg_3.clone(), 3).unwrap(),
            AssemblerAddRemoveResult::Unavailable(msg_3)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 1);
    }
}
