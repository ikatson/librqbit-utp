use std::{
    collections::VecDeque,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Poll, Waker},
};

use crate::{utils::update_optional_waker, Payload};
use anyhow::{bail, Context};
use parking_lot::Mutex;
use tokio::io::AsyncRead;
use tracing::{trace, warn};

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

impl Drop for UtpStreamReadHalf {
    fn drop(&mut self) {
        self.locked.lock().close();
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
                    g.queue_len_bytes -= payload.len();
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
    dead: bool,
    current: Option<BeingRead>,
    queue: VecDeque<UserRxMessage>,

    // Woken by dispatcher
    queue_has_data: Option<Waker>,

    dispatcher: Option<Waker>,

    queue_len_bytes: usize,
    capacity_bytes: usize,
}

impl UserRxLocked {
    pub fn new(capacity_bytes: usize) -> Self {
        Self {
            closed: false,
            dead: false,
            current: None,
            queue: Default::default(),
            queue_has_data: None,
            queue_len_bytes: 0,
            dispatcher: None,
            capacity_bytes,
        }
    }

    fn close(&mut self) {
        if !self.closed {
            trace!("closing reader");
            self.closed = true;
            if let Some(w) = self.dispatcher.take() {
                w.wake();
            }
        }
    }

    pub fn len(&self) -> usize {
        self.queue_len_bytes + self.current.as_ref().map_or(0, |c| c.remaining())
    }

    pub fn window(&self) -> usize {
        self.capacity_bytes - self.len()
    }

    #[cfg(test)]
    pub fn is_full(&self) -> bool {
        self.window() == 0
    }

    // Returns back the message if there's no space.
    pub fn enqueue(&mut self, msg: UserRxMessage) -> Result<(), UserRxMessage> {
        let len = msg.len_bytes();
        if len > self.window() {
            return Err(msg);
        }
        self.queue.push_back(msg);
        self.queue_len_bytes += len;
        if let Some(waker) = self.queue_has_data.take() {
            waker.wake();
        }
        Ok(())
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

    pub fn is_closed(&self) -> bool {
        self.locked.lock().closed
    }

    pub fn mark_stream_dead(&self) {
        self.locked.lock().dead = true;
    }

    pub fn window(&self) -> usize {
        let w = self.locked.lock().window();
        w.saturating_sub(self.out_of_order_queue.stored_bytes())
    }

    pub fn flush(&mut self) -> anyhow::Result<usize> {
        self.out_of_order_queue.flush(&mut self.locked.lock())
    }

    pub fn enqueue_last_message(&self, msg: UserRxMessage) {
        let mut g = self.locked.lock();
        g.queue.push_back(msg);
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        self.out_of_order_queue.selective_ack()
    }

    pub fn assembler_empty(&self) -> bool {
        self.out_of_order_queue.is_empty()
    }

    #[cfg(test)]
    pub fn assembler_packets(&self) -> usize {
        self.out_of_order_queue.stored_packets()
    }

    pub fn add_remove(
        &mut self,
        msg: UtpMessage,
        offset: usize,
    ) -> anyhow::Result<AssemblerAddRemoveResult> {
        match self.out_of_order_queue.add_remove(msg, offset)? {
            res @ AssemblerAddRemoveResult::ConsumedSequenceNumbers(..) => {
                self.flush()?;
                Ok(res)
            }
            res => Ok(res),
        }
    }
}

pub struct OutOfOrderQueue {
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

                write!(f, ", queue={:?}", self.q.out_of_order_queue)?;
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
        if start >= self.out_of_order_queue.len() {
            return None;
        }
        let unacked = self
            .out_of_order_queue
            .range(start..)
            .enumerate()
            .filter_map(|(idx, data)| if data.is_empty() { None } else { Some(idx) });

        SelectiveAck::new(unacked)
    }

    fn flush(&mut self, user_rx: &mut UserRxLocked) -> anyhow::Result<usize> {
        // Flush as many items as possible from the beginning of out of order queue to the user RX
        let mut total_bytes = 0;
        let mut total_packets = 0;
        while self.filled_front > 0 && self.out_of_order_queue[0].len() <= user_rx.window() {
            let msg = self
                .out_of_order_queue
                .pop_front()
                .context("bug: should have popped")?;
            self.filled_front -= 1;
            self.len -= 1;
            self.len_bytes -= msg.len();
            self.out_of_order_queue.push_back(Default::default());
            total_bytes += msg.len();
            total_packets += 1;
            if user_rx.enqueue(UserRxMessage::Payload(msg)).is_err() {
                bail!("bug: should have enqueued")
            };
        }
        if total_bytes > 0 {
            trace!(
                packets = total_packets,
                bytes = total_bytes,
                "flushed from out-of-order user RX"
            );
        }

        Ok(total_bytes)
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

        if effective_offset >= self.out_of_order_queue.len() {
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
    use std::{num::NonZeroUsize, sync::Arc};

    use parking_lot::Mutex;
    use tracing::trace;

    use crate::{
        message::UtpMessage,
        stream_dispatch::UserRxMessage,
        stream_rx::{AssemblerAddRemoveResult, OutOfOrderQueue},
        test_util::setup_test_logging,
    };

    use super::{UserRx, UserRxLocked};

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

    fn user_rx(capacity_bytes: usize, out_of_order_max_packets: usize) -> UserRx {
        let locked = Arc::new(Mutex::new(UserRxLocked::new(capacity_bytes)));
        UserRx::new(locked, NonZeroUsize::new(out_of_order_max_packets).unwrap())
    }

    #[test]
    fn test_asm_add_one_in_order() {
        let mut asm = OutOfOrderQueue::new(NonZeroUsize::new(2).unwrap());
        assert_eq!(
            asm.add_remove(msg(0, b"a"), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
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
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );
        assert_eq!(asm.stored_packets(), 1);
        assert_eq!(asm.stored_bytes(), 1);
        assert_eq!(asm.filled_front(), 0);
    }

    #[test]
    fn test_asm_channel_full_asm_empty() {
        setup_test_logging();
        let mut user_rx = user_rx(1, 2);
        let msg = msg(0, b"a");

        {
            let rx = user_rx.locked.lock();
            trace!(
                rx.capacity_bytes,
                rx.queue_len_bytes,
                len = rx.len(),
                win = rx.window()
            )
        }

        // fill RX
        user_rx
            .locked
            .lock()
            .enqueue(UserRxMessage::Payload(b"a".to_vec()))
            .unwrap();

        assert!(user_rx.locked.lock().is_full());

        assert_eq!(
            user_rx.add_remove(msg.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
        );
        assert_eq!(user_rx.out_of_order_queue.stored_packets(), 1);
        assert_eq!(user_rx.out_of_order_queue.stored_bytes(), 1);
        assert_eq!(user_rx.out_of_order_queue.filled_front(), 1);
    }

    #[test]
    fn test_asm_channel_full_asm_not_empty() {
        let mut user_rx = user_rx(1, 2);
        let msg = msg(0, b"a");

        // fill RX
        user_rx
            .locked
            .lock()
            .enqueue(UserRxMessage::Payload(b"a".to_vec()))
            .unwrap();

        assert_eq!(
            user_rx.add_remove(msg.clone(), 1).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );

        assert_eq!(user_rx.out_of_order_queue.stored_packets(), 1);
        assert_eq!(user_rx.out_of_order_queue.stored_bytes(), 1);
        assert_eq!(user_rx.out_of_order_queue.filled_front(), 0);

        assert_eq!(
            user_rx.add_remove(msg.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(2)
        );
        assert_eq!(user_rx.out_of_order_queue.stored_packets(), 2);
        assert_eq!(user_rx.out_of_order_queue.stored_bytes(), 2);
        assert_eq!(user_rx.out_of_order_queue.filled_front(), 2);
    }

    #[test]
    fn test_asm_out_of_order() {
        setup_test_logging();

        let mut asm = user_rx(100, 3);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            asm.add_remove(msg_1.clone(), 1).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );
        trace!(asm=%asm.out_of_order_queue.debug_string(true));
        assert_eq!(asm.out_of_order_queue.stored_packets(), 1);

        assert_eq!(
            asm.add_remove(msg_2.clone(), 2).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(0)
        );
        trace!(asm=%asm.out_of_order_queue.debug_string(true));

        assert_eq!(
            asm.add_remove(msg_0.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(3)
        );
        trace!(asm=%asm.out_of_order_queue.debug_string(true));
        assert_eq!(asm.out_of_order_queue.stored_packets(), 0);

        assert_eq!(
            asm.locked.lock().queue.pop_front().unwrap(),
            UserRxMessage::Payload(msg_0.into_payload())
        );
        assert_eq!(
            asm.locked.lock().queue.pop_front().unwrap(),
            UserRxMessage::Payload(msg_1.into_payload())
        );
        assert_eq!(
            asm.locked.lock().queue.pop_front().unwrap(),
            UserRxMessage::Payload(msg_2.into_payload())
        );
    }

    #[test]
    fn test_asm_inorder() {
        setup_test_logging();
        let mut asm = user_rx(100, 3);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            asm.add_remove(msg_0.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
        );
        trace!(asm=%asm.out_of_order_queue.debug_string(true));
        assert_eq!(asm.out_of_order_queue.stored_packets(), 0);

        assert_eq!(
            asm.add_remove(msg_1.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
        );
        trace!(asm=%asm.out_of_order_queue.debug_string(true));

        assert_eq!(
            asm.add_remove(msg_2.clone(), 0).unwrap(),
            AssemblerAddRemoveResult::ConsumedSequenceNumbers(1)
        );
        trace!(asm=%asm.out_of_order_queue.debug_string(true));
        assert_eq!(asm.out_of_order_queue.stored_packets(), 0);

        assert_eq!(
            asm.locked.lock().queue.pop_front().unwrap(),
            UserRxMessage::Payload(msg_0.into_payload())
        );
        assert_eq!(
            asm.locked.lock().queue.pop_front().unwrap(),
            UserRxMessage::Payload(msg_1.into_payload())
        );
        assert_eq!(
            asm.locked.lock().queue.pop_front().unwrap(),
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
