use std::collections::VecDeque;

use anyhow::Context;
use smoltcp::storage::Assembler;
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::{message::UtpMessage, raw::selective_ack::SelectiveAck, stream::UserRxMessage};

pub struct AssembledRx {
    assembler: Assembler,
    queue: VecDeque<UtpMessage>,
    len: usize,
    capacity: usize,
}

#[derive(Debug, PartialEq, Eq)]
pub enum AssemblerAddRemoveResult {
    SentToUserRx(usize),
    Unavailable(UtpMessage),
}

impl AssembledRx {
    pub fn new(tx_buf_len: usize) -> Self {
        Self {
            assembler: Assembler::new(),
            queue: VecDeque::from(vec![Default::default(); tx_buf_len]),
            len: 0,
            capacity: tx_buf_len,
        }
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

    pub fn debug_string(&self, with_queue: bool) -> impl std::fmt::Display + '_ {
        struct D<'a> {
            asm: &'a AssembledRx,
            with_queue: bool,
        }
        impl std::fmt::Display for D<'_> {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                write!(f, "len={}, asm={}", self.asm.len(), self.asm.assembler)?;

                if !self.with_queue {
                    return Ok(());
                }

                write!(f, ", queue=")?;

                for (idx, item) in self.asm.queue.iter().enumerate() {
                    if idx > 0 {
                        write!(f, ", ")?;
                    }
                    write!(
                        f,
                        "{{type={:?}:seq_nr={}:payload={}}}",
                        item.header.get_type(),
                        item.header.seq_nr,
                        item.payload().len()
                    )?;
                }
                Ok(())
            }
        }
        D {
            asm: self,
            with_queue,
        }
    }

    pub fn selective_ack(&self) -> Option<SelectiveAck> {
        SelectiveAck::new(&self.assembler)
    }

    // anyhow error on fatal
    // otherwise either len or message back TODO: a different enum for this
    pub fn add_remove(
        &mut self,
        msg: UtpMessage,
        offset: usize,
        user_rx: &mpsc::Sender<UserRxMessage>,
    ) -> anyhow::Result<AssemblerAddRemoveResult> {
        if self.is_full() {
            trace!(offset, "assembler buffer full");
            return Ok(AssemblerAddRemoveResult::Unavailable(msg));
        }

        let asm_before = self.assembler.clone();

        let removed = match self.assembler.add_then_remove_front(offset, 1) {
            Ok(count) => count,
            Err(_) => {
                trace!(?self.assembler, offset, ?msg.header.seq_nr, "too many holes");
                return Ok(AssemblerAddRemoveResult::Unavailable(msg));
            }
        };

        let rxcap = user_rx.capacity();
        if rxcap < removed {
            debug!(
                current = rxcap,
                required = removed,
                "user rx doesn't have enough capacity"
            );
            self.assembler = asm_before;
            return Ok(AssemblerAddRemoveResult::Unavailable(msg));
        }

        let send = |msg| -> anyhow::Result<()> {
            user_rx
                .try_send(UserRxMessage::UtpMessage(msg))
                .context("user rx must have had capacity but it doesnt. cant recover from this")
        };

        if removed > 0 {
            // The first message is guaranteed to be there. We don't need to write it
            // user_rx. (msg);
            send(msg)?;

            for _ in 1..removed {
                let msg = self.queue.pop_front().unwrap();
                self.queue.push_back(Default::default());
                send(msg)?;
            }

            self.len -= removed - 1;

            Ok(AssemblerAddRemoveResult::SentToUserRx(removed))
        } else {
            *self.queue.get_mut(offset - 1).unwrap() = msg;
            self.len += 1;
            Ok(AssemblerAddRemoveResult::SentToUserRx(removed))
        }
    }
}

#[cfg(test)]
mod tests {
    use tokio::sync::mpsc::channel;
    use tracing::trace;

    use crate::{
        assembled_rx::AssemblerAddRemoveResult, message::UtpMessage, stream::UserRxMessage,
        test_util::setup_test_logging,
    };

    use super::AssembledRx;

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
        let (tx, _rx) = channel(1);
        let mut asm = AssembledRx::new(2);
        assert_eq!(
            asm.add_remove(msg(0, b""), 0, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(1)
        );
    }

    #[test]
    fn test_asm_add_one_out_of_order() {
        let (tx, _rx) = channel(1);
        let mut asm = AssembledRx::new(2);
        assert_eq!(
            asm.add_remove(msg(0, b""), 1, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(0)
        );
    }

    #[test]
    fn test_asm_channel_full_asm_empty() {
        let (tx, _rx) = channel(1);
        let mut asm = AssembledRx::new(2);
        let msg = msg(0, b"");
        // fill the channel
        tx.try_send(crate::stream::UserRxMessage::UtpMessage(msg.clone()))
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
        let mut asm = AssembledRx::new(2);
        let msg = msg(0, b"");
        // fill the channel
        tx.try_send(crate::stream::UserRxMessage::UtpMessage(msg.clone()))
            .unwrap();

        assert_eq!(
            asm.add_remove(msg.clone(), 1, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(0)
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
        let mut asm = AssembledRx::new(3);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            asm.add_remove(msg_1.clone(), 1, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(0)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 1);

        assert_eq!(
            asm.add_remove(msg_2.clone(), 2, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(0)
        );
        trace!(asm=%asm.debug_string(true));

        assert_eq!(
            asm.add_remove(msg_0.clone(), 0, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(3)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 0);

        assert_eq!(rx.try_recv().unwrap(), UserRxMessage::UtpMessage(msg_0));
        assert_eq!(rx.try_recv().unwrap(), UserRxMessage::UtpMessage(msg_1));
        assert_eq!(rx.try_recv().unwrap(), UserRxMessage::UtpMessage(msg_2));
    }

    #[test]
    fn test_asm_inorder() {
        setup_test_logging();

        let (tx, mut rx) = channel(3);
        let mut asm = AssembledRx::new(3);

        let msg_0 = msg(0, b"hello");
        let msg_1 = msg(1, b"world");
        let msg_2 = msg(2, b"test");

        assert_eq!(
            asm.add_remove(msg_0.clone(), 0, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(1)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 0);

        assert_eq!(
            asm.add_remove(msg_1.clone(), 0, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(1)
        );
        trace!(asm=%asm.debug_string(true));

        assert_eq!(
            asm.add_remove(msg_2.clone(), 0, &tx).unwrap(),
            AssemblerAddRemoveResult::SentToUserRx(1)
        );
        trace!(asm=%asm.debug_string(true));
        assert_eq!(asm.len(), 0);

        assert_eq!(rx.try_recv().unwrap(), UserRxMessage::UtpMessage(msg_0));
        assert_eq!(rx.try_recv().unwrap(), UserRxMessage::UtpMessage(msg_1));
        assert_eq!(rx.try_recv().unwrap(), UserRxMessage::UtpMessage(msg_2));
    }
}
