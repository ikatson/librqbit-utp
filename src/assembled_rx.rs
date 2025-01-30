use anyhow::Context;
use smoltcp::storage::{Assembler, RingBuffer};
use tokio::sync::mpsc;
use tracing::{debug, trace};

use crate::{message::UtpMessage, raw::selective_ack::SelectiveAck, stream::UserRxMessage};

pub struct AssembledRx {
    assembler: Assembler,
    rx: RingBuffer<'static, UtpMessage>,
}

impl AssembledRx {
    pub fn new(tx_buf_len: usize) -> Self {
        Self {
            assembler: Assembler::new(),
            rx: RingBuffer::new(vec![Default::default(); tx_buf_len]),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.assembler.is_empty()
    }

    pub fn debug_string(&self) -> &impl std::fmt::Display {
        &self.assembler
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
    ) -> anyhow::Result<std::result::Result<usize, UtpMessage>> {
        let slot = self.rx.get_unallocated(offset, 1);
        if slot.is_empty() {
            trace!(?msg, offset, "empty assembler buffer slot");
            return Ok(Err(msg));
        }

        let asm_before = self.assembler.clone();

        let removed = match self.assembler.add_then_remove_front(offset, 1) {
            Ok(count) => count,
            Err(_) => {
                trace!(?self.assembler, offset, ?msg.header.seq_nr, "too many holes");
                return Ok(Err(msg));
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
            return Ok(Err(msg));
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

            self.rx.enqueue_unallocated(removed);
            self.rx.dequeue_one().unwrap();

            for _ in 1..removed {
                let msg = self.rx.dequeue_one().unwrap();
                send(std::mem::take(msg))?;
            }
            Ok(Ok(removed))
        } else {
            // Store the out of order message for future.
            slot[0] = msg;
            Ok(Ok(removed))
        }
    }
}
