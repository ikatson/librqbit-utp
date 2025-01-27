use smoltcp::storage::{Assembler, RingBuffer};
use tracing::trace;

use crate::message::UtpMessage;

pub struct AssembledRx {
    assembler: Assembler,
    rx: RingBuffer<'static, UtpMessage>,
    max_payload_size: usize,
}

impl AssembledRx {
    pub fn new(tx_buf_len: usize, max_payload_size: usize) -> Self {
        Self {
            assembler: Assembler::new(),
            rx: RingBuffer::new(vec![Default::default(); tx_buf_len]),
            max_payload_size,
        }
    }

    fn unacked_packets(&self) -> usize {
        self.assembler.iter_data(0).map(|(s, e)| e - s).sum()
    }

    pub fn window(&self) -> usize {
        let unacked = self.unacked_packets();
        assert!(unacked <= self.rx.capacity());
        (self.rx.capacity() - unacked) * self.max_payload_size
    }

    pub fn is_empty(&self) -> bool {
        self.assembler.is_empty()
    }

    pub fn debug_string(&self) -> &impl std::fmt::Display {
        &self.assembler
    }

    pub fn add_remove(
        &mut self,
        msg: UtpMessage,
        offset: usize,

        // TODO: when bounding user queues, this should be able to return errors
        mut f: impl FnMut(UtpMessage),
    ) -> std::result::Result<usize, UtpMessage> {
        let slot = self.rx.get_unallocated(offset, 1);
        if slot.is_empty() {
            trace!(?msg, offset, "empty assembler buffer slot");
            return Err(msg);
        }

        let removed = match self.assembler.add_then_remove_front(offset, 1) {
            Ok(count) => count,
            Err(_) => {
                trace!(?self.assembler, offset, ?msg.header.seq_nr, "too many holes");
                return Err(msg);
            }
        };

        if removed > 0 {
            // The first message is guaranteed to be there. We don't need to write it
            f(msg);

            self.rx.enqueue_unallocated(removed);
            self.rx.dequeue_one().unwrap();

            for _ in 1..removed {
                let msg = self.rx.dequeue_one().unwrap();
                f(std::mem::take(msg));
            }
            Ok(removed)
        } else {
            // Store the out of order message for future.
            slot[0] = msg;
            Ok(removed)
        }
    }
}
