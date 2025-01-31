use bitvec::{order::Lsb0, BitArr};

type SelectiveAckData = BitArr!(for 64, in u8, Lsb0);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SelectiveAck {
    data: SelectiveAckData,
}

impl SelectiveAck {
    pub fn new(unacked: impl Iterator<Item = usize>) -> Option<Self> {
        let mut data = SelectiveAckData::default();

        for idx in unacked.take_while(|i| *i < 64) {
            data.set(idx, true);
        }
        Some(Self { data })
    }

    pub fn as_bytes(&self) -> &[u8] {
        self.data.as_raw_slice()
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{message::UtpMessage, stream_rx::OutOfOrderQueue};

    fn asm() -> OutOfOrderQueue {
        OutOfOrderQueue::new(NonZeroUsize::new(65).unwrap())
    }

    fn msg() -> UtpMessage {
        UtpMessage::new_test(Default::default(), b"a")
    }

    #[test]
    fn test_empty_is_none() {
        let asm = asm();
        assert!(asm.selective_ack().is_none());
    }

    #[test]
    fn test_holes() {
        let mut asm = asm();
        asm.add_remove(msg(), 8).unwrap();
        asm.add_remove(msg(), 1).unwrap();
        asm.add_remove(msg(), 2).unwrap();
        asm.add_remove(msg(), 64).unwrap();
        assert_eq!(
            asm.selective_ack().unwrap().data.as_raw_slice(),
            [0b1000_0011, 0, 0, 0, 0, 0, 0, 0b1000_0000]
        );
    }
}
