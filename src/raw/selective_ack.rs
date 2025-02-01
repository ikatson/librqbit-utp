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

    pub fn deserialize(bytes: &[u8]) -> Self {
        // The spec says the len must be a multiple of 4, but there's a ton of messages
        // in the wild that are 1 bytes long (probably coming from libutp). So we can
        // thus deserialize any payload.
        //
        // If it's longer than 8 bytes (unlikely), it will truncate the end, which is fine, as
        // we'll just resend that data if anything.
        let len = bytes.len().min(std::mem::size_of::<SelectiveAckData>());
        let mut data = SelectiveAckData::default();
        data.as_raw_mut_slice()[..len].copy_from_slice(&bytes[..len]);
        Self { data }
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use crate::{
        message::UtpMessage, raw::selective_ack::SelectiveAck, stream_rx::OutOfOrderQueue,
    };

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

        let sack = asm.selective_ack().unwrap();
        assert_eq!(
            sack.data.as_raw_slice(),
            [0b1000_0011, 0, 0, 0, 0, 0, 0, 0b1000_0000]
        );

        assert_eq!(sack, SelectiveAck::deserialize(sack.as_bytes()));
    }
}
