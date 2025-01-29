use bitvec::{
    order::{Lsb0, Msb0},
    BitArr,
};

use smoltcp::storage::Assembler;

type SelectiveAckData = BitArr!(for 64, in u8, Lsb0);

#[derive(Debug)]
pub struct SelectiveAck {
    data: SelectiveAckData,
}

impl SelectiveAck {
    pub fn new(asm: &Assembler) -> Option<Self> {
        if asm.is_empty() {
            return None;
        }
        if asm.peek_front() > 0 {
            return None;
        }
        let mut data = SelectiveAckData::default();
        for (start, end) in asm.iter_data(0) {
            data.get_mut(start - 1..end - 1)?.fill(true);
        }
        Some(Self { data })
    }
}

#[cfg(test)]
mod tests {
    use smoltcp::storage::Assembler;

    use crate::raw::selective_ack::SelectiveAck;

    #[test]
    fn test_empty_is_none() {
        let asm = Assembler::new();
        assert!(SelectiveAck::new(&asm).is_none());
    }

    #[test]
    fn test_if_first_available_then_none() {
        let mut asm = Assembler::new();
        asm.add(0, 1).unwrap();
        assert!(SelectiveAck::new(&asm).is_none());

        let mut asm = Assembler::new();
        asm.add(0, 3).unwrap();
        assert!(SelectiveAck::new(&asm).is_none());

        let mut asm = Assembler::new();
        asm.add(0, 3).unwrap();
        asm.add(9, 10).unwrap();
        assert!(SelectiveAck::new(&asm).is_none());
    }

    #[test]
    fn test_holes() {
        let mut asm = Assembler::new();
        asm.add(8, 1).unwrap();
        asm.add(1, 2).unwrap();
        asm.add(64, 1).unwrap();
        assert_eq!(
            SelectiveAck::new(&asm).unwrap().data.as_raw_slice(),
            [0b1000_0011, 0, 0, 0, 0, 0, 0, 0b1000_0000]
        );
    }
}
