use std::io::IoSlice;

/// A helper for working with a buffer split into 2.
/// You can advance it forward (like you would do with buf=&buf[idx..])
#[derive(Clone, Copy)]
pub struct DoubleBufHelper<'a> {
    buf_0: &'a [u8],
    buf_1: &'a [u8],
}

impl<'a> DoubleBufHelper<'a> {
    pub fn new(buf: &'a [u8], buf2: &'a [u8]) -> Self {
        Self {
            buf_0: buf,
            buf_1: buf2,
        }
    }

    #[allow(unused)]
    pub fn len(&self) -> usize {
        self.buf_0.len() + self.buf_1.len()
    }

    #[allow(unused)]
    pub fn is_empty(&self) -> bool {
        self.buf_0.len() == 0 && self.buf_1.len() == 0
    }

    /// Advance it forward. If it was a single buffer this would be equivalent to buf=&buf[idx..]).
    /// If offset is too large, will set itself empty.
    pub fn advance(&mut self, offset: usize) {
        let buf_0_adv = self.buf_0.len().min(offset);
        self.buf_0 = &self.buf_0[buf_0_adv..];
        let buf_1_adv = (offset - buf_0_adv).min(self.buf_1.len());
        self.buf_1 = &self.buf_1[buf_1_adv..];
    }

    pub fn as_ioslices(&self, len_limit: usize) -> [IoSlice<'a>; 2] {
        let buf_0_len = self.buf_0.len().min(len_limit);
        let buf_1_len = (len_limit - buf_0_len).min(self.buf_1.len());
        [
            IoSlice::new(&self.buf_0[..buf_0_len]),
            IoSlice::new(&self.buf_1[..buf_1_len]),
        ]
    }
}
