use smoltcp::storage::RingBuffer;

use crate::{raw::UtpHeader, utils::seq_nr_offset};

type RingBufferHeader = RingBuffer<'static, (UtpHeader, usize)>;

// The TX queue of the virtual socket. uTP doesn't refragemnt, so we store the original headers.
// The payloads are stored in the user TX behind a shared lock. Users write there with poll_write().
pub struct Tx {
    headers: RingBufferHeader,

    wrap_tolerance: u16,
}

pub struct TxIterItem<'a> {
    pub header: &'a UtpHeader,
    payload_size: usize,
    payload_offset: usize,
}

impl TxIterItem<'_> {
    pub fn header(&self) -> &UtpHeader {
        self.header
    }
    pub fn payload_size(&self) -> usize {
        self.payload_size
    }
    pub fn payload_offset(&self) -> usize {
        self.payload_offset
    }
}

impl Tx {
    // TODO: we can avoid copying by re-using user-sides TX queue. We can  just store fragments.
    pub fn new(wrap_tolerance: u16) -> Self {
        Tx {
            headers: RingBufferHeader::new(vec![Default::default(); 64]),
            wrap_tolerance,
        }
    }

    pub fn first_seq_nr(&self) -> Option<u16> {
        if self.headers.is_empty() {
            return None;
        }
        Some(self.headers.get_allocated(0, 1)[0].0.seq_nr)
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    #[allow(unused)]
    pub fn is_full(&self) -> bool {
        self.headers.is_full()
    }

    pub fn enqueue(&mut self, h: UtpHeader, payload_size: usize) {
        *self.headers.enqueue_one().unwrap() = (h, payload_size);
    }

    #[allow(unused)]
    pub fn stats(&self) -> impl std::fmt::Debug {
        // this is for debugging only
        format!(
            "headers: {}/{}",
            self.headers.len(),
            self.headers.capacity(),
        )
    }

    // Returns number of removed headers, and their comibined payload size.
    pub fn remove_up_to_ack(&mut self, ack_nr: u16) -> (usize, usize) {
        let is_acked = |seq_nr: u16| seq_nr_offset(ack_nr, seq_nr, self.wrap_tolerance) >= 0;
        let mut removed = 0;
        let mut payload_size = 0;
        while let Ok(Ok(ps)) = self.headers.dequeue_one_with(|(header, payload_size)| {
            if is_acked(header.seq_nr) {
                return Ok(*payload_size);
            }
            Err(())
        }) {
            removed += 1;
            payload_size += ps;
        }
        (removed, payload_size)
    }

    // Iterate stored data - headers and their payloads (as a function to copy payload to some other buffer).
    pub fn iter(&self) -> impl Iterator<Item = TxIterItem<'_>> {
        struct It<'a> {
            tx: &'a Tx,
            offset: usize,
            payload_offset: usize,
        }

        impl<'a> Iterator for It<'a> {
            type Item = TxIterItem<'a>;

            fn next(&mut self) -> Option<Self::Item> {
                if self.offset >= self.tx.headers.len() {
                    return None;
                }
                let items = self.tx.headers.get_allocated(self.offset, 1);
                let payload_size = items[0].1;

                let payload_offset = self.payload_offset;
                self.payload_offset += payload_size;
                self.offset += 1;

                Some(TxIterItem {
                    header: &items[0].0,
                    payload_size,
                    payload_offset,
                })
            }
        }

        It {
            tx: self,
            offset: 0,
            payload_offset: 0,
        }
    }
}
