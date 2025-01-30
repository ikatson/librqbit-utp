use std::collections::VecDeque;

use crate::{raw::UtpHeader, seq_nr::SeqNr};

type RingBufferHeader = VecDeque<(UtpHeader, usize)>;

// The TX queue of the virtual socket. uTP doesn't refragemnt, so we store the original headers.
// The payloads are stored in the user TX behind a shared lock. Users write there with poll_write().
pub struct Tx {
    headers: RingBufferHeader,
    len_bytes: usize,
    capacity: usize,
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
    pub fn new() -> Self {
        Tx {
            headers: VecDeque::new(),
            len_bytes: 0,
            capacity: 64,
        }
    }

    pub fn first_seq_nr(&self) -> Option<SeqNr> {
        Some(self.headers.front()?.0.seq_nr)
    }

    #[cfg(test)]
    pub fn total_len_packets(&self) -> usize {
        self.headers.len()
    }

    pub fn total_len_bytes(&self) -> usize {
        self.len_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.headers.is_empty()
    }

    #[allow(unused)]
    pub fn is_full(&self) -> bool {
        self.headers.len() == self.capacity
    }

    #[must_use]
    pub fn enqueue(&mut self, h: UtpHeader, payload_size: usize) -> bool {
        if self.headers.len() == self.capacity {
            return false;
        }
        self.headers.push_back((h, payload_size));
        self.len_bytes += payload_size;
        true
    }

    #[allow(unused)]
    pub fn stats(&self) -> impl std::fmt::Debug {
        // this is for debugging only
        format!("headers: {}/{}", self.headers.len(), self.capacity)
    }

    // Returns number of removed headers, and their comibined payload size.
    pub fn remove_up_to_ack(&mut self, ack_nr: SeqNr) -> (usize, usize) {
        let mut removed = 0;
        let mut payload_size = 0;

        while let Some((header, ps)) = self.headers.pop_front() {
            if ack_nr < header.seq_nr {
                self.headers.push_front((header, ps));
                break;
            }

            removed += 1;
            payload_size += ps;
            self.len_bytes -= ps;
        }

        (removed, payload_size)
    }

    // Iterate stored data - headers and their payloads (as a function to copy payload to some other buffer).
    pub fn iter(&self) -> impl Iterator<Item = TxIterItem<'_>> {
        self.headers
            .iter()
            .scan(0, |offset, (header, payload_size)| {
                let current_offset = *offset;
                *offset += payload_size;
                Some(TxIterItem {
                    header,
                    payload_size: *payload_size,
                    payload_offset: current_offset,
                })
            })
    }
}
