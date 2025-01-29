#[derive(Default, Clone)]
pub struct Packet {
    buf: Vec<u8>,
}

impl Packet {
    pub fn new(buf: &[u8]) -> Self {
        Self {
            buf: buf.to_owned(),
        }
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    pub fn get(&self) -> &[u8] {
        &self.buf
    }
}

impl std::fmt::Debug for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Packet")
            .field("len", &self.buf.len())
            .finish_non_exhaustive()
    }
}
