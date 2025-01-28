use tracing::debug;

use crate::{
    packet_pool::Packet,
    raw::{Type, UtpHeader},
};

#[derive(Default, Clone)]
pub struct UtpMessage {
    pub header: UtpHeader,
    payload_start: usize,
    size: usize,
    data: Packet,
}

impl std::fmt::Debug for UtpMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpMessage")
            .field("header", &self.header)
            .field("payload_size", &self.payload().len())
            .finish_non_exhaustive()
    }
}

impl UtpMessage {
    #[cfg(test)]
    pub fn new_test(header: UtpHeader, payload: &[u8]) -> anyhow::Result<Self> {
        let packet = Packet::new_test(payload.to_vec());
        Ok(UtpMessage {
            header,
            payload_start: 0,
            size: payload.len(),
            data: packet,
        })
    }

    pub fn deserialize(packet: Packet, size: usize) -> Option<Self> {
        let mut packet = packet;
        let (header, hsize) = UtpHeader::deserialize(packet.get_mut())?;

        let payload_size = size - hsize;

        match header.get_type() {
            Type::ST_DATA => {
                if payload_size == 0 {
                    debug!("ST_DATA packet with 0 payload, ignoring");
                    return None;
                }
            }
            other => {
                if payload_size > 0 {
                    debug!("{other:?} packet with payload, ignoring");
                    return None;
                }
            }
        }

        Some(Self {
            header,
            payload_start: hsize,
            size,
            data: packet,
        })
    }

    pub fn payload(&self) -> &[u8] {
        &self.data.get()[self.payload_start..self.size]
    }
}
