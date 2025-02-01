use std::net::SocketAddr;

use tracing::debug;

use crate::{
    raw::{Type, UtpHeader},
    Payload,
};

#[derive(Default, Clone, PartialEq, Eq)]
pub struct UtpMessage {
    pub header: UtpHeader,
    pub data: Payload,
}

impl std::fmt::Debug for UtpMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}:seq_nr={}:ack_nr={}:payload={}",
            self.header.get_type(),
            self.header.seq_nr,
            self.header.ack_nr,
            self.payload().len()
        )
    }
}

impl UtpMessage {
    #[cfg(test)]
    pub fn new_test(header: UtpHeader, payload: &[u8]) -> Self {
        UtpMessage {
            header,
            data: payload.to_owned(),
        }
    }

    pub fn deserialize(buf: &[u8]) -> Option<Self> {
        let (header, hsize) = UtpHeader::deserialize(buf)?;

        let payload_size = buf.len() - hsize;

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
            data: buf[hsize..].to_owned(),
        })
    }

    pub fn payload(&self) -> &[u8] {
        &self.data
    }
}
