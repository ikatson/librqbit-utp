pub mod ext_close_reason;
pub mod selective_ack;

use anyhow::{bail, Context};

use tracing::{debug, trace};

use crate::{constants::UTP_HEADER_SIZE, seq_nr::SeqNr};

const EXT_CLOSE_REASON: u8 = 3;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[allow(non_camel_case_types)]
pub enum Type {
    ST_DATA = 0,
    ST_FIN = 1,
    #[default]
    ST_STATE = 2,
    ST_RESET = 3,
    ST_SYN = 4,
}

impl Type {
    fn from_number(num: u8) -> Option<Type> {
        match num {
            0 => Some(Type::ST_DATA),
            1 => Some(Type::ST_FIN),
            2 => Some(Type::ST_STATE),
            3 => Some(Type::ST_RESET),
            4 => Some(Type::ST_SYN),
            _ => None,
        }
    }

    fn to_number(self) -> u8 {
        match self {
            Type::ST_DATA => 0,
            Type::ST_FIN => 1,
            Type::ST_STATE => 2,
            Type::ST_RESET => 3,
            Type::ST_SYN => 4,
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Extensions {
    pub close_reason: Option<ext_close_reason::LibTorrentCloseReason>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct UtpHeader {
    pub htype: Type,                            // 4 bits type and 4 bits version
    pub connection_id: SeqNr,                   // Connection ID
    pub timestamp_microseconds: u32,            // Timestamp in microseconds
    pub timestamp_difference_microseconds: u32, // Timestamp difference in microseconds
    pub wnd_size: u32,                          // Window size
    pub seq_nr: SeqNr,                          // Sequence number
    pub ack_nr: SeqNr,                          // Acknowledgment number
    pub extensions: Option<Extensions>,
}

impl UtpHeader {
    pub fn set_type(&mut self, packet_type: Type) {
        // let packet_type = packet_type.to_number();
        // self.type_ver = (self.type_ver & 0xF0) | (packet_type & 0x0F);
        self.htype = packet_type;
    }

    pub fn get_type(&self) -> Type {
        // Type::from_number(self.type_ver & 0x0F)
        self.htype
    }

    pub fn serialize(&self, buffer: &mut [u8]) -> anyhow::Result<usize> {
        if buffer.len() < UTP_HEADER_SIZE {
            bail!("too small buffer");
        }
        const VERSION: u8 = 1;
        let typever = (self.htype.to_number() << 4) | VERSION;
        buffer[0] = typever;
        // TODO: extensions not supported yet.
        buffer[1] = 0;
        buffer[2..4].copy_from_slice(&self.connection_id.to_be_bytes());
        buffer[4..8].copy_from_slice(&self.timestamp_microseconds.to_be_bytes());
        buffer[8..12].copy_from_slice(&self.timestamp_difference_microseconds.to_be_bytes());
        buffer[12..16].copy_from_slice(&self.wnd_size.to_be_bytes());
        buffer[16..18].copy_from_slice(&self.seq_nr.to_be_bytes());
        buffer[18..20].copy_from_slice(&self.ack_nr.to_be_bytes());
        Ok(UTP_HEADER_SIZE)
    }

    pub fn serialize_with_payload(
        &self,
        out_buf: &mut [u8],
        payload_serialize: impl FnOnce(&mut [u8]) -> anyhow::Result<usize>,
    ) -> anyhow::Result<usize> {
        let sz = self.serialize(out_buf)?;
        let payload_sz = payload_serialize(out_buf.get_mut(sz..).context("too small buffer")?)
            .context("error serializing payload")?;
        Ok(sz + payload_sz)
    }

    pub fn deserialize(mut buffer: &[u8]) -> Option<(Self, usize)> {
        if buffer.len() < UTP_HEADER_SIZE {
            return None;
        }
        let mut header = UtpHeader::default();

        let typenum = buffer[0] >> 4;
        let version = buffer[0] & 0xf;
        if version != 1 {
            trace!(version, "wrong version");
            return None;
        }
        header.htype = Type::from_number(typenum)?;
        let mut next_ext = buffer[1];
        header.connection_id = u16::from_be_bytes(buffer[2..4].try_into().unwrap()).into();
        header.timestamp_microseconds = u32::from_be_bytes(buffer[4..8].try_into().unwrap());
        header.timestamp_difference_microseconds =
            u32::from_be_bytes(buffer[8..12].try_into().unwrap());
        header.wnd_size = u32::from_be_bytes(buffer[12..16].try_into().unwrap());
        header.seq_nr = u16::from_be_bytes(buffer[16..18].try_into().unwrap()).into();
        header.ack_nr = u16::from_be_bytes(buffer[18..20].try_into().unwrap()).into();

        buffer = &buffer[UTP_HEADER_SIZE..];

        let mut total_ext_size = 0usize;

        while next_ext > 0 {
            total_ext_size += 2;
            let ext = next_ext;
            next_ext = *buffer.first()?;
            let ext_len = *buffer.get(1)? as usize;

            let ext_data = buffer.get(2..2 + ext_len)?;
            trace!(ext, ext_len, "extension in packet");
            match (ext, ext_len) {
                (EXT_CLOSE_REASON, 4) => {
                    header.extensions.get_or_insert_default().close_reason =
                        Some(ext_close_reason::LibTorrentCloseReason::parse(
                            ext_data.try_into().unwrap(),
                        ));
                }
                _ => {
                    debug!(ext, next_ext, ext_len, "unsupported extension, skipping");
                }
            }

            total_ext_size += ext_len;
            buffer = buffer.get(2 + ext_len..)?;
        }

        Some((header, UTP_HEADER_SIZE + total_ext_size))
    }
}

#[cfg(test)]
mod tests {
    use crate::{raw::Type, test_util::setup_test_logging};

    use super::UtpHeader;

    #[test]
    fn test_parse_fin_with_extension() {
        setup_test_logging();
        let packet = include_bytes!("../test/resources/packet_fin_with_extension.bin");
        let (header, len) = UtpHeader::deserialize(packet).unwrap();
        assert_eq!(
            header,
            UtpHeader {
                htype: Type::ST_FIN,
                connection_id: 30796.into(),
                timestamp_microseconds: 2293274188,
                timestamp_difference_microseconds: 1967430273,
                wnd_size: 1048576,
                seq_nr: 54661.into(),
                ack_nr: 54397.into(),
                extensions: Some(crate::raw::Extensions {
                    close_reason: Some(crate::raw::ext_close_reason::LibTorrentCloseReason(15))
                })
            }
        );
        assert_eq!(len, packet.len());
    }
}
