use crate::{
    message::UtpMessage,
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
};

#[derive(Default)]
pub struct CmpUtpHeader {
    pub htype: Option<Type>,
    pub wnd_size: Option<u32>,
    pub seq_nr: Option<SeqNr>,
    pub ack_nr: Option<SeqNr>,
    pub payload: Option<String>,
}

impl std::fmt::Debug for CmpUtpHeader {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        macro_rules! w {
            ($name:ident) => {
                if let Some(v) = self.$name.as_ref() {
                    write!(f, "{}={:?};", stringify!($name), v)?;
                }
            };
        }
        w!(htype);
        w!(seq_nr);
        w!(ack_nr);
        w!(wnd_size);
        w!(payload);
        Ok(())
    }
}

impl PartialEq<CmpUtpHeader> for UtpHeader {
    fn eq(&self, other: &CmpUtpHeader) -> bool {
        macro_rules! cmp {
            ($name:ident) => {
                if let Some(v) = other.$name {
                    if self.$name != v {
                        return false;
                    }
                }
            };
        }
        cmp!(htype);
        cmp!(wnd_size);
        cmp!(seq_nr);
        cmp!(ack_nr);
        true
    }
}

impl PartialEq<CmpUtpHeader> for UtpMessage {
    fn eq(&self, other: &CmpUtpHeader) -> bool {
        self.header == *other
            && self.payload()
                == other
                    .payload
                    .as_ref()
                    .map(|p| p.as_bytes())
                    .unwrap_or_default()
    }
}
