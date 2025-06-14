use crate::stream_dispatch::VirtualSocketState;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("{0}")]
    Text(&'static str),

    #[error(
        "provided link_mtu ({link_mtu}) too low, not enough for even 1-byte IPv4 packets (min {min_mtu})"
    )]
    LinkMtuTooLow { link_mtu: u16, min_mtu: u16 },

    #[error("error sending SYN: {0}")]
    ErrorSendingSyn(std::io::Error),

    #[error("task cancelled")]
    TaskCancelled,

    #[error(transparent)]
    Dualstack(#[from] librqbit_dualstack_sockets::Error),

    #[error("error receiving: {0}")]
    Recv(std::io::Error),

    #[error("error sending UDP packet: {0}")]
    Send(std::io::Error),

    #[error("remote was inactive for too long: state={state:?}")]
    RemoteInactiveForTooLong { state: VirtualSocketState },

    #[error(
        "error filling output buffer from user_tx: too small buffer: out_buf.len() < len ({out_buf_len} < {len})"
    )]
    BugTooSmallBuffer { out_buf_len: usize, len: usize },
    #[error(
        "bug in buffer computations: user_tx_buflen={user_tx_buflen} segmented_len={segmented_len}"
    )]
    BugInBufferComputations {
        user_tx_buflen: usize,
        segmented_len: usize,
    },
    #[error("bug: truncate_front: skipped({skipped}) != count({count})")]
    BugTruncateFront { skipped: usize, count: usize },
    #[error("bug in assembler: slot {0} should be there")]
    BugAssemblerMissingSlot(usize),
}

pub type Result<T> = std::result::Result<T, Error>;

pub(crate) trait OptionContext<T>: Sized {
    fn context(self, msg: &'static str) -> Result<T>;
}

impl<T> OptionContext<T> for Option<T> {
    fn context(self, msg: &'static str) -> Result<T> {
        self.ok_or(Error::Text(msg))
    }
}
