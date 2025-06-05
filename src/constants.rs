// TODO: make MTU configurable and auto-detectable,
// or at least shrink it down for VPNs and other common shrink reasons.

use std::time::Duration;

use tracing::Level;

pub const IPV4_HEADER: u16 = 20;
pub const IPV6_HEADER: u16 = 40;

pub const UDP_HEADER: u16 = 8;
pub const UTP_HEADER: u16 = 20;

// By default, flow control (dropping incoming packets) starts after this
// many bytes are unread in user's stream reader.
pub const DEFAULT_MAX_RX_BUF_SIZE_PER_VSOCK: usize = 1024 * 1024;
// By default, this is how many unACKed bytes the socket can store without blocking writer.
pub const DEFAULT_MAX_TX_BUF_SIZE_PER_VSOCK: usize = 64 * 1024;

// Delayed ACK timer. Linux has 40ms, so we set to it too.
pub const ACK_DELAY: Duration = Duration::from_millis(40);

pub const SOFT_IMMEDIATE_ACK_EVERY_RMSS: usize = 2;
pub const HARD_IMMEDIATE_ACK_EVERY_RMSS: usize = 50;

pub const SYNACK_RESEND_INTERNAL: Duration = Duration::from_millis(200);

// u16 SeqNrs wrap around. If they are too far apart, this is used to detect if they wrapped or not.
pub const WRAP_TOLERANCE: u16 = 1024;

pub const CONGESTION_TRACING_LOG_LEVEL: Level = Level::DEBUG;
pub const RTTE_TRACING_LOG_LEVEL: Level = Level::TRACE;
pub const RECOVERY_TRACING_LOG_LEVEL: Level = Level::TRACE;

// How long to wait to kill the connection if the remote is non-responsive.
pub const DEFAULT_REMOTE_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(30);

pub const DEFAULT_MAX_ACTIVE_STREAMS_PER_SOCKET: usize = 64;

pub const SACK_DUP_THRESH: u8 = 3;
pub const SACK_DEPTH: usize = 64;

pub fn calc_pipe_expiry(rtt: Duration) -> Duration {
    // rtt/2 might be too aggressive
    rtt * 3 / 4
}
