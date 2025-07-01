// TODO: make MTU configurable and auto-detectable,
// or at least shrink it down for VPNs and other common shrink reasons.

use std::time::Duration;

use tracing::Level;

pub const IPV4_HEADER: u16 = 20;
pub const IPV6_HEADER: u16 = 40;

pub const UDP_HEADER: u16 = 8;
pub const UTP_HEADER: u16 = 20;

/// Socket read buffer. Serves several purposes:
/// - temporary storage before the data is read back to the user's application
/// - reassembling out of order packets
/// - advertising recv window to the other side (sender). The sender will not send more data than this and might thus stall.
///   Ideal value for throughput should be BDP (bandwidth delay product).
pub const DEFAULT_MAX_RX_BUF_SIZE_PER_VSOCK: usize = 1024 * 1024;

/// How many unACKed bytes the socket can store without blocking writer.
/// Also how many bytes can we send without receivng an ACK.
/// This should be governed by BDP (bandwidth delay product). If it's too low, the pipeline
/// would be stalling.
/// If it's too high, every VSock would take too much memory.
pub const DEFAULT_MAX_TX_BUF_SIZE_PER_VSOCK: usize = 1024 * 1024;

// Delayed ACK timer. Linux has 40ms, so we set to it too.
pub const ACK_DELAY: Duration = Duration::from_millis(40);

/// This HUGELY impacts perf. The higher this number, the better the perf, as a lot of CPU is
/// spent to send ACKs.
/// However if N * mss happens to exceed cwnd, the performance would tank as we'll start sending
/// delayed ACKs only, and the pipeline would be stalling.
pub const IMMEDIATE_ACK_EVERY_RMSS: usize = 2;

pub const SYNACK_RESEND_INTERNAL: Duration = Duration::from_millis(200);

// u16 SeqNrs wrap around. If they are too far apart, this is used to detect if they wrapped or not.
pub const WRAP_TOLERANCE: u16 = 1024;

pub const CONGESTION_TRACING_LOG_LEVEL: Level = Level::DEBUG;
pub const RTTE_TRACING_LOG_LEVEL: Level = Level::TRACE;
pub const RECOVERY_TRACING_LOG_LEVEL: Level = Level::TRACE;

// How long to wait to kill the connection if the remote is non-responsive.
pub const DEFAULT_REMOTE_INACTIVITY_TIMEOUT: Duration = Duration::from_secs(10);

pub const DEFAULT_MAX_ACTIVE_STREAMS_PER_SOCKET: usize = 128;

pub const SACK_DUP_THRESH: u8 = 3;
pub const SACK_DEPTH: usize = 64;

pub fn calc_pipe_expiry(rtt: Duration) -> Duration {
    // rtt/2 might be too aggressive
    rtt * 3 / 4
}
