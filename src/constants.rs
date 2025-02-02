// TODO: make MTU configurable and auto-detectable,
// or at least shrink it down for VPNs and other common shrink reasons.

use std::time::Duration;

use tracing::Level;

pub const IPV4_HEADER: usize = 20;
pub const MIN_UDP_HEADER: usize = 8;
pub const UTP_HEADER_SIZE: usize = 20;

// DEFAULT_MTU is very conservative (to support VPNs / tunneling etc).
// It's used if auto-detection doesn't work.
pub const DEFAULT_CONSERVATIVE_OUTGOING_MTU: usize = 1280;
// This is used to calculate the packet pool sizes.
pub const DEFAULT_INCOMING_MTU: usize = 1520;

// Delayed ACK timer
pub const ACK_DELAY: Duration = Duration::from_millis(10);
// How often to send ACKs for out of order packets.
pub const CHALLENGE_ACK_RATELIMIT: Duration = Duration::from_secs(1);

pub const IMMEDIATE_ACK_EVERY: isize = 5;

pub const WRAP_TOLERANCE: u16 = 1024;

pub const CONGESTION_TRACING_LOG_LEVEL: Level = Level::WARN;
pub const RTTE_TRACING_LOG_LEVEL: Level = Level::WARN;
