// TODO: make MTU configurable and auto-detectable,
// or at least shrink it down for VPNs and other common shrink reasons.

use std::time::Duration;

pub const IPV4_HEADER: usize = 20;
pub const MIN_UDP_HEADER: usize = 8;
pub const UTP_HEADER_SIZE: usize = 20;

pub const DEFAULT_MTU: usize = 1280;

// Delayed ACK timer
pub const ACK_DELAY: Duration = Duration::from_millis(10);
// How often to send ACKs for out of order packets.
pub const CHALLENGE_ACK_RATELIMIT: Duration = Duration::from_secs(1);

pub const IMMEDIATE_ACK_EVERY: isize = 5;

pub const WRAP_TOLERANCE: u16 = 1024;
