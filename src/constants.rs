// TODO: make MTU configurable and auto-detectable,
// or at least shrink it down for VPNs and other common shrink reasons.

use std::{
    net::{IpAddr, Ipv4Addr},
    time::Duration,
};

use tracing::Level;

pub const IPV4_HEADER: usize = 20;
pub const MIN_UDP_HEADER: usize = 8;
pub const UTP_HEADER_SIZE: usize = 20;

// This is used to calculate the packet pool sizes.
pub const DEFAULT_INCOMING_MTU: usize = 1520;
// By default, flow control (dropping incoming packets) starts after this
// many bytes are unread in user's stream reader.
pub const DEFAULT_MAX_RX_BUF_SIZE_PER_VSOCK: usize = 1024 * 1024;
// By default, this is how many unACKed bytes the socket can store without blocking writer.
pub const DEFAULT_MAX_TX_BUF_SIZE_PER_VSOCK: usize = 1024 * 1024;
// How many out-of-order packets to track.
pub const DEFAULT_MAX_OUT_OF_ORDER_PACKETS: usize = 64;
// Outgoing MTU is autodetected using this IP. It's used to calculate the maximum uTP
// segment size we can send.
pub const DEFAULT_MTU_AUTODETECT_IP: IpAddr = IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1));
// This MTU is used in case it can't be autodetected. Very conservative to support VPNs / tunneling etc.
pub const DEFAULT_CONSERVATIVE_OUTGOING_MTU: usize = 1280;

// Delayed ACK timer. Linux has 40ms, so we set to it too.
pub const ACK_DELAY: Duration = Duration::from_millis(40);

// u16 SeqNrs wrap around. If they are too far apart, this is used to detect if they wrapped or not.
pub const WRAP_TOLERANCE: u16 = 1024;

pub const CONGESTION_TRACING_LOG_LEVEL: Level = Level::TRACE;
pub const RTTE_TRACING_LOG_LEVEL: Level = Level::TRACE;
