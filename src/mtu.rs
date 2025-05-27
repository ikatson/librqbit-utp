// Calculations for payload sizes that we should be sending, based on MTU and successfully
// delivered payloads so far. Binary searches (probes) the payload size until convergence.
use crate::constants::{IPV4_HEADER, IPV6_HEADER, UDP_HEADER, UTP_HEADER};

const PROBE_EXPIRY_COOLDOWN_PACKETS: u16 = 2;

#[derive(Clone, Copy, Debug)]
pub struct SegmentSizes {
    // The minimum uTP payload size that we know can go through the link.
    min_ss: u16,
    // The maximum uTP payload size to probe for. Calculated from link MTU
    // (default 1500, ethernet)
    max_ss: u16,

    cooldown_remaining_packets: u16,
}

impl SegmentSizes {
    pub fn new(is_ipv4: bool, link_mtu: u16) -> Self {
        let ip_header_size = if is_ipv4 { IPV4_HEADER } else { IPV6_HEADER };
        let default_min_mtu = if is_ipv4 { 576 } else { 1280 };

        let calc = |mtu: u16| mtu - ip_header_size - UTP_HEADER - UDP_HEADER;

        // If the user provided too small MTU, clamp it up to 1 byte.
        let link_mtu = link_mtu.max(ip_header_size + UDP_HEADER + UTP_HEADER + 1);
        let min_mtu = default_min_mtu.min(link_mtu);
        let max_mtu = link_mtu;

        let min_ss = calc(min_mtu);
        let max_ss = calc(max_mtu);
        Self {
            min_ss,
            max_ss,
            cooldown_remaining_packets: 1,
        }
    }

    pub fn on_payload_delivered(&mut self, payload_size: usize) {
        let payload_size = payload_size.min(u16::MAX as usize) as u16;
        self.min_ss = self.min_ss.max(payload_size);
        self.max_ss = self.max_ss.max(self.min_ss);
    }

    pub fn mss(&self) -> u16 {
        self.min_ss
    }

    pub fn max_ss(&self) -> u16 {
        self.max_ss
    }

    pub fn next_segment_size(&mut self) -> u16 {
        if self.cooldown_remaining_packets == 0 {
            return self.next_probe();
        }
        self.cooldown_remaining_packets = self.cooldown_remaining_packets.saturating_sub(1);
        self.min_ss
    }

    fn next_probe(&self) -> u16 {
        self.min_ss + (self.max_ss - self.min_ss) / 2
    }

    pub fn is_probing(&self) -> bool {
        self.next_probe() > self.min_ss
    }

    pub fn on_probe_expired(&mut self, size: usize) {
        self.max_ss = self.max_ss.min(size as u16).max(self.min_ss);
        self.cooldown_remaining_packets = PROBE_EXPIRY_COOLDOWN_PACKETS;
    }
}
