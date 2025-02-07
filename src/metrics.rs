use lazy_static::lazy_static;
use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

pub struct Metrics {
    pub connection_attempts: Counter,
    pub connection_successes: Counter,
    pub connection_failures: Counter,

    pub accepting: Gauge,
    pub accepts: Counter,
    pub connecting: Gauge,

    pub send_count: Counter,
    pub sent_bytes: Counter,
    pub send_poll_pending: Counter,
    pub send_errors: Counter,
    pub retransmissions: Counter,
    pub live_virtual_sockets: Gauge,
    pub retransmitted_bytes: Counter,
    pub sent_control_packets: Counter,
    pub unsent_control_packets: Counter,
    pub duplicate_acks_received: Counter,
    pub out_of_order_packets: Counter,
    pub rtt: Histogram,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            connection_attempts: counter!("utp_connection_attempts"),
            connection_successes: counter!("utp_connection_successes"),
            connection_failures: counter!("utp_connection_failures"),
            connecting: gauge!("utp_connecting"),
            accepting: gauge!("utp_accepting"),
            accepts: counter!("utp_accepts"),
            send_count: counter!("utp_send_count"),
            sent_bytes: counter!("utp_sent_bytes"),
            send_poll_pending: counter!("utp_send_poll_pending"),
            retransmissions: counter!("utp_retransmissions"),
            retransmitted_bytes: counter!("utp_retransmisted_bytes"),
            sent_control_packets: counter!("utp_sent_control_packets"),
            unsent_control_packets: counter!("utp_unsent_control_packets"),
            send_errors: counter!("utp_send_errors"),
            live_virtual_sockets: gauge!("utp_live_virtual_sockets"),
            duplicate_acks_received: counter!("utp_duplicate_acks_received"),
            out_of_order_packets: counter!("utp_out_of_order_packets"),
            rtt: histogram!("utp_rtt"),
        }
    }
}

lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}
