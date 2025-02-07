use lazy_static::lazy_static;
use metrics::{counter, gauge, histogram, Counter, Gauge, Histogram};

pub struct Metrics {
    pub accepting: Gauge,
    pub accepts: Counter,
    pub connecting: Gauge,
    pub connection_attempts: Counter,
    pub connection_failures: Counter,
    pub connection_successes: Counter,
    pub consumed_bytes: Counter,
    pub consumed_data_seq_nrs: Counter,
    pub duplicate_acks_received: Counter,
    pub inactivity_timeouts: Counter,
    pub immediate_acks: Counter,
    pub delayed_acks: Counter,
    pub live_virtual_sockets: Gauge,
    pub out_of_order_packets: Counter,
    pub retransmissions: Counter,
    pub retransmitted_bytes: Counter,
    pub rtt: Histogram,
    pub send_count: Counter,
    pub send_errors: Counter,
    pub send_poll_pending: Counter,
    pub send_window_exhausted: Counter,
    pub sent_bytes: Counter,
    pub sent_control_packets: Counter,
    pub unsent_control_packets: Counter,
}

impl Metrics {
    pub fn new() -> Self {
        Self {
            accepting: gauge!("utp_accepting"),
            accepts: counter!("utp_accepts"),
            connecting: gauge!("utp_connecting"),
            connection_attempts: counter!("utp_connection_attempts"),
            connection_failures: counter!("utp_connection_failures"),
            connection_successes: counter!("utp_connection_successes"),
            consumed_bytes: counter!("utp_consumed_bytes"),
            consumed_data_seq_nrs: counter!("utp_consumed_data_seq_nrs"),
            duplicate_acks_received: counter!("utp_duplicate_acks_received"),
            live_virtual_sockets: gauge!("utp_live_virtual_sockets"),
            out_of_order_packets: counter!("utp_out_of_order_packets"),
            retransmissions: counter!("utp_retransmissions"),
            retransmitted_bytes: counter!("utp_retransmisted_bytes"),
            rtt: histogram!("utp_rtt"),
            send_count: counter!("utp_send_count"),
            send_errors: counter!("utp_send_errors"),
            send_poll_pending: counter!("utp_send_poll_pending"),
            send_window_exhausted: counter!("utp_send_window_exhausted"),
            sent_bytes: counter!("utp_sent_bytes"),
            sent_control_packets: counter!("utp_sent_control_packets"),
            unsent_control_packets: counter!("utp_unsent_control_packets"),
            immediate_acks: counter!("utp_immediate_acks"),
            delayed_acks: counter!("utp_delayed_acks"),
            inactivity_timeouts: counter!("utp_inactivity_timeouts"),
        }
    }
}

lazy_static! {
    pub static ref METRICS: Metrics = Metrics::new();
}
