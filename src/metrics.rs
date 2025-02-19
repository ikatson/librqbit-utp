#[cfg(feature = "export-metrics")]
macro_rules! create_metrics {
    (
        $(
            $type:ident $name:ident
        ),*
        $(,)?
    ) => {
        use lazy_static::lazy_static;
        use metrics::{
            counter, gauge, histogram, Counter as counter, Gauge as gauge, Histogram as histogram,
        };

        lazy_static! {
            pub static ref METRICS: Metrics = Metrics::new();
        }

        pub struct Metrics {
            $(
                pub $name: $type,
            )*
        }

        impl Metrics {
            pub fn new() -> Self {
                Self {
                    $(
                        $name: $type!(
                            concat!("utp_", stringify!($name))
                        ),
                    )*
                }
            }
        }
    };
}

#[cfg(not(feature = "export-metrics"))]
macro_rules! create_metrics {
    (
        $(
            $type:ident $name:ident
        ),*
        $(,)?
    ) => {
        pub static METRICS: Metrics = Metrics::new();
        pub struct Dummy;
        impl Dummy {
            pub fn increment<T>(&self, _: T) {}
            pub fn decrement<T>(&self, _: T) {}
            pub fn record<T>(&self, _: T) {}
        }
        pub struct Metrics {
            $(
                pub $name: Dummy,
            )*
        }

        impl Metrics {
            pub const fn new() -> Self {
                Self {
                    $(
                        $name: Dummy,
                    )*
                }
            }
        }
    };
}

create_metrics!(
    gauge accepting,
    counter accepts,
    gauge connecting,
    counter connection_attempts,
    counter connection_failures,
    counter connection_successes,
    counter consumed_bytes,
    counter consumed_data_seq_nrs,
    counter duplicate_acks_received,
    counter inactivity_timeouts,
    counter immediate_acks,
    counter delayed_acks,
    gauge live_virtual_sockets,
    counter out_of_order_packets,
    counter data_retransmissions,
    counter synack_retransmissions,
    counter retransmitted_bytes,
    histogram rtt,
    counter send_count,
    counter send_errors,
    counter send_poll_pending,
    counter send_window_exhausted,
    counter max_retransmissions_reached,
    counter sent_bytes,
    counter sent_control_packets,
    counter unsent_control_packets,
    counter incoming_already_acked_data_packets,
    counter recovery_retransmitted_segments_count,
    counter recovery_enter_count,
    counter rto_timeouts_count,
    counter recovery_transmitted_new_segments_count,
    counter recovery_rto_during_recovery_count
);

#[cfg(feature = "per-connection-metrics")]
pub struct PerConnectionMetrics {
    pub cwnd: ::metrics::Gauge,
    pub sshthresh: ::metrics::Gauge,
    pub flight_size: ::metrics::Gauge,
    pub sent_bytes: ::metrics::Counter,
    pub last_remote_window: ::metrics::Gauge,
}

#[cfg(feature = "per-connection-metrics")]
impl PerConnectionMetrics {
    pub fn new(remote: std::net::SocketAddr) -> Self {
        let labels = [("remote_addr", format!("{}", remote))];
        Self {
            cwnd: ::metrics::gauge!("utp_conn_cwnd", &labels),
            sshthresh: ::metrics::gauge!("utp_conn_sshthresh", &labels),
            flight_size: ::metrics::gauge!("utp_conn_flightsize", &labels),
            sent_bytes: ::metrics::counter!("utp_conn_sent_bytes", &labels),
            last_remote_window: ::metrics::gauge!("utp_last_remote_window", &labels),
        }
    }
}
