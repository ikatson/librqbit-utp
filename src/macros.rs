macro_rules! once_every_ms {
    ($dur:expr, $code:tt) => {{
        static LAST_RUN: ::std::sync::atomic::AtomicU64 = ::std::sync::atomic::AtomicU64::new(0);

        if let Ok(now) = std::time::SystemTime::now().duration_since(::std::time::UNIX_EPOCH) {
            let last = LAST_RUN.load(::std::sync::atomic::Ordering::Relaxed);
            let now = now.as_millis() as u64;
            if (now - last) > $dur {
                if let Ok(_) = LAST_RUN.compare_exchange_weak(
                    last,
                    now,
                    std::sync::atomic::Ordering::Relaxed,
                    std::sync::atomic::Ordering::Relaxed,
                ) {
                    $code
                }
            }
        }
    }};
}

macro_rules! log_every_ms {
    ($dur:expr, $level:expr, $($rest:tt)*) => {
        if tracing::enabled!($level) {
            once_every_ms!($dur, {
                tracing::event!($level, $($rest)*);
            });
        }
    };
}

#[allow(unused)]
macro_rules! trace_every_ms {
    ($dur:expr, $($rest:tt)*) => {
        log_every_ms!($dur, tracing::Level::TRACE, $($rest)*);
    };
}

#[allow(unused)]
macro_rules! debug_every_ms {
    ($dur:expr, $($rest:tt)*) => {
        log_every_ms!($dur, tracing::Level::DEBUG, $($rest)*);
    };
}

#[allow(unused)]
macro_rules! warn_every_ms {
    ($dur:expr, $($rest:tt)*) => {
        log_every_ms!($dur, tracing::Level::WARN, $($rest)*);
    };
}

// obj, calc, maybe_change, callback
macro_rules! log_every_ms_if_changed {
    ($dur:expr, $level:expr, $name:expr, $obj:expr, $calc:expr, $maybe_change:expr) => {
        crate::utils::run_before_and_after_if_changed($obj, $calc, $maybe_change, |_, before, after| {
            log_every_ms!($dur, $level, before=?before, after=?after, "{} changed", $name);
        });
    };
}

#[cfg(test)]
mod tests {
    use crate::{constants::CONGESTION_TRACING_LOG_LEVEL, test_util::setup_test_logging};

    #[test]
    fn test_log_every_msg() {
        setup_test_logging();

        for _ in 0..5 {
            log_every_ms!(
                100,
                CONGESTION_TRACING_LOG_LEVEL,
                arg1 = 1,
                arg2 = 2,
                arg3 = 3,
                "retransmitting"
            );
        }
    }
}
