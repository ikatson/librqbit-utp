use std::time::Duration;

macro_rules! once_every_ms {
    ($dur:expr, $code:tt) => {
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
    };
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tracing::info;

    use crate::test_util::setup_test_logging;

    #[test]
    fn test_once_every_ms() {
        setup_test_logging();
        for i in 0..5 {
            once_every_ms!(100, {
                info!("hello {i}");
            });
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}
