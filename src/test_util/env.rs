use std::{sync::Arc, time::Instant};

use parking_lot::Mutex;

use crate::traits::UtpEnvironment;

pub struct MockRandom {
    pub current: usize,
    pub all: Vec<u16>,
}

impl Default for MockRandom {
    fn default() -> Self {
        Self {
            current: 0,
            all: (0..10).collect(),
        }
    }
}

impl MockRandom {
    fn next(&mut self) -> Option<u16> {
        let current = self.current;
        self.current += 1;
        self.all.get(current).copied()
    }
}

struct MockUtpEnvironmentInner {
    now: Instant,
    random: MockRandom,
}

#[derive(Clone)]
pub struct MockUtpEnvironment {
    inner: Arc<Mutex<MockUtpEnvironmentInner>>,
}

impl MockUtpEnvironment {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(MockUtpEnvironmentInner {
                now: Instant::now(),
                random: Default::default(),
            })),
        }
    }

    pub fn set_now(&self, now: Instant) {
        self.inner.lock().now = now;
    }
}

impl UtpEnvironment for MockUtpEnvironment {
    fn now(&self) -> std::time::Instant {
        self.inner.lock().now
    }

    fn copy(&self) -> Self {
        self.clone()
    }

    fn random_u16(&self) -> u16 {
        self.inner
            .lock()
            .random
            .next()
            .expect("exhausted random numbers")
    }
}
