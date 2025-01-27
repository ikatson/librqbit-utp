use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use parking_lot::Mutex;

use crate::traits::UtpEnvironment;

pub struct MockRandom {
    pub current: u16,
}

impl Default for MockRandom {
    fn default() -> Self {
        Self { current: 1 }
    }
}

impl MockRandom {
    fn next(&mut self) -> u16 {
        let current = self.current;
        self.current = self.current.wrapping_add(100);
        current
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
}

impl UtpEnvironment for MockUtpEnvironment {
    fn now(&self) -> Instant {
        let mut g = self.inner.lock();
        let ret = g.now;
        g.now += Duration::from_millis(1);
        ret
    }

    fn copy(&self) -> Self {
        self.clone()
    }

    fn random_u16(&self) -> u16 {
        self.inner.lock().random.next()
    }
}
