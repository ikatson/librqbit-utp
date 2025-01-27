use std::sync::{Arc, Weak};

use crossbeam_queue::ArrayQueue;

#[derive(Default, Clone)]
pub struct Packet {
    id: usize,
    buf: Vec<u8>,
    pool: Weak<PacketPool>,
}

impl Packet {
    fn return_to_pool(&mut self) {
        if let Some(pool) = self.pool.upgrade() {
            pool.return_packet(self);
        }
    }

    pub fn get_mut(&mut self) -> &mut [u8] {
        &mut self.buf
    }

    pub fn get(&self) -> &[u8] {
        &self.buf
    }
}

impl Drop for Packet {
    fn drop(&mut self) {
        self.return_to_pool();
    }
}

impl std::fmt::Debug for Packet {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Packet")
            .field("id", &self.id)
            .finish_non_exhaustive()
    }
}

pub(crate) struct PacketPool {
    queue: ArrayQueue<Packet>,
    notify: tokio::sync::Notify,
}

impl PacketPool {
    pub fn new(capacity: usize, packet_size: usize) -> Arc<Self> {
        let pool = Arc::new(Self {
            queue: ArrayQueue::new(capacity),
            notify: tokio::sync::Notify::new(),
        });
        for id in 0..capacity {
            let packet = Packet {
                id,
                buf: vec![0u8; packet_size],
                pool: Arc::downgrade(&pool),
            };
            pool.queue.push(packet).unwrap();
        }

        pool
    }

    pub async fn get(&self) -> Packet {
        loop {
            let notify = self.notify.notified();
            if let Some(packet) = self.queue.pop() {
                return packet;
            }
            notify.await;
        }
    }

    pub fn return_packet(&self, packet: &mut Packet) {
        self.queue.push(std::mem::take(packet)).unwrap();
        self.notify.notify_waiters();
    }
}
