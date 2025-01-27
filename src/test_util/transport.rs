use std::{
    collections::VecDeque,
    future::poll_fn,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    task::Poll,
};

use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};

use crate::Transport;

type Msg = (SocketAddr, Vec<u8>);

// Without this indirection rust analyzer doesn't work somehow
struct MockUtpTransportInnerLocked {
    rx: UnboundedReceiver<Msg>,
}

struct MockUtpTransportInner {
    locked: Mutex<MockUtpTransportInnerLocked>,
    tx: UnboundedSender<Msg>,
}

#[derive(Clone)]
pub struct MockUtpTransport {
    inner: Arc<MockUtpTransportInner>,
}

impl MockUtpTransport {
    pub fn new() -> Self {
        let (tx, rx) = unbounded_channel();
        Self {
            inner: Arc::new(MockUtpTransportInner {
                locked: Mutex::new(MockUtpTransportInnerLocked { rx }),
                tx,
            }),
        }
    }
}

const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000);

impl Transport for MockUtpTransport {
    async fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> std::io::Result<(usize, std::net::SocketAddr)> {
        let f = poll_fn(|cx| self.inner.locked.lock().rx.poll_recv(cx));
        let (addr, data) = f.await.unwrap();
        assert!(data.len() <= buf.len());
        buf[..data.len()].copy_from_slice(&data);
        Ok((data.len(), addr))
    }

    async fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: std::net::SocketAddr,
    ) -> std::io::Result<usize> {
        let len = buf.len();
        self.inner.tx.send((target, buf.to_owned())).unwrap();
        Ok(len)
    }

    fn poll_send_to(
        &self,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: std::net::SocketAddr,
    ) -> std::task::Poll<std::io::Result<usize>> {
        let len = buf.len();
        self.inner.tx.send((target, buf.to_owned())).unwrap();
        Poll::Ready(Ok(len))
    }

    fn bind_addr(&self) -> std::net::SocketAddr {
        DEFAULT_BIND_ADDR
    }
}
