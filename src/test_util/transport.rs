use std::{
    future::poll_fn,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    task::Poll,
};

use anyhow::Context;
use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::trace;

use crate::{raw::UtpHeader, Transport};

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
    bind_addr: SocketAddr,
}

impl MockUtpTransport {
    pub fn new(bind_addr: SocketAddr) -> Self {
        let (tx, rx) = unbounded_channel();
        Self {
            inner: Arc::new(MockUtpTransportInner {
                locked: Mutex::new(MockUtpTransportInnerLocked { rx }),
                tx,
            }),
            bind_addr,
        }
    }

    #[tracing::instrument(name = "MockUtpTransport::send", skip(self, buf), fields(?target))]
    pub fn send(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        let (header, len) = UtpHeader::deserialize(buf).unwrap();
        trace!(?header, payload_size = buf.len() - len, "sending");

        let len = buf.len();
        self.inner.tx.send((target, buf.to_owned())).unwrap();
        Ok(len)
    }
}

impl Transport for MockUtpTransport {
    async fn recv_from<'a>(&'a self, buf: &'a mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        let f = poll_fn(|cx| self.inner.locked.lock().rx.poll_recv(cx));
        let (addr, data) = f.await.unwrap();
        assert!(data.len() <= buf.len());
        buf[..data.len()].copy_from_slice(&data);
        Ok((data.len(), addr))
    }

    async fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> std::io::Result<usize> {
        self.send(buf, target)
    }

    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> std::task::Poll<std::io::Result<usize>> {
        Poll::Ready(self.send(buf, target))
    }

    fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}
