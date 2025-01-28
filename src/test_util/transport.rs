use std::{
    collections::HashMap,
    future::{poll_fn, Future},
    net::SocketAddr,
    sync::Arc,
    task::Poll,
};

use parking_lot::Mutex;
use tokio::sync::mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender};
use tracing::{error_span, trace, Instrument};

use crate::{message::UtpMessage, packet_pool::Packet, raw::UtpHeader, Transport};

use super::{env::MockUtpEnvironment, MockDispatcher, MockUtpSocket};

type Msg = (SocketAddr, Vec<u8>);

pub struct MockInterface {
    sockets: Mutex<HashMap<SocketAddr, UnboundedSender<Msg>>>,
    pub env: MockUtpEnvironment,
}

impl MockInterface {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            sockets: Default::default(),
            env: MockUtpEnvironment::new(),
        })
    }

    pub fn create_socket_with_dispatcher(
        self: &Arc<Self>,
        bind_addr: SocketAddr,
    ) -> (Arc<MockUtpSocket>, MockDispatcher) {
        let (tx, rx) = unbounded_channel();
        let transport = MockUtpTransport::new(bind_addr, rx, self.clone());
        let env = self.env.clone();

        let socket =
            MockUtpSocket::new_with_opts_and_dispatcher(transport, env, Default::default())
                .unwrap();

        if self.sockets.lock().insert(bind_addr, tx).is_some() {
            panic!("socket with {} already existed", bind_addr)
        }
        socket
    }

    pub fn create_socket(self: &Arc<Self>, bind_addr: SocketAddr) -> Arc<MockUtpSocket> {
        let (sock, dispatcher) = self.create_socket_with_dispatcher(bind_addr);
        tokio::spawn(
            dispatcher
                .run_forever()
                .instrument(error_span!("sock", ?bind_addr)),
        );
        sock
    }
}

// Without this indirection rust analyzer doesn't work somehow
struct MockUtpTransportInnerLocked {
    rx: UnboundedReceiver<Msg>,
}

struct MockUtpTransportInner {
    locked: Mutex<MockUtpTransportInnerLocked>,
    interface: Arc<MockInterface>,
}

#[derive(Clone)]
pub struct MockUtpTransport {
    inner: Arc<MockUtpTransportInner>,
    bind_addr: SocketAddr,
}

impl MockUtpTransport {
    pub fn new(
        bind_addr: SocketAddr,
        rx: UnboundedReceiver<Msg>,
        interface: Arc<MockInterface>,
    ) -> Self {
        Self {
            inner: Arc::new(MockUtpTransportInner {
                locked: Mutex::new(MockUtpTransportInnerLocked { rx }),
                interface,
            }),
            bind_addr,
        }
    }

    #[tracing::instrument(name = "MockUtpTransport::send", skip(self, buf), fields(?target))]
    pub fn send(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        let (header, len) = UtpHeader::deserialize(buf).unwrap();
        trace!(?header, payload_size = buf.len() - len, "sending");
        let len = buf.len();

        match self.inner.interface.sockets.lock().get(&target) {
            Some(tx) => match tx.send((self.bind_addr, buf.to_owned())) {
                Ok(_) => {}
                Err(_) => return Err(std::io::Error::other(format!("target {target} is dead"))),
            },
            None => return Err(std::io::Error::other(format!("no route to {target}"))),
        };

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

#[derive(Clone)]
pub struct RememberingTransport {
    pub bind_addr: SocketAddr,
    pub messages: Arc<Mutex<Vec<Msg>>>,
}

impl RememberingTransport {
    pub fn new(bind_addr: SocketAddr) -> Self {
        Self {
            bind_addr,
            messages: Default::default(),
        }
    }

    pub fn send(&self, addr: SocketAddr, buf: &[u8]) {
        self.messages.lock().push((addr, buf.to_owned()));
    }

    pub fn take_sent(&self) -> Vec<Msg> {
        std::mem::take(&mut self.messages.lock())
    }

    pub fn take_sent_utpmessages(&self) -> Vec<UtpMessage> {
        let sent = self.take_sent();
        sent.into_iter()
            .map(|(_, msg)| {
                let len = msg.len();
                let packet = Packet::new_test(msg);
                UtpMessage::deserialize(packet, len).unwrap()
            })
            .collect()
    }
}

impl Transport for RememberingTransport {
    fn recv_from<'a>(
        &'a self,
        _buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> {
        std::future::pending()
    }

    async fn send_to<'a>(&'a self, buf: &'a [u8], target: SocketAddr) -> std::io::Result<usize> {
        self.send(target, buf);
        Ok(buf.len())
    }

    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        self.send(target, buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }
}
