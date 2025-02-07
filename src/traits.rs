use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    task::{Context, Poll},
    time::Instant,
};

use tokio::net::UdpSocket;

use crate::metrics::METRICS;

pub trait Transport: Send + Sync + Unpin + 'static {
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> + Send + Sync + 'a;

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> impl Future<Output = std::io::Result<usize>> + Send + Sync + 'a;

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>>;

    // The local address transport is bound to. Used only for logging.
    fn bind_addr(&self) -> SocketAddr;
}

impl Transport for UdpSocket {
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> + Send + Sync + 'a {
        UdpSocket::recv_from(self, buf)
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> impl Future<Output = std::io::Result<usize>> + Send + Sync + 'a {
        UdpSocket::send_to(self, buf, target)
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        let res = UdpSocket::poll_send_to(self, cx, buf, target);
        if res.is_pending() {
            METRICS.send_poll_pending.increment(1);
        }
        res
    }

    fn bind_addr(&self) -> SocketAddr {
        UdpSocket::local_addr(self).unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
    }
}

// A trait for mocking stuff in tests.
pub trait UtpEnvironment: Send + Sync + Unpin + 'static {
    fn now(&self) -> Instant;
    fn copy(&self) -> Self;
    fn random_u16(&self) -> u16;
}

#[derive(Default, Clone, Copy)]
pub struct DefaultUtpEnvironment {}

impl UtpEnvironment for DefaultUtpEnvironment {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn copy(&self) -> Self {
        *self
    }

    fn random_u16(&self) -> u16 {
        rand::random()
    }
}
