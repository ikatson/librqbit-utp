use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
};

use parking_lot::Mutex;

use crate::Transport;

struct MockUtpTransportInner {}

pub struct MockUtpTransport {
    inner: Arc<Mutex<MockUtpTransportInner>>,
}

const DEFAULT_BIND_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 5000);

impl Transport for MockUtpTransport {
    async fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> std::io::Result<(usize, std::net::SocketAddr)> {
        Ok((0, "0.0.0.0:0".parse().unwrap()))
    }

    async fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: std::net::SocketAddr,
    ) -> std::io::Result<usize> {
        Ok(0)
    }

    fn poll_send_to(
        &self,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        target: std::net::SocketAddr,
    ) -> std::task::Poll<std::io::Result<usize>> {
        todo!()
    }

    fn bind_addr(&self) -> std::net::SocketAddr {
        DEFAULT_BIND_ADDR
    }
}
