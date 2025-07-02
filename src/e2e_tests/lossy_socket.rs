use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};

use dontfrag::UdpSocketExt;
use librqbit_dualstack_sockets::PollSendToVectored;
use rand::Rng;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::UdpSocket,
};

use crate::{SocketOpts, Transport, UtpSocket, traits::DefaultUtpEnvironment};

use super::AcceptConnect;

pub struct LossyUdpSocket<const LOSS_PCT: usize> {
    socket: UdpSocket,
    sent: AtomicUsize,
    lost: AtomicUsize,
}

impl<const LOSS_PCT: usize> LossyUdpSocket<LOSS_PCT> {
    pub async fn bind(addr: SocketAddr) -> anyhow::Result<Self> {
        let socket = UdpSocket::bind(addr).await?;
        socket.set_dontfrag_v4(true)?;
        Ok(Self {
            socket,
            sent: AtomicUsize::new(0),
            lost: AtomicUsize::new(0),
        })
    }

    fn loss(&self) -> bool {
        // First packet shouldn't be lost as we don't retry SYNs yet
        if self
            .sent
            .compare_exchange(0, 1, Ordering::Relaxed, Ordering::Relaxed)
            .is_ok()
        {
            return false;
        }
        let loss = rand::rng().random_bool(LOSS_PCT as f64 / 100.);
        if loss {
            self.lost.fetch_add(1, Ordering::Relaxed);
        } else {
            self.sent.fetch_add(1, Ordering::Relaxed);
        }
        loss
    }
}

impl<const LOSS_PCT: usize> Transport for LossyUdpSocket<LOSS_PCT> {
    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> impl Future<Output = std::io::Result<(usize, SocketAddr)>> + Send + Sync + 'a {
        UdpSocket::recv_from(&self.socket, buf)
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> impl Future<Output = std::io::Result<usize>> + Send + Sync + 'a {
        let loss = self.loss();
        async move {
            if loss {
                return Ok(buf.len());
            }
            self.socket.send_to(buf, target).await
        }
    }

    fn poll_send_to(
        &self,
        cx: &mut Context<'_>,
        buf: &[u8],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        if self.loss() {
            return Poll::Ready(Ok(buf.len()));
        }
        UdpSocket::poll_send_to(&self.socket, cx, buf, target)
    }

    fn bind_addr(&self) -> SocketAddr {
        UdpSocket::local_addr(&self.socket)
            .unwrap_or(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
    }
}

impl<const LOSS_PCT: usize> PollSendToVectored for LossyUdpSocket<LOSS_PCT> {
    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        bufs: &[std::io::IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<std::io::Result<usize>> {
        if self.loss() {
            return Poll::Ready(Ok(bufs.iter().map(|b| b.len()).sum()));
        }
        self.socket.poll_send_to_vectored(cx, bufs, target)
    }
}

pub type LossyUtpUdpSocket<const LOSS_PCT: usize> =
    Arc<UtpSocket<LossyUdpSocket<LOSS_PCT>, DefaultUtpEnvironment>>;

impl<const LOSS_PCT: usize> AcceptConnect for LossyUtpUdpSocket<LOSS_PCT> {
    async fn accept(&self) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        let s = UtpSocket::accept(self).await.unwrap();
        s.split()
    }

    async fn connect(&self, addr: SocketAddr) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        UtpSocket::connect(self, addr).await.unwrap().split()
    }

    async fn bind(addr: SocketAddr) -> Self {
        let socket = LossyUdpSocket::bind(addr).await.unwrap();
        UtpSocket::new_with_opts(
            socket,
            DefaultUtpEnvironment::default(),
            SocketOpts {
                congestion: crate::CongestionConfig {
                    tracing: true,
                    ..Default::default()
                },
                dont_wait_for_lastack: false,
                remote_inactivity_timeout: Some(Duration::from_secs(10)),
                mtu_probe_max_retransmissions: Some(3),
                ..Default::default()
            },
        )
        .unwrap()
    }

    fn bind_addr(&self) -> Option<SocketAddr> {
        Some(self.transport.bind_addr())
    }
}
