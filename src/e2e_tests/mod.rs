mod lossy_socket;

use std::{
    net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::Context;
use libutp_rs2::UtpUdpContext;
use lossy_socket::LossyUtpUdpSocket;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
    try_join,
};
use tracing::{Instrument, error_span, info};

use crate::{SocketOpts, UtpSocketUdp, test_util::setup_test_logging};

trait AcceptConnect {
    async fn bind(addr: SocketAddr) -> Self;
    async fn accept(&self) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin);
    async fn connect(&self, addr: SocketAddr) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin);
    fn bind_addr(&self) -> Option<SocketAddr>;
}

impl AcceptConnect for Arc<UtpSocketUdp> {
    async fn accept(&self) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        let s = UtpSocketUdp::accept(self).await.unwrap();
        s.split()
    }

    async fn connect(&self, addr: SocketAddr) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        UtpSocketUdp::connect(self, addr).await.unwrap().split()
    }

    async fn bind(addr: SocketAddr) -> Self {
        UtpSocketUdp::new_udp_with_opts(
            addr,
            SocketOpts {
                congestion: crate::CongestionConfig {
                    tracing: true,
                    ..Default::default()
                },
                ..Default::default()
            },
        )
        .await
        .unwrap()
    }

    fn bind_addr(&self) -> Option<SocketAddr> {
        Some(self.transport.bind_addr())
    }
}

impl AcceptConnect for Arc<UtpUdpContext> {
    async fn accept(&self) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        let stream = UtpUdpContext::accept(self).await.unwrap();
        tokio::io::split(stream)
    }

    async fn connect(&self, addr: SocketAddr) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin) {
        let s = UtpUdpContext::connect(self, addr).await.unwrap();
        tokio::io::split(s)
    }

    async fn bind(addr: SocketAddr) -> Self {
        UtpUdpContext::new_udp(addr).await.unwrap()
    }

    fn bind_addr(&self) -> Option<SocketAddr> {
        None
    }
}

pub const TIMEOUT: Duration = Duration::from_secs(50);

async fn echo(
    reader: impl AsyncRead + Unpin,
    writer: impl AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    let mut reader = reader;

    const MAX_COUNTER: u64 = 10_000;
    const PRINT_EVERY: u64 = 1_000;

    let reader = async move {
        for expected in 0..=MAX_COUNTER {
            let current = timeout(TIMEOUT, reader.read_u64())
                .await
                .context("timeout reading")?
                .context("error reading")?;
            if current != expected {
                anyhow::bail!("expected {expected}, got {current}");
            }

            if current % PRINT_EVERY == 0 {
                info!("current counter {current}");
            }
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    let writer = async move {
        let mut writer = writer;
        for counter in 0..=MAX_COUNTER {
            timeout(TIMEOUT, writer.write_u64(counter))
                .await
                .context("timeout writing")?
                .context("error writing")?;
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    try_join!(reader, writer)?;
    Ok(())
}

async fn test_one_echo<T1: AcceptConnect, T2: AcceptConnect>(
    server_addr: SocketAddr,
    client_addr: SocketAddr,
    connect_ip: Option<IpAddr>,
) {
    setup_test_logging();

    let (server, client) = tokio::join!(T1::bind(server_addr), T2::bind(client_addr));
    let server_port = server
        .bind_addr()
        .map(|a| a.port())
        .unwrap_or(server_addr.port());
    let connect_ip = connect_ip.unwrap_or(server_addr.ip());
    let connect_addr = SocketAddr::from((connect_ip, server_port));

    tracing::debug!(s1_bind_addr=?server.bind_addr(), s2_bind_addr=?client.bind_addr(), connect_addr=?connect_addr);

    let server = async {
        let s = server;
        let (r, w) = s.accept().await;
        echo(r, w).await.unwrap()
    }
    .instrument(error_span!("echo", addr = ?server_addr));

    let client = async {
        let s = client;
        let (r, w) = s.connect(connect_addr).await;
        echo(r, w).await.unwrap()
    }
    .instrument(error_span!("echo", addr = ?client_addr));

    tokio::join!(server, client);
}

fn localhost_ipv4(port: u16) -> SocketAddr {
    (Ipv4Addr::LOCALHOST, port).into()
}

fn localhost_ipv6(port: u16) -> SocketAddr {
    (Ipv6Addr::LOCALHOST, port).into()
}

fn unspecified_ipv6(port: u16) -> SocketAddr {
    (Ipv6Addr::UNSPECIFIED, port).into()
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server_ipv6() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv6(0),
        localhost_ipv6(0),
        None,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server_dualstack() {
    let f = test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>;

    f(
        unspecified_ipv6(0),
        unspecified_ipv6(0),
        Some(Ipv4Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack both over IPv4"))
    .await;

    f(localhost_ipv6(0), localhost_ipv6(0), None).await;

    f(
        unspecified_ipv6(0),
        unspecified_ipv6(0),
        Some(Ipv6Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack both over IPv6"))
    .await;

    f(
        unspecified_ipv6(0),
        localhost_ipv4(0),
        Some(Ipv4Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack server over IPv4"))
    .await;

    f(
        unspecified_ipv6(0),
        localhost_ipv6(0),
        Some(Ipv6Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack server over IPv6"))
    .await;

    f(
        localhost_ipv4(0),
        unspecified_ipv6(0),
        Some(Ipv4Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("dualstack client over IPv4"))
    .await;

    f(
        localhost_ipv6(0),
        localhost_ipv6(0),
        Some(Ipv6Addr::LOCALHOST.into()),
    )
    .instrument(error_span!("ipv6-only localhost"))
    .await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_libutp_rs2_server() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<libutp_rs2::UtpUdpContext>>(
        localhost_ipv4(8530),
        localhost_ipv4(8531),
        None,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_libutp_rs2_client_librqbit_utp_server() {
    test_one_echo::<Arc<libutp_rs2::UtpUdpContext>, Arc<crate::UtpSocketUdp>>(
        localhost_ipv4(8534),
        localhost_ipv4(8535),
        None,
    )
    .await;
}

#[tokio::test]
async fn e2e_test_libutp_rs2_client_libutp_rs2_server() {
    test_one_echo::<Arc<libutp_rs2::UtpUdpContext>, Arc<libutp_rs2::UtpUdpContext>>(
        localhost_ipv4(8536),
        localhost_ipv4(8537),
        None,
    )
    .await;
}

#[ignore]
#[tokio::test]
async fn e2e_test_loss_5_pct() {
    test_one_echo::<LossyUtpUdpSocket<5>, LossyUtpUdpSocket<5>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
    )
    .await
}

#[ignore]
#[tokio::test]
async fn e2e_test_loss_20_pct() {
    test_one_echo::<LossyUtpUdpSocket<20>, LossyUtpUdpSocket<20>>(
        localhost_ipv4(0),
        localhost_ipv4(0),
        None,
    )
    .await
}
