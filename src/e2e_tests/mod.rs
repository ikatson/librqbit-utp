mod lossy_socket;

use std::{
    net::{Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Context};
use libutp_rs2::UtpUdpContext;
use lossy_socket::LossyUtpUdpSocket;
use tokio::{
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    time::timeout,
    try_join,
};
use tracing::{error_span, info, Instrument};

use crate::{test_util::setup_test_logging, SocketOpts, UtpSocketUdp};

trait AcceptConnect {
    async fn bind(addr: SocketAddr) -> Self;
    async fn accept(&self) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin);
    async fn connect(&self, addr: SocketAddr) -> (impl AsyncRead + Unpin, impl AsyncWrite + Unpin);
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
                bail!("expected {expected}, got {current}");
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

async fn test_one_echo<T1: AcceptConnect, T2: AcceptConnect>(port1: u16, port2: u16) {
    setup_test_logging();

    let addr1: SocketAddr = (Ipv4Addr::LOCALHOST, port1).into();
    let addr2: SocketAddr = (Ipv4Addr::LOCALHOST, port2).into();

    let t1 = async {
        let s = T1::bind(addr1).await;
        let (r, w) = s.accept().await;
        echo(r, w).await.unwrap()
    }
    .instrument(error_span!("echo", port = port1));

    let t2 = async {
        let s = T2::bind(addr2).await;
        let (r, w) = s.connect(addr1).await;
        echo(r, w).await.unwrap()
    }
    .instrument(error_span!("echo", port = port2));

    tokio::join!(t1, t2);
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_librqbit_utp_server() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<crate::UtpSocketUdp>>(8532, 8533).await;
}

#[tokio::test]
async fn e2e_test_librqbit_utp_client_libutp_rs2_server() {
    test_one_echo::<Arc<crate::UtpSocketUdp>, Arc<libutp_rs2::UtpUdpContext>>(8530, 8531).await;
}

#[tokio::test]
async fn e2e_test_libutp_rs2_client_librqbit_utp_server() {
    test_one_echo::<Arc<libutp_rs2::UtpUdpContext>, Arc<crate::UtpSocketUdp>>(8534, 8535).await;
}

#[tokio::test]
async fn e2e_test_libutp_rs2_client_libutp_rs2_server() {
    test_one_echo::<Arc<libutp_rs2::UtpUdpContext>, Arc<libutp_rs2::UtpUdpContext>>(8536, 8537)
        .await;
}

#[ignore]
#[tokio::test]
async fn e2e_test_loss_5_pct() {
    test_one_echo::<LossyUtpUdpSocket<5>, LossyUtpUdpSocket<5>>(8538, 8539).await
}
