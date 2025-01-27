use std::{
    io::Write,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use anyhow::{bail, Context};
pub use librqbit_utp::UtpSocket;
use librqbit_utp::UtpStream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use tracing::{error_span, info, Instrument};

const TICK_INTERVAL: Duration = Duration::from_millis(1);
const TIMEOUT: Duration = Duration::from_secs(1);

async fn echo(stream: UtpStream) -> anyhow::Result<()> {
    let (reader, writer) = stream.split();

    let mut reader = tokio::io::BufReader::new(reader);

    let reader = async move {
        let mut expected = 0;
        loop {
            let current = timeout(TIMEOUT, reader.read_u64())
                .await
                .context("timeout reading")?
                .context("error reading")?;
            if current != expected {
                bail!("expected {expected}, got {current}");
            }

            if current % 100 == 0 {
                info!("current counter {current}");
            }
            expected += 1;
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    let writer = async move {
        let mut counter = 0;
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        let mut writer = writer;
        loop {
            interval.tick().await;
            timeout(TIMEOUT, writer.write_u64(counter))
                .await
                .context("timeout writing")?
                .context("error writing")?;
            counter += 1;
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    tokio::pin!(reader);
    tokio::pin!(writer);

    tokio::select! {
        r = &mut reader => r,
        r = &mut writer => r
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "trace");
    }
    let _ = tracing_subscriber::fmt::try_init();

    let client = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 8001);
    let server = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 8002);

    let client = tokio::spawn(
        async move {
            let client = UtpSocket::new(client)
                .await
                .context("error creating socket")?;
            let sock = timeout(TIMEOUT, client.connect(server))
                .await
                .context("timeout connecting")?
                .context("error connecting")?;
            echo(sock).await
        }
        .instrument(error_span!("client")),
    );

    let server = tokio::spawn(
        async move {
            let server = UtpSocket::new(server)
                .await
                .context("error creating socket")?;
            let sock = server.accept().await.context("error accepting")?;
            echo(sock).await
        }
        .instrument(error_span!("server")),
    );

    tokio::select! {
        r = client => r.context("error joining client")?.context("client died"),
        r = server => r.context("error joining server")?.context("server died"),
    }
}
