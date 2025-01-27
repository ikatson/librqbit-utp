use std::{
    io::Write,
    net::{Ipv4Addr, SocketAddr},
    time::Duration,
};

use anyhow::Context;
pub use librqbit_utp::UtpSocket;
use librqbit_utp::UtpStream;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use tracing::{error_span, info, Instrument};

const TICK_INTERVAL: Duration = Duration::from_millis(1);
const TIMEOUT: Duration = Duration::from_secs(1);

async fn echo(stream: UtpStream, name: &str) -> anyhow::Result<()> {
    let (mut read, mut write) = stream.split();

    let reader = async move {
        let mut buf = vec![0u8; 1024];
        loop {
            let len = timeout(TIMEOUT, read.read(&mut buf))
                .await
                .context("timeout reading")?
                .context("error reading")?;
            info!("received {:?}", std::str::from_utf8(&buf[..len]));
        }
        #[allow(unreachable_code)]
        Ok::<_, anyhow::Error>(())
    };

    let writer = async move {
        let mut counter = 0;
        let mut buf = vec![0u8; 1024];
        let mut interval = tokio::time::interval(TICK_INTERVAL);
        loop {
            interval.tick().await;
            buf.clear();
            writeln!(&mut buf, "{}: {}", name, counter).unwrap();
            counter += 1;
            timeout(TIMEOUT, write.write_all(&buf))
                .await
                .context("timeout writing")?
                .context("error writing")?;
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
            echo(sock, "client").await
        }
        .instrument(error_span!("client")),
    );

    let server = tokio::spawn(
        async move {
            let server = UtpSocket::new(server)
                .await
                .context("error creating socket")?;
            let sock = server.accept().await.context("error accepting")?;
            echo(sock, "server").await
        }
        .instrument(error_span!("server")),
    );

    tokio::select! {
        r = client => r.context("error joining client")?.context("client died"),
        r = server => r.context("error joining server")?.context("server died"),
    }
}
