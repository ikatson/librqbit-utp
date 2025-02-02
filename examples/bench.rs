use std::{
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use futures::FutureExt;
use librqbit_utp::{UtpSocket, UtpStreamUdp};
use rand::Rng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
    try_join,
};
use tracing::{error_span, info, Instrument};

const TIMEOUT: Duration = Duration::from_secs(5);
const PRINT_INTERVAL: Duration = Duration::from_secs(1);
const BUFFER_SIZE: usize = 16384; // 16KB buffer

async fn flatten<JoinError>(
    handle: impl std::future::Future<Output = Result<anyhow::Result<()>, JoinError>>,
) -> anyhow::Result<()> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(_) => bail!("joining failed"),
    }
}

async fn sender(mut stream: UtpStreamUdp) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    rand::thread_rng().fill(buffer.as_mut_slice());

    loop {
        match timeout(TIMEOUT, stream.write_all(&buffer)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => bail!("Error writing: {}", e),
            Err(_) => bail!("Timeout while writing"),
        }
    }
}

async fn receiver(mut stream: UtpStreamUdp) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut total_bytes = 0u64;
    let start = Instant::now();
    let mut last_print = start;

    loop {
        match timeout(TIMEOUT, stream.read_exact(&mut buffer)).await {
            Ok(Ok(_)) => {
                total_bytes += BUFFER_SIZE as u64;
                let now = Instant::now();
                if now.duration_since(last_print) >= PRINT_INTERVAL {
                    let elapsed = now.duration_since(start).as_secs_f64();
                    let speed = (total_bytes as f64) / (1024.0 * 1024.0) / elapsed;
                    info!("Receiving speed: {:.2} MB/s", speed);
                    last_print = now;
                }
            }
            Ok(Err(e)) => bail!("Error reading: {}", e),
            Err(_) => bail!("Timeout while reading"),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    let _ = tracing_subscriber::fmt::try_init();

    let client_addr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 8001);
    let server_addr = SocketAddr::new(std::net::IpAddr::V4(Ipv4Addr::LOCALHOST), 8002);

    let listener = UtpSocket::new_udp(server_addr)
        .await
        .context("error creating socket")?;

    let client = tokio::spawn(
        async move {
            let client = UtpSocket::new_udp(client_addr)
                .await
                .context("error creating socket")?;
            let sock = timeout(TIMEOUT, client.connect(server_addr))
                .await
                .context("timeout connecting")?
                .context("error connecting")?;
            sender(sock).await.context("error running sender")
        }
        .instrument(error_span!("sender"))
        .map(|v| v.context("sender died")),
    );

    let server = tokio::spawn(
        async move {
            let sock = listener.accept().await.context("error accepting")?;
            receiver(sock).await.context("error running receiver")
        }
        .instrument(error_span!("receiver"))
        .map(|v| v.context("receiver died")),
    );

    try_join!(flatten(client), flatten(server))?;
    Ok(())
}
