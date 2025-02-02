use std::{
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use librqbit_utp::{SocketOpts, UtpSocket, UtpStreamUdp};
use rand::Rng;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::time::timeout;
use tracing::info;

const TIMEOUT: Duration = Duration::from_secs(5);
const PRINT_INTERVAL: Duration = Duration::from_secs(1);
const BUFFER_SIZE: usize = 16384; // 16KB buffer

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

    let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 8001).into();
    let server_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 8002).into();

    let opts = SocketOpts {
        // mtu: Some(8192),
        // mtu_autodetect_host: Some(Ipv4Addr::LOCALHOST.into()),
        ..Default::default()
    };

    // Check command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        // If no arguments, spawn both client and server processes
        let server_child = std::process::Command::new(&args[0])
            .arg("server")
            .spawn()
            .context("Failed to spawn server process")?;

        // Wait a bit for the server to start
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client_child = std::process::Command::new(&args[0])
            .arg("client")
            .spawn()
            .context("Failed to spawn client process")?;

        // Wait for both processes to complete
        server_child.wait_with_output()?;
        client_child.wait_with_output()?;

        Ok(())
    } else {
        match args[1].as_str() {
            "server" => {
                let listener = UtpSocket::new_udp_with_opts(server_addr, opts)
                    .await
                    .context("error creating socket")?;
                let sock = listener.accept().await.context("error accepting")?;
                receiver(sock).await.context("error running receiver")
            }
            "client" => {
                let client = UtpSocket::new_udp_with_opts(client_addr, opts)
                    .await
                    .context("error creating socket")?;
                let sock = timeout(TIMEOUT, client.connect(server_addr))
                    .await
                    .context("timeout connecting")?
                    .context("error connecting")?;
                sender(sock).await.context("error running sender")
            }
            _ => bail!("Invalid argument. Use 'server' or 'client'"),
        }
    }
}
