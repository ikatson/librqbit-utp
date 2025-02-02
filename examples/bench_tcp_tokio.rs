use std::{
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use rand::Rng;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpSocket, TcpStream},
    time::timeout,
};
use tracing::info;

const TIMEOUT: Duration = Duration::from_secs(5);
const PRINT_INTERVAL: Duration = Duration::from_secs(1);
const BUFFER_SIZE: usize = 1452; // UTP payload with MTU 1500

async fn sender(mut stream: TcpStream) -> anyhow::Result<()> {
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

async fn receiver(mut stream: TcpStream) -> anyhow::Result<()> {
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
                // Configure socket options for better performance
                let socket = TcpSocket::new_v4().context("error creating socket")?;
                socket.set_reuseaddr(true)?;
                socket.bind(server_addr)?;

                let listener = socket.listen(1024)?;
                let listener = TcpListener::from_std(listener.into_std()?)?;

                // Accept connection
                let (stream, _) = listener
                    .accept()
                    .await
                    .context("error accepting connection")?;

                receiver(stream).await.context("error running receiver")
            }
            "client" => {
                // Configure socket options for better performance
                let socket = TcpSocket::new_v4().context("error creating socket")?;
                socket.set_reuseaddr(true)?;
                socket.bind(client_addr)?;

                // Connect
                let stream = timeout(TIMEOUT, socket.connect(server_addr))
                    .await
                    .context("timeout connecting")?
                    .context("error connecting")?;

                sender(stream).await.context("error running sender")
            }
            _ => bail!("Invalid argument. Use 'server' or 'client'"),
        }
    }
}
