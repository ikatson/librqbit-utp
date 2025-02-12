use std::future::Future;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use anyhow::bail;
use anyhow::Context;
use metrics_exporter_prometheus::PrometheusBuilder;
use rand::Rng;
use tokio::io::AsyncRead;
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWrite;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;
use tokio::try_join;
use tracing::info;

pub const TIMEOUT: Duration = Duration::from_secs(5);
const PRINT_INTERVAL: Duration = Duration::from_secs(1);
const BUFFER_SIZE: usize = 16384;

pub async fn receiver_async(mut stream: impl AsyncRead + Unpin) -> anyhow::Result<()> {
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

pub async fn sender_async(mut stream: impl AsyncWrite + Unpin) -> anyhow::Result<()> {
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

pub fn sender_sync(mut stream: impl Write) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    rand::thread_rng().fill(buffer.as_mut_slice());

    loop {
        stream.write_all(&buffer)?;
    }
}

pub fn receiver_sync(mut stream: impl Read) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut total_bytes = 0u64;
    let start = Instant::now();
    let mut last_print = start;

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => bail!("Connection closed by peer"),
            Ok(received) => {
                total_bytes += received as u64;
                let now = Instant::now();
                if now.duration_since(last_print) >= PRINT_INTERVAL {
                    let elapsed = now.duration_since(start).as_secs_f64();
                    let speed = (total_bytes as f64) / (1024.0 * 1024.0) / elapsed;
                    info!("Receiving speed: {:.2} MB/s", speed);
                    last_print = now;
                }
            }
            Err(e) => bail!("Error reading: {}", e),
        }
    }
}

pub async fn echo(
    reader: impl AsyncRead + Unpin,
    writer: impl AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    let mut reader = reader;

    const MAX_COUNTER: u64 = 1_000_000;
    const PRINT_EVERY: u64 = 100_000;

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

pub async fn flatten<JoinError>(
    handle: impl Future<Output = Result<anyhow::Result<()>, JoinError>>,
) -> anyhow::Result<()> {
    match handle.await {
        Ok(Ok(result)) => Ok(result),
        Ok(Err(err)) => Err(err),
        Err(_) => bail!("joining failed"),
    }
}

pub fn bench_main(
    run_server: impl FnOnce(SocketAddr) -> anyhow::Result<()>,
    run_client: impl FnOnce(SocketAddr) -> anyhow::Result<()>,
) -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

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
        std::thread::sleep(Duration::from_millis(500));

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
            "server" => run_server(server_addr).context("server error"),
            "client" => run_client(server_addr).context("client error"),
            _ => bail!("Invalid argument. Use 'server' or 'client'"),
        }
    }
}

pub async fn bench_main_async<Listener, ListenerFut, Reader, Client, ClientFut, Writer>(
    listener: Listener,
    client: Client,
) -> anyhow::Result<()>
where
    Listener: FnOnce(SocketAddr) -> ListenerFut,
    ListenerFut: Future<Output = anyhow::Result<Reader>>,
    Reader: AsyncRead + Unpin,
    Client: FnOnce(SocketAddr) -> ClientFut,
    ClientFut: Future<Output = anyhow::Result<Writer>>,
    Writer: AsyncWrite + Unpin,
{
    tracing_subscriber::fmt::init();

    let sender_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 8001).into();
    let sender_prometheus_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 9001).into();

    let receiver_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 8002).into();
    let receiver_prometheus_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 9002).into();

    const SENDER: &str = "sender";
    const RECEIVER: &str = "receiver";

    // Check command line arguments
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        // If no arguments, spawn both client and server processes
        let server_child = std::process::Command::new(&args[0])
            .arg(RECEIVER)
            .spawn()
            .context("Failed to spawn server process")?;

        // Wait a bit for the server to start
        tokio::time::sleep(Duration::from_millis(500)).await;

        let client_child = std::process::Command::new(&args[0])
            .arg(SENDER)
            .spawn()
            .context("Failed to spawn client process")?;

        // Wait for both processes to complete
        server_child.wait_with_output()?;
        client_child.wait_with_output()?;

        Ok(())
    } else {
        match args[1].as_str() {
            RECEIVER => {
                let builder = PrometheusBuilder::new().with_http_listener(receiver_prometheus_addr);
                builder
                    .install()
                    .expect("failed to install recorder/exporter");
                let sock = timeout(TIMEOUT, listener(receiver_addr))
                    .await
                    .context("timeout starting listener")?
                    .context("error starting listener")?;
                receiver_async(sock).await.context("error running receiver")
            }
            SENDER => {
                let builder = PrometheusBuilder::new().with_http_listener(sender_prometheus_addr);
                builder
                    .install()
                    .expect("failed to install recorder/exporter");
                let sock = timeout(TIMEOUT, client(sender_addr))
                    .await
                    .context("timeout starting client")?
                    .context("error starting client")?;
                sender_async(sock).await.context("error running sender")
            }
            _ => bail!("Invalid argument. Use 'server' or 'client'"),
        }
    }
}
