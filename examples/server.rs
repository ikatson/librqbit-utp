use std::{
    net::SocketAddr,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use librqbit_utp::{UtpSocket, UtpStream};
use tokio::io::AsyncReadExt;
use tokio::time::timeout;
use tracing::info;

const TIMEOUT: Duration = Duration::from_secs(5);
const PRINT_INTERVAL: Duration = Duration::from_secs(1);
const BUFFER_SIZE: usize = 16384;

async fn receiver(mut stream: UtpStream) -> anyhow::Result<()> {
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
    tracing_subscriber::fmt::init();

    let mut args = std::env::args().skip(1);

    let bind_addr: SocketAddr = args
        .next()
        .expect("first arg should be address")
        .parse()
        .unwrap();

    let listener = UtpSocket::new_udp(bind_addr)
        .await
        .context("error creating socket")?;
    loop {
        let sock = listener.accept().await.context("error accepting")?;
        tokio::spawn(receiver(sock));
    }
}
