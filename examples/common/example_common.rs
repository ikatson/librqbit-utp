use std::future::Future;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::time::{Duration, Instant};

use anyhow::bail;
use anyhow::Context;
use metrics::histogram;
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

pub async fn bench_receiver(mut stream: impl AsyncRead + Unpin) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut total_bytes = 0u64;
    let start = Instant::now();
    let mut last_print = start;

    let m_read_len = histogram!("utp_bench_read_len");

    loop {
        match timeout(TIMEOUT, stream.read(&mut buffer)).await {
            Ok(Ok(len)) => {
                m_read_len.record(len as f64);
                total_bytes += len as u64;
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

pub async fn bench_sender(mut stream: impl AsyncWrite + Unpin) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    rand::thread_rng().fill(buffer.as_mut_slice());

    let m_send_len = histogram!("utp_bench_send_len");

    loop {
        match timeout(TIMEOUT, stream.write(&buffer)).await {
            Ok(Ok(len)) => {
                m_send_len.record(len as f64);
            }
            Ok(Err(e)) => bail!("Error writing: {}", e),
            Err(_) => bail!("Timeout while writing"),
        }
    }
}

pub async fn echo(
    reader: impl AsyncRead + Unpin,
    writer: impl AsyncWrite + Unpin,
) -> anyhow::Result<()> {
    let mut reader = reader;

    // const MAX_COUNTER: u64 = 1_000_000;
    // const PRINT_EVERY: u64 = 100_000;

    const MAX_COUNTER: u64 = 1000;
    const PRINT_EVERY: u64 = 100;

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
        Ok(reader)
    };

    let writer = async move {
        let mut writer = writer;
        for counter in 0..=MAX_COUNTER {
            timeout(TIMEOUT, writer.write_u64(counter))
                .await
                .context("timeout writing")?
                .context("error writing")?;
        }
        Ok(writer)
    };

    let (mut reader, mut writer) = try_join!(reader, writer)?;

    // Ensure we got everything sent and ACKed. Otherwise we'll quit and tokio will die killing everything in the
    // process before we were able to send it all.
    if let Err(e) = timeout(TIMEOUT, writer.shutdown())
        .await
        .context("timeout shutting down")?
    {
        // libutp2-rs doesn't implement shutdown, so if it errored, just wait a bit, that's all we can do.
        if e.to_string().contains("not implemented") {
            // Ensure it sends everything
            tokio::time::sleep(Duration::from_millis(500)).await;
            timeout(TIMEOUT, writer.shutdown())
                .await
                .context("timeout shutting down")?
                .context("error shutting down for the second time")?;
        } else {
            return Err(e).context("error shutting down");
        }
    }

    let len = timeout(TIMEOUT, reader.read(&mut [0u8; 8192]))
        .await
        .context("timeout checking for EOF")?
        .context("expected to read EOF")?;
    if len != 0 {
        bail!("read unexpected {len} bytes at the end")
    }

    info!("echo completed successfully");
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
