use std::cell::OnceCell;
use std::future::Future;
use std::io::{Read, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, UNIX_EPOCH};

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
const BUFFER_SIZE: usize = 16384;

pub async fn bench_receiver(mut stream: impl AsyncRead + Unpin) -> anyhow::Result<()> {
    // Create static/global counters for all connections
    static LAST_PRINT: AtomicU64 = AtomicU64::new(0);
    static TOTAL_BYTES: AtomicU64 = AtomicU64::new(0);

    // Initialize start time only once
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let m_read_len = histogram!("utp_bench_read_len");

    loop {
        match timeout(TIMEOUT, stream.read(&mut buffer)).await {
            Ok(Ok(len)) => {
                m_read_len.record(len as f64);
                TOTAL_BYTES.fetch_add(len as u64, Ordering::Relaxed);

                let now = UNIX_EPOCH.elapsed().unwrap().as_millis() as u64;
                let prev = LAST_PRINT.load(Ordering::Relaxed);
                if now - prev > 1000
                    && LAST_PRINT
                        .compare_exchange(prev, now, Ordering::Relaxed, Ordering::Relaxed)
                        .is_ok()
                    && prev != 0
                {
                    let total = TOTAL_BYTES
                        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |_| Some(0))
                        .unwrap();
                    let elapsed = (now - prev) as f64 / 1000.;
                    let speed = (total as f64) / (1024.0 * 1024.0) / elapsed;
                    info!("Total receiving speed: {:.2} MB/s", speed);
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
    let m_send_time = histogram!("utp_bench_send_time_us");

    loop {
        let start = Instant::now();
        match timeout(TIMEOUT, stream.write(&buffer)).await {
            Ok(Ok(len)) => {
                m_send_time.record(start.elapsed().as_micros() as f64);
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
