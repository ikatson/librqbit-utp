use std::{net::SocketAddr, time::Duration};

use anyhow::{bail, Context};
use librqbit_utp::{UtpSocket, UtpStream};
use rand::Rng;
use tokio::io::AsyncWriteExt;
use tokio::time::timeout;

const TIMEOUT: Duration = Duration::from_secs(5);
const BUFFER_SIZE: usize = 16384;

async fn sender(mut stream: UtpStream) -> anyhow::Result<()> {
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt::init();

    let mut args = std::env::args().skip(1);

    let bind_addr: SocketAddr = args
        .next()
        .expect("first arg should be listen address")
        .parse()
        .unwrap();
    let remote_addr: SocketAddr = args
        .next()
        .expect("second arg should be address to connect")
        .parse()
        .unwrap();

    let socket = UtpSocket::new_udp(bind_addr)
        .await
        .context("error creating socket")?;
    let sock = socket
        .connect(remote_addr)
        .await
        .context("error connecting")?;
    sender(sock).await.context("error sending")
}
