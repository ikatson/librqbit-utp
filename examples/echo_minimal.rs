use std::net::{Ipv4Addr, SocketAddr};

pub use librqbit_utp::UtpSocket;

use librqbit_utp::UtpStream;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::{error_span, info, Instrument};

const MAX_COUNTER: u64 = 1_000_000;
const PRINT_EVERY: u64 = 100_000;

// Writes MAX_COUNTER numbers and expects the receive the same amount of numbers back.
async fn echo(stream: UtpStream) {
    let (reader, writer) = stream.split();

    let mut reader = reader;

    let reader = async move {
        for _ in 0..=MAX_COUNTER {
            let current = reader.read_u64().await.unwrap();

            if current % PRINT_EVERY == 0 {
                info!("current counter {current}");
            }
        }
    };

    let writer = async move {
        let mut writer = writer;
        for counter in 0..=MAX_COUNTER {
            writer.write_u64(counter).await.unwrap()
        }
        writer.flush().await.unwrap()
    };

    tokio::join!(reader, writer);
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 8001).into();
    let server_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 8002).into();

    let client = tokio::spawn(
        async move {
            let client = UtpSocket::new_udp(client_addr).await.unwrap();
            let sock = client.connect(server_addr).await.unwrap();
            echo(sock).await
        }
        .instrument(error_span!("client")),
    );

    let server = tokio::spawn(
        async move {
            let listener = UtpSocket::new_udp(server_addr).await.unwrap();
            let sock = listener.accept().await.unwrap();
            echo(sock).await
        }
        .instrument(error_span!("server")),
    );

    let (cr, sr) = tokio::join!(client, server);
    cr.unwrap();
    sr.unwrap();
}
