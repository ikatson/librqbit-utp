#[allow(unused)]
#[path = "./common/example_common.rs"]
mod example_common;

use std::net::{SocketAddr, TcpListener, TcpStream};

use anyhow::Context;
use example_common::{bench_main, receiver_sync, sender_sync};
use tracing::info;

fn run_server(addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).context("error binding TCP listener")?;
    info!("Server listening on {}", addr);

    let (stream, peer_addr) = listener.accept()?;
    info!("Accepted connection from {}", peer_addr);

    receiver_sync(stream).context("error running receiver")
}

fn run_client(server_addr: SocketAddr) -> anyhow::Result<()> {
    let stream = TcpStream::connect(server_addr).context("error connecting to server")?;
    info!("Connected to server at {}", server_addr);

    sender_sync(stream).context("error running sender")
}

fn main() -> anyhow::Result<()> {
    bench_main(run_server, run_client)
}
