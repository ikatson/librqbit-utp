use std::{
    net::{Ipv4Addr, SocketAddr, UdpSocket},
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use rand::Rng;
use tracing::info;

const PRINT_INTERVAL: Duration = Duration::from_secs(1);
const BUFFER_SIZE: usize = 1452; // UTP payload with MTU 1500

fn sender(socket: UdpSocket, target: SocketAddr) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    rand::thread_rng().fill(buffer.as_mut_slice());

    loop {
        socket.send_to(&buffer, target)?;
    }
}

fn receiver(socket: UdpSocket) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut total_bytes = 0u64;
    let start = Instant::now();
    let mut last_print = start;

    loop {
        match socket.recv(&mut buffer) {
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

fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    let _ = tracing_subscriber::fmt::try_init();

    let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 8001).into();
    let server_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 8002).into();

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
            "server" => {
                let socket = UdpSocket::bind(server_addr).context("error binding socket")?;
                receiver(socket).context("error running receiver")
            }
            "client" => {
                let socket = UdpSocket::bind(client_addr).context("error binding socket")?;
                sender(socket, server_addr).context("error running sender")
            }
            _ => bail!("Invalid argument. Use 'server' or 'client'"),
        }
    }
}
