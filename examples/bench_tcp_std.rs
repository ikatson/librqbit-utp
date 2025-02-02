use std::{
    io::{Read, Write},
    net::{Ipv4Addr, SocketAddr, TcpListener, TcpStream},
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use rand::Rng;
use tracing::info;

const PRINT_INTERVAL: Duration = Duration::from_secs(1);
const BUFFER_SIZE: usize = 1452; // UTP payload with MTU 1500

fn sender(mut stream: TcpStream) -> anyhow::Result<()> {
    let mut buffer = vec![0u8; BUFFER_SIZE];
    rand::thread_rng().fill(buffer.as_mut_slice());

    loop {
        stream.write_all(&buffer)?;
    }
}

fn receiver(mut stream: TcpStream) -> anyhow::Result<()> {
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

fn run_server(addr: SocketAddr) -> anyhow::Result<()> {
    let listener = TcpListener::bind(addr).context("error binding TCP listener")?;
    info!("Server listening on {}", addr);

    let (stream, peer_addr) = listener.accept()?;
    info!("Accepted connection from {}", peer_addr);

    receiver(stream).context("error running receiver")
}

fn run_client(server_addr: SocketAddr) -> anyhow::Result<()> {
    let stream = TcpStream::connect(server_addr).context("error connecting to server")?;
    info!("Connected to server at {}", server_addr);

    sender(stream).context("error running sender")
}

fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    let _ = tracing_subscriber::fmt::try_init();

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
