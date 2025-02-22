#[allow(unused)]
#[path = "./common/example_common.rs"]
mod example_common;

use std::{net::SocketAddr, sync::Arc, time::Duration};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use example_common::{bench_receiver, bench_sender, echo, TIMEOUT};
use librqbit_utp::UtpSocketUdp;
use libutp_rs2::UtpUdpContext;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::{TcpListener, TcpStream},
    time::timeout,
};
use tracing::info;

#[derive(Debug, Clone, ValueEnum)]
enum Mode {
    Tcp,
    Utp,
}

#[derive(Debug, Clone, ValueEnum)]
enum LibUtpRs2LogLevel {
    None,
    Normal,
    Debug,
}

#[derive(Debug, Clone, ValueEnum)]
enum UtpKind {
    LibrqbitUtp,
    LibutpRs2,
}

#[derive(Clone, Copy, Debug, Subcommand)]
enum Command {
    Server,
    Client,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
enum Program {
    Echo,
    Bench,
}

#[derive(Clone, Debug, Parser)]
struct BenchArgs {
    #[arg(long, default_value = "utp")]
    mode: Mode,

    #[arg(long, default_value = "127.0.0.1:5001")]
    server_listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:5002")]
    client_listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:5001")]
    client_connect_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:9001")]
    client_prometheus_listen_addr: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:9002")]
    server_prometheus_listen_addr: SocketAddr,

    #[arg(long, default_value = "librqbit-utp")]
    server_utp_kind: UtpKind,
    #[arg(long, default_value = "librqbit-utp")]
    client_utp_kind: UtpKind,

    #[arg(long, default_value = "1200")]
    udp_rcvbuf_kb: Option<usize>,

    #[arg(long)]
    librqbit_utp_congestion_tracing: bool,

    #[arg(long, default_value = "none")]
    libutp_rs2_log_level: LibUtpRs2LogLevel,

    #[arg(long, default_value = "bench")]
    program: Program,

    #[arg(long, default_value = "1")]
    connections: u16,

    #[arg(long)]
    same_client_socket_per_connection: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

type BoxRead = Box<dyn AsyncRead + Send + Unpin + 'static>;
type BoxWrite = Box<dyn AsyncWrite + Send + Unpin + 'static>;
type RW = (BoxRead, BoxWrite);

fn boxrw(
    t: (
        impl AsyncRead + Send + Unpin + 'static,
        impl AsyncWrite + Send + Unpin + 'static,
    ),
) -> RW {
    let (r, w) = t;
    (Box::new(r), Box::new(w))
}

enum Acceptor {
    Tcp(TcpListener),
    UtpRs2(Arc<UtpUdpContext>),
    LibrqbitUtp(Arc<UtpSocketUdp>),
}

impl Acceptor {
    async fn accept(&self) -> anyhow::Result<RW> {
        Ok(match self {
            Acceptor::Tcp(tcp_listener) => tcp_listener
                .accept()
                .await
                .map(|(s, _)| boxrw(s.into_split()))?,
            Acceptor::UtpRs2(utp_context) => utp_context
                .accept()
                .await
                .map(|s| boxrw(tokio::io::split(s)))?,
            Acceptor::LibrqbitUtp(utp_socket) => {
                utp_socket.accept().await.map(|s| boxrw(s.split()))?
            }
        })
    }
}

#[derive(Clone)]
enum Connector {
    Tcp,
    UtpRs2(Arc<UtpUdpContext>),
    LibrqbitUtp(Arc<UtpSocketUdp>),
}

impl Connector {
    async fn connect(&self, addr: SocketAddr) -> anyhow::Result<RW> {
        Ok(match self {
            Connector::Tcp => TcpStream::connect(addr)
                .await
                .map(|s| boxrw(s.into_split()))?,
            Connector::UtpRs2(utp_context) => utp_context
                .connect(addr)
                .await
                .map(|s| boxrw(tokio::io::split(s)))?,
            Connector::LibrqbitUtp(utp_socket) => {
                utp_socket.connect(addr).await.map(|s| boxrw(s.split()))?
            }
        })
    }
}

impl BenchArgs {
    fn install_prometheus(&self, addr: SocketAddr) -> anyhow::Result<()> {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(addr)
            .set_bucket_duration(Duration::from_secs(1))?
            .set_quantiles(&[0., 0.01, 0.1, 0.5, 0.9, 0.99, 1.])?
            .install()
            .context("error installing prometheus")
    }

    #[tracing::instrument(name = "server", skip_all)]
    async fn server(&self) -> anyhow::Result<()> {
        self.install_prometheus(self.server_prometheus_listen_addr)?;

        let mut handles = Vec::new();

        let acceptor = self.acceptor().await?;

        for i in 0..self.connections {
            let (r, w) = acceptor.accept().await?;
            info!("accepted connection {}", i + 1);

            let program = self.program;
            let handle = tokio::spawn(async move {
                match program {
                    Program::Echo => {
                        info!("starting echo for connection {}", i + 1);
                        echo(r, w).await
                    }
                    Program::Bench => {
                        info!("starting receiver for connection {}", i + 1);
                        bench_receiver(r).await
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all connections to complete
        for (i, handle) in handles.into_iter().enumerate() {
            handle
                .await
                .context(format!("connection {} failed", i + 1))??;
        }

        Ok(())
    }

    #[tracing::instrument(name = "client", skip_all)]
    async fn client(&self) -> anyhow::Result<()> {
        self.install_prometheus(self.client_prometheus_listen_addr)?;

        let mut handles = Vec::new();

        let connector: Option<Connector> = if !self.same_client_socket_per_connection {
            None
        } else {
            Some(
                timeout(TIMEOUT, self.connector(0))
                    .await
                    .context("timeout connecting")?
                    .context("error connecting")?,
            )
        };

        for i in 0..self.connections {
            let program = self.program;
            let connector = connector.clone();
            let args = self.clone();

            let handle = tokio::spawn(async move {
                let connector = match connector {
                    Some(c) => c,
                    None => timeout(TIMEOUT, args.connector(i))
                        .await
                        .context("timeout connecting")?
                        .context("error connecting")?,
                };

                let (r, w) = connector.connect(args.client_connect_addr).await?;

                info!("connected client {}", i + 1);

                match program {
                    Program::Echo => {
                        info!("starting echo for client {}", i + 1);
                        echo(r, w).await
                    }
                    Program::Bench => {
                        info!("starting sender for client {}", i + 1);
                        bench_sender(w).await
                    }
                }
            });
            handles.push(handle);
        }

        // Wait for all clients to complete
        for (i, handle) in handles.into_iter().enumerate() {
            handle.await.context(format!("client {} failed", i + 1))??;
        }

        Ok(())
    }

    fn librqbut_utp_socket_opts(&self) -> librqbit_utp::SocketOpts {
        librqbit_utp::SocketOpts {
            congestion: librqbit_utp::CongestionConfig {
                tracing: self.librqbit_utp_congestion_tracing,
                ..Default::default()
            },
            udp_socket_rx_bufsize_bytes: self.udp_rcvbuf_kb.map(|v| v * 1024),
            ..Default::default()
        }
    }

    fn libutp_rs2_opts(&self) -> libutp_rs2::UtpOpts {
        #[allow(clippy::needless_update)]
        libutp_rs2::UtpOpts {
            log_level: match self.libutp_rs2_log_level {
                LibUtpRs2LogLevel::None => libutp_rs2::UtpLogLevel::None,
                LibUtpRs2LogLevel::Normal => libutp_rs2::UtpLogLevel::Normal,
                LibUtpRs2LogLevel::Debug => libutp_rs2::UtpLogLevel::Debug,
            },
            udp_rcvbuf: self.udp_rcvbuf_kb.map(|v| v * 1024),
            ..Default::default()
        }
    }

    async fn acceptor(&self) -> anyhow::Result<Acceptor> {
        let addr = self.server_listen_addr;
        match &self.mode {
            Mode::Tcp => {
                info!(addr=?addr, "starting TCP server");
                let l = TcpListener::bind(addr)
                    .await
                    .with_context(|| format!("error binding to {addr}"))?;
                Ok(Acceptor::Tcp(l))
            }
            Mode::Utp => match &self.server_utp_kind {
                UtpKind::LibrqbitUtp => {
                    info!(addr=?addr, "starting librqbit-utp server");
                    Ok(Acceptor::LibrqbitUtp(
                        librqbit_utp::UtpSocketUdp::new_udp_with_opts(
                            addr,
                            self.librqbut_utp_socket_opts(),
                        )
                        .await
                        .with_context(|| format!("error creating uTP socket at {addr}"))?,
                    ))
                }
                UtpKind::LibutpRs2 => {
                    info!(addr=?addr, "starting libutp-rs-2 server");
                    Ok(Acceptor::UtpRs2(
                        libutp_rs2::UtpContext::new_udp_with_opts(addr, self.libutp_rs2_opts())
                            .await
                            .with_context(|| format!("error creating uTP socket at {addr}"))?,
                    ))
                }
            },
        }
    }

    async fn connector(&self, port_offset: u16) -> anyhow::Result<Connector> {
        let listen = SocketAddr::new(
            self.client_listen_addr.ip(),
            self.client_listen_addr.port() + port_offset,
        );
        match &self.mode {
            Mode::Tcp => Ok(Connector::Tcp),
            Mode::Utp => match &self.client_utp_kind {
                UtpKind::LibrqbitUtp => {
                    info!(addr=?listen, "starting librqbit-utp server");
                    Ok(Connector::LibrqbitUtp(
                        librqbit_utp::UtpSocketUdp::new_udp_with_opts(
                            listen,
                            self.librqbut_utp_socket_opts(),
                        )
                        .await
                        .with_context(|| format!("error creating uTP socket at {listen}"))?,
                    ))
                }
                UtpKind::LibutpRs2 => {
                    info!(addr=?listen, "starting libutp-rs-2 server");
                    Ok(Connector::UtpRs2(
                        libutp_rs2::UtpContext::new_udp_with_opts(listen, self.libutp_rs2_opts())
                            .await
                            .with_context(|| format!("error creating uTP socket at {listen}"))?,
                    ))
                }
            },
        }
    }

    fn make_runtime(&self) -> anyhow::Result<tokio::runtime::Runtime> {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .context("error building tokio runtime")
    }

    fn run(&self) -> anyhow::Result<()> {
        let mut args = std::env::args();
        let binary = args.next().unwrap();
        let args: Vec<String> = args.collect();
        match self.command {
            None => {
                fn spawn(
                    arg0: &'static str,
                    binary: &str,
                    args: &[String],
                    final_arg: &str,
                ) -> std::io::Result<std::process::Child> {
                    let mut builder = std::process::Command::new(binary);
                    builder.args(args).arg(final_arg);
                    if cfg!(unix) {
                        use std::os::unix::process::CommandExt;
                        builder.arg0(arg0);
                    }
                    builder.spawn()
                }

                let server_child = spawn("utp-bench-server", &binary, &args, "server")
                    .context("failed to spawn server process")?;

                // Wait a bit for the server to start
                std::thread::sleep(Duration::from_millis(500));

                let client_child = spawn("utp-bench-client", &binary, &args, "client")
                    .context("failed to spawn client process")?;

                // Wait for both processes to complete
                server_child.wait_with_output()?;
                client_child.wait_with_output()?;
                Ok(())
            }
            Some(Command::Server) => self
                .make_runtime()?
                .block_on(async { self.server().await.context("error running server") }),
            Some(Command::Client) => self
                .make_runtime()?
                .block_on(async { self.client().await.context("error running client") }),
        }
    }
}

fn main() -> anyhow::Result<()> {
    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "info");
    }
    tracing_subscriber::fmt::init();

    let args = BenchArgs::parse();
    args.run().context("error running bench")
}
