#[allow(unused)]
#[path = "./common/example_common.rs"]
mod example_common;

use std::{net::SocketAddr, time::Duration};

use anyhow::Context;
use clap::{Parser, Subcommand, ValueEnum};
use example_common::{bench_receiver, bench_sender, echo, TIMEOUT};
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

#[derive(Clone, Debug, Subcommand)]
enum Command {
    Server,
    Client,
}

#[derive(Debug, Clone, ValueEnum)]
enum Program {
    Echo,
    Bench,
}

#[derive(Clone, Debug, Parser)]
struct BenchArgs {
    #[arg(long, default_value = "utp")]
    mode: Mode,

    #[arg(long, default_value = "127.0.0.1:5000")]
    client_listen_addr: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:5001")]
    client_connect_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:9001")]
    client_prometheus_listen_addr: SocketAddr,
    #[arg(long, default_value = "127.0.0.1:9002")]
    server_prometheus_listen_addr: SocketAddr,

    #[arg(long, default_value = "127.0.0.1:5001")]
    server_listen_addr: SocketAddr,

    #[arg(long, default_value = "librqbit-utp")]
    server_utp_kind: UtpKind,
    #[arg(long, default_value = "librqbit-utp")]
    client_utp_kind: UtpKind,

    #[arg(long)]
    librqbit_utp_congestion_tracing: bool,

    #[arg(long, default_value = "none")]
    libutp_rs2_log_level: LibUtpRs2LogLevel,

    #[arg(long, default_value = "bench")]
    program: Program,

    #[arg(long)]
    inprocess: bool,

    #[command(subcommand)]
    command: Option<Command>,
}

type BoxRead = Box<dyn AsyncRead + Unpin + 'static>;
type BoxWrite = Box<dyn AsyncWrite + Unpin + 'static>;
type RW = (BoxRead, BoxWrite);

fn boxrw(
    t: (
        impl AsyncRead + Unpin + 'static,
        impl AsyncWrite + Unpin + 'static,
    ),
) -> RW {
    let (r, w) = t;
    (Box::new(r), Box::new(w))
}

impl BenchArgs {
    #[tracing::instrument(name = "server", skip_all)]
    async fn server(&self) -> anyhow::Result<()> {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(self.server_prometheus_listen_addr)
            .install()
            .context("error installing prometheus")?;

        let (r, w) = self.accept().await?;
        info!("accepted a connection");
        match self.program {
            Program::Echo => {
                info!("starting echo");
                echo(r, w).await
            }
            Program::Bench => {
                info!("starting receiver");
                bench_receiver(r).await
            }
        }
    }

    #[tracing::instrument(name = "client", skip_all)]
    async fn client(&self) -> anyhow::Result<()> {
        metrics_exporter_prometheus::PrometheusBuilder::new()
            .with_http_listener(self.client_prometheus_listen_addr)
            .install()
            .context("error installing prometheus")?;

        let (r, w) = timeout(TIMEOUT, self.connect())
            .await
            .with_context(|| format!("timeout connecting to {}", self.client_connect_addr))?
            .with_context(|| format!("error connecting to {}", self.client_connect_addr))?;
        info!("connected");
        match self.program {
            Program::Echo => {
                info!("starting echo");
                echo(r, w).await
            }
            Program::Bench => {
                info!("starting sender");
                bench_sender(w).await
            }
        }
    }

    fn librqbut_utp_socket_opts(&self) -> librqbit_utp::SocketOpts {
        librqbit_utp::SocketOpts {
            congestion: librqbit_utp::CongestionConfig {
                tracing: self.librqbit_utp_congestion_tracing,
                ..Default::default()
            },
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
            ..Default::default()
        }
    }

    async fn accept(&self) -> anyhow::Result<RW> {
        let listen = self.server_listen_addr;
        match &self.mode {
            Mode::Tcp => {
                info!(addr=?listen, "starting TCP server");
                let l = TcpListener::bind(listen)
                    .await
                    .with_context(|| format!("error binding to {listen}"))?;
                let (stream, _) = l.accept().await.context("error accepting")?;
                Ok(boxrw(stream.into_split()))
            }
            Mode::Utp => match &self.server_utp_kind {
                UtpKind::LibrqbitUtp => {
                    info!(addr=?listen, "starting librqbit-utp server");
                    librqbit_utp::UtpSocketUdp::new_udp_with_opts(
                        listen,
                        self.librqbut_utp_socket_opts(),
                    )
                    .await
                    .with_context(|| format!("error creating uTP socket at {listen}"))?
                    .accept()
                    .await
                    .context("error accepting")
                    .map(|s| boxrw(s.split()))
                }
                UtpKind::LibutpRs2 => {
                    info!(addr=?listen, "starting libutp-rs-2 server");
                    libutp_rs2::UtpContext::new_udp_with_opts(listen, self.libutp_rs2_opts())
                        .await
                        .with_context(|| format!("error creating uTP socket at {listen}"))?
                        .accept()
                        .await
                        .context("error accepting")
                        .map(|s| boxrw(tokio::io::split(s)))
                }
            },
        }
    }

    async fn connect(&self) -> anyhow::Result<RW> {
        let listen = self.client_listen_addr;
        let remote = self.client_connect_addr;
        match &self.mode {
            Mode::Tcp => {
                info!(addr=?remote, "connecting over TCP");
                TcpStream::connect(remote)
                    .await
                    .with_context(|| format!("TCP: error conecting to {remote}"))
                    .map(|s| boxrw(s.into_split()))
            }
            Mode::Utp => match &self.client_utp_kind {
                UtpKind::LibrqbitUtp => {
                    info!(addr=?remote, "connecting over uTP with librqbit-utp");
                    librqbit_utp::UtpSocketUdp::new_udp_with_opts(
                        listen,
                        self.librqbut_utp_socket_opts(),
                    )
                    .await
                    .with_context(|| format!("error creating uTP socket at {listen}"))?
                    .connect(remote)
                    .await
                    .with_context(|| format!("error connecting to {remote}"))
                    .map(|s| boxrw(s.split()))
                }
                UtpKind::LibutpRs2 => {
                    info!(addr=?remote, "connecting over uTP with libutp-rs-2");
                    libutp_rs2::UtpContext::new_udp_with_opts(listen, self.libutp_rs2_opts())
                        .await
                        .with_context(|| format!("error creating uTP socket at {listen}"))?
                        .connect(remote)
                        .await
                        .with_context(|| format!("error connecting to {remote}"))
                        .map(|s| boxrw(tokio::io::split(s)))
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
