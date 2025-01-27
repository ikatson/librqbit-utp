use std::{
    net::SocketAddr,
    sync::{
        atomic::{AtomicU16, Ordering},
        Arc,
    },
    task::Poll,
    time::{Duration, Instant},
};

use crate::{
    constants::{DEFAULT_MTU, IPV4_HEADER, MIN_UDP_HEADER, UTP_HEADER_SIZE},
    message::UtpMessage,
    packet_pool::PacketPool,
    raw::{Type, UtpHeader},
    stream::StreamArgs,
    traits::{DefaultUtpEnvironment, Transport, UtpEnvironment},
    utils::spawn_print_error,
    UtpStream,
};
use anyhow::{bail, Context};
use tokio::sync::{
    mpsc::{unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tracing::{debug, error_span, trace, warn};

type ConnectionId = u16;

type Key = (SocketAddr, ConnectionId);

type UtpHeaderMessage = (UtpHeader, SocketAddr);

struct Connecting {
    sender: tokio::sync::oneshot::Sender<anyhow::Result<UtpHeaderMessage>>,
}

#[derive(Default)]
pub struct SocketOpts {
    // The MTU to base calculations on.
    pub mtu: Option<usize>,

    // How much memory to pre-allocate for incoming packet pool.
    pub packet_pool_max_memory: Option<usize>,

    // How many packets to track in the RX window.
    pub tx_packets: Option<usize>,

    // How many bytes to allocate for each virtual socket's TX.
    pub tx_bytes: Option<usize>,

    // Disable Nagle's algorithm (buffering outgoing packets)
    pub disable_nagle: bool,
}

impl SocketOpts {
    fn validate(&self) -> anyhow::Result<ValidatedSocketOpts> {
        // DEFAULT_MTU is very conservative (to support VPNs / tunneling etc),
        // would be great to auto-detect it.
        let mtu = self.mtu.unwrap_or(DEFAULT_MTU);

        let max_packet_size = mtu
            .checked_sub(IPV4_HEADER)
            .context("MTU too low")?
            .checked_sub(MIN_UDP_HEADER)
            .context("MTU too low")?;
        let max_payload_size = max_packet_size
            .checked_sub(UTP_HEADER_SIZE)
            .context("MTU too low")?;
        if max_payload_size == 0 {
            bail!("MTU too low");
        }
        let packet_pool_max_packets =
            self.packet_pool_max_memory.unwrap_or(32 * 1024 * 1024) / max_packet_size;
        if packet_pool_max_packets == 0 {
            bail!("invalid configuration: packet_pool_max_memory / max_packet_size = 0");
        }

        let virtual_socket_tx_packets = self.tx_packets.unwrap_or(64);
        if virtual_socket_tx_packets == 0 {
            bail!("invalid configuration: virtual_socket_tx_packets = 0");
        }
        let virtual_socket_tx_bytes = self.tx_bytes.unwrap_or(1024 * 1024);
        if virtual_socket_tx_bytes == 0 {
            bail!("invalid configuration: virtual_socket_tx_bytes = 0")
        }

        Ok(ValidatedSocketOpts {
            max_packet_size,
            max_payload_size,
            packet_pool_max_packets,
            virtual_socket_tx_packets,
            virtual_socket_tx_bytes,
            nagle: !self.disable_nagle,
        })
    }
}

#[derive(Clone, Copy)]
pub(crate) struct ValidatedSocketOpts {
    pub max_packet_size: usize,
    pub max_payload_size: usize,

    pub packet_pool_max_packets: usize,

    pub virtual_socket_tx_packets: usize,
    pub virtual_socket_tx_bytes: usize,

    pub nagle: bool,
}

pub struct UtpSocket<Transport, Env> {
    // Todo private/public
    pub(crate) udp_socket: Transport,
    pub(crate) created: Instant,
    pub(crate) streams: dashmap::DashMap<Key, UnboundedSender<UtpMessage>>,

    accept_tx: UnboundedSender<tokio::sync::oneshot::Sender<UtpHeaderMessage>>,

    connection_id: AtomicU16,
    connecting: dashmap::DashMap<Key, Connecting>,
    local_addr: SocketAddr,
    opts: ValidatedSocketOpts,

    pub(crate) env: Env,
}

impl<Transport, Env> std::fmt::Debug for UtpSocket<Transport, Env> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpSocket")
            .field("addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

impl UtpSocket<tokio::net::UdpSocket, DefaultUtpEnvironment> {
    pub async fn new_udp(bind_addr: SocketAddr) -> anyhow::Result<Arc<Self>> {
        let opts = SocketOpts::default();
        let sock = tokio::net::UdpSocket::bind(bind_addr)
            .await
            .context("error binding")?;
        Self::new_with_opts(sock, Default::default(), opts).await
    }
}

impl<T: Transport, Env: UtpEnvironment> UtpSocket<T, Env> {
    pub async fn new_with_opts(
        transport: T,
        env: Env,
        opts: SocketOpts,
    ) -> anyhow::Result<Arc<Self>> {
        let opts = opts.validate().context("error validating socket options")?;
        let sock = transport;
        let local_addr = sock.bind_addr();

        let (accept_tx, accept_rx) = unbounded_channel();

        let sock = Arc::new(Self {
            udp_socket: sock,
            created: env.now(),
            connection_id: AtomicU16::new(env.random_u16()),
            accept_tx,
            streams: Default::default(),
            connecting: Default::default(),
            local_addr,
            opts,
            env,
        });
        spawn_print_error(
            error_span!("utp_socket", addr=?local_addr),
            sock.clone().dispatcher(accept_rx),
        );
        Ok(sock)
    }

    pub(crate) fn opts(&self) -> &ValidatedSocketOpts {
        &self.opts
    }

    async fn dispatcher(
        self: Arc<Self>,
        mut accept_rx: UnboundedReceiver<tokio::sync::oneshot::Sender<UtpHeaderMessage>>,
    ) -> anyhow::Result<()> {
        let packet_pool =
            { PacketPool::new(self.opts.packet_pool_max_packets, self.opts.max_packet_size) };

        'outer: loop {
            let mut packet = packet_pool.get().await;
            let buf = packet.get_mut();
            let (size, from) = self
                .udp_socket
                .recv_from(buf)
                .await
                .context("error receiving")?;

            trace!(addr = ?from, size, "received a message");

            let message = match UtpMessage::deserialize(packet, size) {
                Some(msg) => msg,
                None => {
                    debug!(
                        size,
                        ?from,
                        "error desserializing and validating UTP message"
                    );
                    continue;
                }
            };

            trace!(addr = ?from, size, ?message, "received");

            // If it's a SYN, consider someone connecting to us.
            if let Type::ST_SYN = message.header.get_type() {
                let msg = (message.header, from);
                loop {
                    match accept_rx.try_recv() {
                        Ok(chan) => match chan.send(msg) {
                            Ok(()) => continue 'outer,
                            Err(_) => {
                                warn!("acceptor dead, trying next one");
                            }
                        },
                        Err(e) => {
                            warn!("error accepting incoming uTP connection: {e:?}");
                            continue 'outer;
                        }
                    }
                }
            }

            let cid = message.header.connection_id;

            // Try creaging a connecting stream first.
            if let Type::ST_STATE = message.header.get_type() {
                if let Some((_, conn)) = self.connecting.remove(&(from, cid)) {
                    let _ = conn.sender.send(Ok((message.header, from)));
                    continue;
                }
            }

            // Dispatch the message to an existing stream.
            if let Some(s) = self.streams.get(&(from, cid)) {
                if s.send(message).is_err() {
                    if let Some((k, _)) = self.streams.remove(&(from, cid)) {
                        debug!(key=?k, "stream dead");
                    }
                }
            }
        }
    }

    #[tracing::instrument(name="utp_socket:accept", skip(self), fields(local=?self.local_addr))]
    pub async fn accept(self: &Arc<Self>) -> anyhow::Result<UtpStream<T, Env>> {
        let (tx, rx) = tokio::sync::oneshot::channel();
        self.accept_tx.send(tx).context("error accepting")?;

        let (header, addr) = rx.await.context("bug")?;

        let (tx, rx) = unbounded_channel();

        trace!(?addr, "accepted uTP connection");

        let conn_id_recv = header.connection_id.wrapping_add(1);
        let conn_id_send = header.connection_id;

        let stream_args = StreamArgs::new_incoming(self.env.random_u16().into(), &header);
        let stream = UtpStream::new(self, addr, rx, stream_args);

        self.streams.insert((addr, conn_id_recv), tx.clone());
        self.streams.insert((addr, conn_id_send), tx);
        Ok(stream)
    }

    #[tracing::instrument(name="utp_socket:connect", skip(self), fields(local=?self.local_addr))]
    pub async fn connect(
        self: &Arc<Self>,
        remote: SocketAddr,
    ) -> anyhow::Result<UtpStream<T, Env>> {
        // create a "connecting" future. On received message, resolve the oneshot.

        // One for send one for recv.
        let connection_id = self.connection_id.fetch_add(2, Ordering::Relaxed);

        let mut buf = [0u8; UTP_HEADER_SIZE];
        let seq_nr = self.env.random_u16().into();
        let mut header = UtpHeader {
            connection_id,
            timestamp_microseconds: self.created.elapsed().as_micros() as u32,
            seq_nr,
            ..Default::default()
        };

        header.set_type(Type::ST_SYN);

        header
            .serialize(&mut buf)
            .context("bug: error serializing SYN")?;

        let (tx, rx) = oneshot::channel();

        self.connecting
            .insert((remote, connection_id), Connecting { sender: tx });

        trace!(remote = ?remote, sock=?self.local_addr, connection_id = connection_id, "sending SYN");

        let syn_sent_ts = Instant::now();

        self.udp_socket
            .send_to(&buf, remote)
            .await
            .inspect_err(|e| {
                debug!("error sending: {e:?}");
                self.connecting.remove(&(remote, connection_id));
            })
            .context("error sending")?;

        let (ack_header, from) = match tokio::time::timeout(Duration::from_secs(10), rx).await {
            Ok(Ok(Ok(reply))) => reply,
            Ok(Ok(Err(e))) => return Err(e).context("error processing STATE"),
            Ok(Err(_)) => bail!("bug: oneshow rx recv error"),
            Err(_) => {
                self.connecting.remove(&(remote, connection_id));
                bail!("timeout connecting")
            }
        };

        let ack_received_ts = Instant::now();

        if ack_header.ack_nr != seq_nr {
            bail!("wrong ack")
        }

        if ack_header.connection_id != connection_id {
            bail!("bug: wrong connection id")
        }

        trace!(?from, remote_window = ack_header.wnd_size, "connected");

        let (tx, rx) = unbounded_channel();

        let stream_args =
            StreamArgs::new_outgoing(connection_id, &ack_header, syn_sent_ts, ack_received_ts);
        let stream = UtpStream::new(self, remote, rx, stream_args);

        self.streams.insert((remote, connection_id), tx.clone());
        self.streams
            .insert((remote, connection_id.wrapping_add(1)), tx);

        Ok(stream)
    }

    /// Returns true if saw Poll::Pending
    pub(crate) fn try_poll_send_to(
        &self,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> anyhow::Result<bool> {
        match self.udp_socket.poll_send_to(cx, buf, addr) {
            Poll::Ready(Ok(sz)) => {
                if sz != buf.len() {
                    warn!(
                        actual_len = sz,
                        expectedlen = buf.len(),
                        "sent a broken packet"
                    );
                }
            }
            Poll::Ready(Err(e)) => {
                bail!("error sending to UDP socket: {e:#}");
            }
            Poll::Pending => {
                warn!("UDP socket full, could not send packet");
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use tokio::join;

    use crate::{
        test_util::{env::MockUtpEnvironment, transport::MockUtpTransport},
        UtpSocket,
    };

    #[tokio::test]
    async fn test_echo() {
        let transport = MockUtpTransport::new();
        let env = MockUtpEnvironment::new();

        let remote: SocketAddr = (Ipv4Addr::LOCALHOST, 2).into();

        let socket = UtpSocket::new_with_opts(transport, env, Default::default())
            .await
            .unwrap();

        let connect = async { socket.connect(remote).await.unwrap() };
        let accept = async { socket.accept().await.unwrap() };

        join!(connect, accept);
    }
}
