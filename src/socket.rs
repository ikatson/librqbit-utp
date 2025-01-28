use std::{
    collections::{hash_map::Entry, HashMap},
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
    time::Instant,
};

use crate::{
    constants::{DEFAULT_MTU, IPV4_HEADER, MIN_UDP_HEADER, UTP_HEADER_SIZE},
    message::UtpMessage,
    packet_pool::{Packet, PacketPool},
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
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

type ConnectionId = SeqNr;

type Key = (SocketAddr, ConnectionId);

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

pub(crate) struct DropGuardSendBeforeDeath<Msg> {
    pub msg: Option<Msg>,
    pub tx: UnboundedSender<Msg>,
}

impl<Msg> DropGuardSendBeforeDeath<Msg> {
    fn new(msg: Msg, tx: UnboundedSender<Msg>) -> Self {
        Self { msg: Some(msg), tx }
    }
    fn disarm(&mut self) {
        self.msg = None;
    }
}

impl<Msg> Drop for DropGuardSendBeforeDeath<Msg> {
    fn drop(&mut self) {
        if let Some(msg) = self.msg.take() {
            let _ = self.tx.send(msg);
        }
    }
}

pub(crate) enum ControlRequest<T: Transport, E: UtpEnvironment> {
    ConnectRequest(SocketAddr, ConnectToken, oneshot::Sender<UtpStream<T, E>>),
    ConnectDropped(SocketAddr, ConnectToken),

    Shutdown {
        remote: SocketAddr,
        conn_id_1: SeqNr,
        conn_id_2: SeqNr,
    },
}

static NEXT_CONNECT_TOKEN: AtomicU64 = AtomicU64::new(0);
type ConnectToken = u64;

struct Connecting<T: Transport, E: UtpEnvironment> {
    token: ConnectToken,
    start: Instant,
    seq_nr: SeqNr,
    tx: oneshot::Sender<UtpStream<T, E>>,
}

const MAX_CONNECTING_PER_ADDR: usize = 4;

#[derive(Default)]
struct ConnectingPerAddr<T: Transport, E: UtpEnvironment> {
    slots: [Option<Connecting<T, E>>; MAX_CONNECTING_PER_ADDR],
    len: usize,
}

impl<T: Transport, E: UtpEnvironment> ConnectingPerAddr<T, E> {
    fn new() -> Self {
        Self {
            // Default() would propagate through all generics so I can't just write
            // [None; MAX_CONNECTING_PER_ADDR]
            slots: [None, None, None, None],
            len: 0,
        }
    }
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn insert(&mut self, c: Connecting<T, E>) -> bool {
        for slot in self.slots.iter_mut() {
            if slot.is_none() {
                *slot = Some(c);
                self.len += 1;
                return true;
            }
        }
        false
    }
    fn pop(&mut self, s: SeqNr) -> Option<Connecting<T, E>> {
        for slot in self.slots.iter_mut() {
            if let Some(c) = slot {
                if c.seq_nr == s {
                    self.len -= 1;
                    return slot.take();
                }
            }
        }
        None
    }

    fn pop_by_token(&mut self, token: ConnectToken) -> Option<Connecting<T, E>> {
        for slot in self.slots.iter_mut() {
            if let Some(c) = slot {
                if c.token == token {
                    self.len -= 1;
                    return slot.take();
                }
            }
        }
        None
    }
}

pub(crate) struct Dispatcher<T: Transport, E: UtpEnvironment> {
    packet_pool: Arc<PacketPool>,
    env: E,
    socket: Arc<UtpSocket<T, E>>,

    next_accept: Option<oneshot::Sender<UtpStream<T, E>>>,
    accept_rx: UnboundedReceiver<oneshot::Sender<UtpStream<T, E>>>,

    pub(crate) streams: HashMap<Key, UnboundedSender<UtpMessage>>,
    connecting: HashMap<SocketAddr, ConnectingPerAddr<T, E>>,
    control_rx: UnboundedReceiver<ControlRequest<T, E>>,
    next_connection_id: SeqNr,
}

async fn recv_from(
    pool: &PacketPool,
    transport: &impl Transport,
) -> anyhow::Result<(SocketAddr, Packet, usize)> {
    let mut packet = pool.get().await;
    let buf = packet.get_mut();
    let (size, addr) = transport.recv_from(buf).await.context("error receiving")?;
    Ok((addr, packet, size))
}

impl<T: Transport, E: UtpEnvironment> Dispatcher<T, E> {
    pub(crate) async fn run_forever(mut self) -> anyhow::Result<()> {
        loop {
            tokio::select! {
                control_request = self.control_rx.recv() => {
                    let control = control_request.unwrap();
                    // If this blocks it should be short lived enough?;
                    // TODO: do smth about this
                    self.on_control(control).await?;
                },
                recv = recv_from(&self.packet_pool, &self.socket.transport) => {
                    let (addr, packet, len) = recv.context("error receiving")?;
                    self.on_recv(addr, packet, len)?;
                }
            }
        }
    }

    fn get_next_free_conn_id(&mut self, addr: SocketAddr) -> SeqNr {
        while self.streams.contains_key(&(addr, self.next_connection_id)) {
            self.next_connection_id += 2;
        }
        self.next_connection_id
    }

    // This would only block if the socket is full. It will block the dispatcher but it's a resonable tradeoff.
    async fn on_control(&mut self, msg: ControlRequest<T, E>) -> anyhow::Result<()> {
        match msg {
            ControlRequest::ConnectRequest(addr, token, sender) => {
                let conn_id = self.get_next_free_conn_id(addr);
                let header = UtpHeader {
                    htype: Type::ST_SYN,
                    connection_id: conn_id,
                    timestamp_microseconds: (self.env.now() - self.socket.created).as_millis()
                        as u32,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    seq_nr: self.env.random_u16().into(),
                    ack_nr: 0.into(),
                };
                let mut buf = [0u8; UTP_HEADER_SIZE];
                header.serialize(&mut buf).unwrap();
                self.socket
                    .transport
                    .send_to(&buf, addr)
                    .await
                    .context("can't send")?;
                let c = Connecting {
                    token,
                    seq_nr: header.seq_nr,
                    tx: sender,
                    start: self.env.now(),
                };
                if self
                    .connecting
                    .entry(addr)
                    .or_insert_with(|| ConnectingPerAddr::new())
                    .insert(c)
                {
                    self.next_connection_id += 2;
                } else {
                    warn!("too many concurrent connectins to {addr}");
                }
            }
            ControlRequest::ConnectDropped(addr, token) => {
                match self.connecting.entry(addr) {
                    Entry::Occupied(mut occ) => {
                        if occ.get_mut().pop_by_token(token).is_some() && occ.get().is_empty() {
                            occ.remove();
                        }
                    }
                    Entry::Vacant(_) => {}
                };
            }
            ControlRequest::Shutdown {
                remote,
                conn_id_1,
                conn_id_2,
            } => {
                self.streams.remove(&(remote, conn_id_1));
                self.streams.remove(&(remote, conn_id_2));
            }
        }
        Ok(())
    }

    #[tracing::instrument(skip_all, fields(addr, seq_nr=?msg.header.seq_nr, ack_nr=?msg.header.ack_nr))]
    fn on_maybe_connect_ack(&mut self, addr: SocketAddr, msg: UtpMessage) -> anyhow::Result<()> {
        let mut occ = match self.connecting.entry(addr) {
            Entry::Occupied(occ) => occ,
            Entry::Vacant(_) => {
                debug!("dropping packet, noone is connecting, and no registered streams");
                return Ok(());
            }
        };

        let conn = if let Some(conn) = occ.get_mut().pop(msg.header.ack_nr) {
            if occ.get_mut().is_empty() {
                occ.remove();
            }
            conn
        } else {
            debug!("dropping packet. we are connecting to this addr, but ack_nr doens't match");
            return Ok(());
        };

        let now = self.env.now();
        let (tx, rx) = unbounded_channel();
        let args = StreamArgs::new_outgoing(&msg.header, conn.start, now);

        let stream = UtpStream::new(&self.socket, addr, rx, args);
        let key1 = (addr, conn.seq_nr);
        let key2 = (addr, conn.seq_nr + 1);

        if self.streams.contains_key(&key1) || self.streams.contains_key(&key2) {
            warn!("clashing sequence numbers, dropping");
            // TODO: send RST or FIN
            return Ok(());
        }

        if conn.tx.send(stream).is_ok() {
            self.streams.insert(key1, tx.clone());
            self.streams.insert(key2, tx);
        } else {
            debug!("connecting receiver is dead. dropping");
        }

        Ok(())
    }

    #[tracing::instrument(skip_all, fields(addr, seq_nr=?msg.header.seq_nr))]
    fn on_syn(&mut self, addr: SocketAddr, msg: UtpMessage) -> anyhow::Result<()> {
        let key1 = (addr, msg.header.connection_id);
        let key2 = (addr, msg.header.connection_id + 1);
        if self.streams.contains_key(&key1) || self.streams.contains_key(&key2) {
            trace!("invalid SYN, it clashes, dropping");
            return Ok(());
        }

        // TODO: Fuck. This can cause a race. We need to queue a few accepts. Refactor.
        let accept_token = match self.accept_rx.try_recv() {
            Ok(t) => t,
            Err(_) => {
                warn!("nothing is accepting, dropping SYN");
                return Ok(());
            }
        };

        let args = StreamArgs::new_incoming(self.env.random_u16().into(), &msg.header);
        let (tx, rx) = unbounded_channel();
        let stream = UtpStream::new(&self.socket, addr, rx, args);

        if accept_token.send(stream).is_ok() {
            self.streams.insert(key1, tx.clone());
            self.streams.insert(key2, tx);
        } else {
            // TODO: try next accept
            warn!("next acceptor dead, dropping SYN");
        }
        Ok(())
    }

    fn on_recv(&mut self, addr: SocketAddr, packet: Packet, len: usize) -> anyhow::Result<()> {
        trace!(?addr, len, "received a message");

        let message = match UtpMessage::deserialize(packet, len) {
            Some(msg) => msg,
            None => {
                debug!(
                    len,
                    ?addr,
                    "error desserializing and validating UTP message"
                );
                return Ok(());
            }
        };

        trace!(?addr, len, ?message, "received");

        let key = (addr, message.header.connection_id);

        if let Some(tx) = self.streams.get(&key) {
            if tx.send(message).is_err() {
                debug!(?key, "stream dead");
                self.streams.remove(&key);
            }
            return Ok(());
        }

        match message.header.get_type() {
            Type::ST_STATE => {
                self.on_maybe_connect_ack(addr, message)?;
            }
            Type::ST_SYN => {
                self.on_syn(addr, message)?;
            }
            _ => {
                trace!("dropping unknown packet");
            }
        }
        Ok(())
    }
}

pub struct UtpSocket<T: Transport, E: UtpEnvironment> {
    // The underlying transport, usually UDP.
    pub(crate) transport: T,
    // When was the socket created. All the uTP "timestamp_microsends" are relative to it.
    pub(crate) created: Instant,
    pub(crate) control_requests: UnboundedSender<ControlRequest<T, E>>,
    pub(crate) env: E,

    accept_requests: UnboundedSender<oneshot::Sender<UtpStream<T, E>>>,

    local_addr: SocketAddr,
    opts: ValidatedSocketOpts,
}

impl<T: Transport, E: UtpEnvironment> std::fmt::Debug for UtpSocket<T, E> {
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
        Self::new_with_opts(sock, Default::default(), opts)
    }
}

impl<T: Transport, Env: UtpEnvironment> UtpSocket<T, Env> {
    pub fn new_with_opts(transport: T, env: Env, opts: SocketOpts) -> anyhow::Result<Arc<Self>> {
        let (sock, dispatcher) = Self::new_with_opts_and_dispatcher(transport, env, opts)?;
        spawn_print_error(
            error_span!("utp_socket", addr=?sock.transport.bind_addr()),
            dispatcher.run_forever(),
        );
        Ok(sock)
    }

    pub(crate) fn new_with_opts_and_dispatcher(
        transport: T,
        env: Env,
        opts: SocketOpts,
    ) -> anyhow::Result<(Arc<Self>, Dispatcher<T, Env>)> {
        let opts = opts.validate().context("error validating socket options")?;
        let sock = transport;
        let local_addr = sock.bind_addr();

        let (accept_tx, accept_rx) = unbounded_channel();
        let (control_tx, control_rx) = unbounded_channel();

        let sock = Arc::new(Self {
            transport: sock,
            created: env.now(),
            control_requests: control_tx,
            local_addr,
            opts,
            env: env.copy(),
            accept_requests: accept_tx,
        });

        let dispatcher = Dispatcher {
            packet_pool: PacketPool::new(opts.packet_pool_max_packets, opts.max_packet_size),
            next_accept: None,
            streams: Default::default(),
            connecting: Default::default(),
            next_connection_id: env.random_u16().into(),
            control_rx,
            socket: sock.clone(),
            env,
            accept_rx,
        };

        Ok((sock, dispatcher))
    }

    pub(crate) fn opts(&self) -> &ValidatedSocketOpts {
        &self.opts
    }

    #[tracing::instrument(name="utp_socket:accept", skip(self), fields(local=?self.local_addr))]
    pub async fn accept(self: &Arc<Self>) -> anyhow::Result<UtpStream<T, Env>> {
        let (tx, rx) = oneshot::channel();
        self.accept_requests.send(tx).context("dispatcher dead")?;

        let stream = rx.await.context("dispatcher dead")?;
        Ok(stream)

        // let (header, addr) = rx.await.context("bug")?;

        // let (tx, rx) = unbounded_channel();

        // trace!(?addr, "accepted uTP connection");

        // let conn_id_recv = header.connection_id.wrapping_add(1);
        // let conn_id_send = header.connection_id;

        // let stream_args = StreamArgs::new_incoming(self.env.random_u16().into(), &header);
        // let stream = UtpStream::new(self, addr, rx, stream_args);

        // self.streams.insert((addr, conn_id_recv), tx.clone());
        // self.streams.insert((addr, conn_id_send), tx);
        // Ok(stream)
    }

    #[tracing::instrument(name="utp_socket:connect", skip(self), fields(local=?self.local_addr))]
    pub async fn connect(
        self: &Arc<Self>,
        remote: SocketAddr,
    ) -> anyhow::Result<UtpStream<T, Env>> {
        let (tx, rx) = oneshot::channel();
        let token = NEXT_CONNECT_TOKEN.fetch_add(1, Ordering::Relaxed);
        self.control_requests
            .send(ControlRequest::ConnectRequest(remote, token, tx))
            .context("dispatcher dead")?;

        let mut guard = DropGuardSendBeforeDeath::new(
            ControlRequest::ConnectDropped(remote, token),
            self.control_requests.clone(),
        );

        let stream = rx.await.context("dispatcher dead")?;
        guard.disarm();

        Ok(stream)
    }

    /// Returns true if saw Poll::Pending
    pub(crate) fn try_poll_send_to(
        &self,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> anyhow::Result<bool> {
        match self.transport.poll_send_to(cx, buf, addr) {
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

    use anyhow::{bail, Context};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        try_join,
    };

    use crate::test_util::{setup_test_logging, transport::MockInterface, MockUtpStream};

    #[tokio::test]
    async fn test_echo() -> anyhow::Result<()> {
        setup_test_logging();
        let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 1).into();
        let server_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 2).into();

        let interface = MockInterface::new();

        let client = interface.create_socket(client_addr);
        let server = interface.create_socket(server_addr);

        async fn echo(s: MockUtpStream) -> anyhow::Result<()> {
            let (mut r, mut w) = s.split();
            w.write_u32(42).await.context("error writing 42")?;

            let read = r.read_u32().await.context("error reading u32")?;
            if read != 42 {
                bail!("expected 42, got {}", read);
            }
            Ok(())
        }

        let connect = async {
            echo(
                client
                    .connect(server_addr)
                    .await
                    .context("error connecting")?,
            )
            .await
            .context("error running echo connect")
        };
        let accept = async {
            echo(server.accept().await.context("error accepting")?)
                .await
                .context("error running echo accept")
        };

        try_join!(connect, accept)?;
        Ok(())
    }
}
