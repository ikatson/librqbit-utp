use std::{
    collections::{hash_map::Entry, VecDeque},
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
    time::{Duration, Instant},
};

use dontfrag::UdpSocketExt;
use rustc_hash::FxHashMap as HashMap;
use tokio_util::sync::CancellationToken;

use crate::{
    congestion::CongestionController,
    constants::{
        DEFAULT_MAX_ACTIVE_STREAMS_PER_SOCKET, DEFAULT_MAX_RX_BUF_SIZE_PER_VSOCK,
        DEFAULT_MAX_TX_BUF_SIZE_PER_VSOCK, DEFAULT_REMOTE_INACTIVITY_TIMEOUT, IPV4_HEADER,
        UDP_HEADER, UTP_HEADER,
    },
    message::UtpMessage,
    metrics::METRICS,
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
    stream_dispatch::{StreamArgs, UtpStreamStarter},
    traits::{DefaultUtpEnvironment, Transport, UtpEnvironment},
    utils::{DropGuardSendBeforeDeath, FnDropGuard},
};
use crate::{spawn_utils::spawn_with_cancel, UtpStream};
use anyhow::Context;
use tokio::sync::{
    mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tracing::{debug, error_span, trace, warn};

type ConnectionId = SeqNr;

// When we get incoming packets this connection id is used to pick the stream.
type StreamRecvKey = (SocketAddr, ConnectionId);

#[derive(Debug, Default, Clone, Copy)]
pub enum CongestionControllerKind {
    #[default]
    Cubic,
}

#[derive(Debug, Default, Clone, Copy)]
pub struct CongestionConfig {
    pub kind: CongestionControllerKind,
    pub tracing: bool,
}

impl CongestionConfig {
    pub(crate) fn create(&self, now: Instant, rmss: usize) -> Box<dyn CongestionController> {
        use crate::congestion::cubic::Cubic;
        use crate::congestion::tracing::TracingController;

        match (self.kind, self.tracing) {
            (CongestionControllerKind::Cubic, true) => {
                tracing::debug!("enabling congestion tracing");
                Box::new(TracingController::new(Cubic::new(now, rmss)))
            }
            (CongestionControllerKind::Cubic, false) => Box::new(Cubic::new(now, rmss)),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct SocketOpts {
    pub link_mtu: Option<usize>,

    /// If set will try to send SO_RCVBUF option on the socket.
    /// If not set, will use OS default.
    pub udp_socket_rx_bufsize_bytes: Option<usize>,

    /// For flow control, if the user isn't reading, when to start dropping packets.
    pub vsock_rx_bufsize_bytes: Option<usize>,
    /// How many bytes to allocate for each virtual socket's TX.
    pub vsock_tx_bufsize_bytes: Option<usize>,

    /// Disable Nagle's algorithm
    pub disable_nagle: bool,

    /// Congestion control configuration.
    pub congestion: CongestionConfig,

    /// The parent span for spawned tokio tasks. If not provided, will use
    /// root span.
    pub parent_span: Option<tracing::Id>,

    /// If provided, this can be used to abruptly terminate all spawned tasks.
    pub cancellation_token: CancellationToken,

    /// How many times to retry retransmitting a packet before terminating
    /// the virtual socket.
    pub max_retransmissions: Option<NonZeroUsize>,

    /// How many seconds to wait for incoming data
    /// before terminating the connection.
    pub remote_inactivity_timeout: Option<Duration>,

    /// How many virtual sockets to have at the same time. Each vsock consumes
    /// resources (memory, tokio tasks).
    pub max_live_vsocks: Option<usize>,

    /// If true, will wait for ACK of FIN. By default we don't wait
    /// as it's not required for torrents.
    pub dont_wait_for_lastack: bool,

    /// How many times should the MTU probes be retransmitted. By default it's 1, so that
    /// MTU probing can handle a bit of packet loss. If it's 0, we would mark RTO'ed probes as failed
    /// sooner and thus perf would be better.
    pub mtu_probe_max_retransmissions: Option<usize>,
}

impl SocketOpts {
    fn validate(&self) -> anyhow::Result<ValidatedSocketOpts> {
        let max_user_rx_buffered_bytes = NonZeroUsize::new(
            self.vsock_rx_bufsize_bytes
                .unwrap_or(DEFAULT_MAX_RX_BUF_SIZE_PER_VSOCK),
        )
        .context("max_user_rx_buffered_bytes = 0. Increase rx_bufsize")?;

        let virtual_socket_tx_bytes = NonZeroUsize::new(
            self.vsock_tx_bufsize_bytes
                .unwrap_or(DEFAULT_MAX_TX_BUF_SIZE_PER_VSOCK),
        )
        .context("invalid configuration: virtual_socket_tx_bytes = 0")?;

        // 1500 is ethernet MTU.
        let link_mtu = self.link_mtu.unwrap_or(1500);
        let link_mtu: u16 = link_mtu.try_into().context("link mtu exceeds u16")?;
        let min_mtu = IPV4_HEADER + UDP_HEADER + UTP_HEADER + 1;
        if link_mtu < min_mtu {
            anyhow::bail!("provided link_mtu ({link_mtu}) too low, not enough for even 1-byte IPv4 packets (min {min_mtu})");
        }

        Ok(ValidatedSocketOpts {
            link_mtu,
            max_user_rx_buffered_bytes,
            virtual_socket_tx_bytes,
            nagle: !self.disable_nagle,
            congestion: self.congestion,
            max_segment_retransmissions: self
                .max_retransmissions
                .unwrap_or(NonZeroUsize::new(5).unwrap()),
            remote_inactivity_timeout: self
                .remote_inactivity_timeout
                .unwrap_or(DEFAULT_REMOTE_INACTIVITY_TIMEOUT),
            max_active_streams: self
                .max_live_vsocks
                .unwrap_or(DEFAULT_MAX_ACTIVE_STREAMS_PER_SOCKET),
            wait_for_last_ack: !self.dont_wait_for_lastack,
            mtu_probe_max_retransmissions: self.mtu_probe_max_retransmissions.unwrap_or(1),
        })
    }
}

#[derive(Clone)]
pub(crate) struct ValidatedSocketOpts {
    pub link_mtu: u16,
    pub max_user_rx_buffered_bytes: NonZeroUsize,

    pub virtual_socket_tx_bytes: NonZeroUsize,
    pub nagle: bool,
    pub congestion: CongestionConfig,
    pub max_segment_retransmissions: NonZeroUsize,

    pub remote_inactivity_timeout: Duration,
    pub max_active_streams: usize,

    pub wait_for_last_ack: bool,

    pub mtu_probe_max_retransmissions: usize,
}

pub(crate) struct RequestWithSpan<V> {
    created_span: tracing::Span,
    tx: oneshot::Sender<V>,
}

type ConnectRequest = RequestWithSpan<anyhow::Result<UtpStream>>;
type Acceptor<T, E> = RequestWithSpan<UtpStreamStarter<T, E>>;

impl<V> RequestWithSpan<V> {
    fn new(tx: oneshot::Sender<V>) -> Self {
        Self {
            created_span: tracing::Span::current(),
            tx,
        }
    }
}

pub(crate) enum ControlRequest {
    ConnectRequest(SocketAddr, ConnectToken, ConnectRequest),
    ConnectDropped(SocketAddr, ConnectToken),

    Shutdown(StreamRecvKey),
}

impl std::fmt::Debug for ControlRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlRequest::ConnectRequest(socket_addr, token, _) => {
                write!(f, "ConnectRequest({socket_addr}, {token})")
            }
            ControlRequest::ConnectDropped(socket_addr, token) => {
                write!(f, "ConnectDropped({socket_addr}, {token})")
            }
            ControlRequest::Shutdown(key) => {
                write!(f, "Shutdown({key:?})")
            }
        }
    }
}

static NEXT_CONNECT_TOKEN: AtomicU64 = AtomicU64::new(0);
type ConnectToken = u64;

struct Connecting {
    token: ConnectToken,
    start: Instant,
    seq_nr: SeqNr,
    requester: ConnectRequest,
}

const MAX_CONNECTING_PER_ADDR: usize = 4;

#[derive(Default)]
struct ConnectingPerAddr {
    slots: [Option<Connecting>; MAX_CONNECTING_PER_ADDR],
    len: usize,
}

impl ConnectingPerAddr {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn insert(&mut self, c: Connecting) -> bool {
        for slot in self.slots.iter_mut() {
            if slot.is_none() {
                *slot = Some(c);
                self.len += 1;
                return true;
            }
        }
        false
    }

    // TODO: use connection ID instead of sequence number. Or even both.
    fn pop(&mut self, s: SeqNr) -> Option<Connecting> {
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

    fn pop_by_token(&mut self, token: ConnectToken) -> Option<Connecting> {
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

const ACCEPT_QUEUE_MAX_ACCEPTORS: usize = 32;
const ACCEPT_QUEUE_MAX_SYNS: usize = 32;

struct Syn {
    remote: SocketAddr,
    header: UtpHeader,
}

enum MatchSynWithAccept<T, E> {
    Matched,
    Full(Syn, Acceptor<T, E>),
    SynInvalid(Acceptor<T, E>),
    ReceiverDead(Syn),
}

struct AcceptQueue<T, E> {
    syns: VecDeque<Syn>,
    next_available_acceptor: Option<Acceptor<T, E>>,
    rx: mpsc::Receiver<Acceptor<T, E>>,
}

impl<T, E> AcceptQueue<T, E> {
    fn try_next_acceptor(&mut self) -> Option<Acceptor<T, E>> {
        if let Some(next) = self.next_available_acceptor.take() {
            return Some(next);
        }
        self.rx.try_recv().ok()
    }

    fn try_cache_syn(&mut self, syn: Syn) -> bool {
        if self.syns.len() < ACCEPT_QUEUE_MAX_SYNS {
            self.syns.push_back(syn);
            return true;
        }
        false
    }
}

pub(crate) struct Dispatcher<T: Transport, E: UtpEnvironment> {
    env: E,
    socket: Arc<UtpSocket<T, E>>,

    accept_queue: AcceptQueue<T, E>,

    // TODO: we need to insert here only once!
    pub(crate) streams: HashMap<StreamRecvKey, UnboundedSender<UtpMessage>>,
    connecting: HashMap<SocketAddr, ConnectingPerAddr>,
    control_rx: UnboundedReceiver<ControlRequest>,
    next_connection_id: SeqNr,
}

impl<T: Transport, E: UtpEnvironment> Dispatcher<T, E> {
    pub(crate) async fn run_forever(mut self) -> anyhow::Result<()> {
        let mut read_buf = [0u8; 16384];

        loop {
            if let Err(e) = self.run_once(&mut read_buf).await {
                warn!("error running dispatcher: {e:#}");
                return Err(e);
            }
        }
    }

    async fn run_once(&mut self, read_buf: &mut [u8]) -> anyhow::Result<()> {
        self.cleanup_accept_queue()?;

        tokio::select! {
            accept = self.accept_queue.rx.recv(), if self.accept_queue.next_available_acceptor.is_none() => {
                let accept = accept.unwrap();
                self.accept_queue.next_available_acceptor = Some(accept);
            }
            control_request = self.control_rx.recv() => {
                let control = control_request.unwrap();
                // If this blocks it should be short lived enough?;
                // TODO: do smth about this
                self.on_control(control).await;
            },
            recv = self.socket.transport.recv_from(read_buf) => {
                let (len, addr) = recv.context("error receiving")?;
                let message = match UtpMessage::deserialize(&read_buf[..len]) {
                    Some(msg) => msg,
                    None => {
                        debug!(len, ?addr, "error deserializing and validating UTP message");
                        return Ok(())
                    }
                };
                self.on_recv(addr, message)?;
            }
        }

        Ok(())
    }

    // Get into a state where we can't match acceptors with cached SYNs anymore.
    fn cleanup_accept_queue(&mut self) -> anyhow::Result<()> {
        while let Some(syn) = self.accept_queue.syns.pop_front() {
            let acceptor = match self.accept_queue.try_next_acceptor() {
                Some(acc) => acc,
                None => {
                    self.accept_queue.syns.push_front(syn);
                    return Ok(());
                }
            };
            match self.match_syn_with_accept(syn, acceptor) {
                MatchSynWithAccept::Matched => continue,
                MatchSynWithAccept::SynInvalid(sender) => {
                    self.accept_queue.next_available_acceptor = Some(sender);
                }
                MatchSynWithAccept::ReceiverDead(syn) => {
                    self.accept_queue.syns.push_front(syn);
                }
                MatchSynWithAccept::Full(syn, acceptor) => {
                    self.accept_queue.syns.push_front(syn);
                    self.accept_queue.next_available_acceptor = Some(acceptor);
                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn get_next_free_conn_id(&mut self, addr: SocketAddr) -> SeqNr {
        while self.streams.contains_key(&(addr, self.next_connection_id)) {
            self.next_connection_id += 2;
        }
        self.next_connection_id
    }

    // This would only block if the socket TX is full. It will block the dispatcher but it's a resonable tradeoff.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn on_control(&mut self, msg: ControlRequest) {
        match msg {
            ControlRequest::ConnectRequest(addr, token, sender) => {
                if self.streams_full() {
                    debug!(?addr, "too many connections, dropping connect request");
                    let _ = sender
                        .tx
                        .send(Err(anyhow::anyhow!("too many active connections")));
                    return;
                }
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
                    extensions: Default::default(),
                };
                let mut buf = [0u8; UTP_HEADER as usize];
                header.serialize(&mut buf).unwrap();
                match self.socket.transport.send_to(&buf, addr).await {
                    Ok(len) if len == buf.len() => {}
                    Ok(len) => {
                        // Never seen this happen yet, so can be warn.
                        warn!(
                            len,
                            expected_len = buf.len(),
                            ?addr,
                            "did not send full length, dropping"
                        );
                        return;
                    }
                    Err(e) => {
                        let _ = sender.tx.send(Err(e).context("error sending SYN"));
                        return;
                    }
                }
                let c = Connecting {
                    token,
                    seq_nr: header.seq_nr,
                    requester: sender,
                    start: self.env.now(),
                };
                if self.connecting.entry(addr).or_default().insert(c) {
                    self.next_connection_id += 2;
                } else {
                    // This is super rare, can be warn.
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
            ControlRequest::Shutdown(key) => {
                debug!(?key, "removing stream");
                self.streams.remove(&key);
            }
        }
    }

    fn streams_full(&self) -> bool {
        self.streams.len() >= self.socket.opts.max_active_streams
    }

    #[tracing::instrument(level = "trace", skip_all, fields(addr, seq_nr=?msg.header.seq_nr, ack_nr=?msg.header.ack_nr))]
    fn on_maybe_connect_ack(&mut self, addr: SocketAddr, msg: UtpMessage) -> anyhow::Result<()> {
        if self.streams_full() {
            debug!(
                active_streams = self.streams.len(),
                ?msg,
                "dropping potential SYN-ACK packet, too many active streams"
            );
            return Ok(());
        }

        let mut occ = match self.connecting.entry(addr) {
            Entry::Occupied(occ) => occ,
            Entry::Vacant(_) => {
                trace!(
                    ?msg,
                    "dropping packet, noone is connecting, and no registered streams"
                );
                return Ok(());
            }
        };

        let conn = if let Some(conn) = occ.get_mut().pop(msg.header.ack_nr) {
            if occ.get_mut().is_empty() {
                occ.remove();
            }
            conn
        } else {
            debug!(
                ?msg,
                "dropping packet. we are connecting to this addr, but ack_nr doens't match"
            );
            return Ok(());
        };

        let now = self.env.now();
        let (tx, rx) = unbounded_channel();
        let args = StreamArgs::new_outgoing(&msg.header, conn.start, now)
            .with_parent_span(conn.requester.created_span.clone());

        let recv_key = (addr, msg.header.connection_id);
        if self.streams.insert(recv_key, tx).is_some() {
            warn!(key=?recv_key, "bug: a stream already existed with key. It should have been checked beforehand.");
        }

        let stream = UtpStreamStarter::new(&self.socket, addr, rx, args).start();
        if conn.requester.tx.send(Ok(stream)).is_ok() {
            trace!(?recv_key, "created stream and passed to connector");
        } else {
            debug!(?recv_key, "connecting receiver is dead. dropping");
            self.streams.remove(&recv_key);
        }

        Ok(())
    }

    fn match_syn_with_accept(
        &mut self,
        syn: Syn,
        accept: Acceptor<T, E>,
    ) -> MatchSynWithAccept<T, E> {
        if self.streams_full() {
            debug!("too many connections, can't match SYN with accept");
            return MatchSynWithAccept::Full(syn, accept);
        }

        let recv_key = (syn.remote, syn.header.connection_id + 1);
        if self.streams.contains_key(&recv_key) {
            debug!(?recv_key, "SYN clashes with an existing stream, ignoring");
            return MatchSynWithAccept::SynInvalid(accept);
        }

        let args = StreamArgs::new_incoming(self.env.random_u16().into(), &syn.header)
            .with_parent_span(accept.created_span.clone());
        let (tx, rx) = unbounded_channel();

        let starter = UtpStreamStarter::new(&self.socket, syn.remote, rx, args);

        self.streams.insert(recv_key, tx);
        match accept.tx.send(starter) {
            Ok(()) => {
                trace!("created stream and passed to acceptor");
                MatchSynWithAccept::Matched
            }
            Err(starter) => {
                starter.disarm();
                self.streams.remove(&recv_key);
                MatchSynWithAccept::ReceiverDead(syn)
            }
        }
    }

    fn on_syn(&mut self, remote: SocketAddr, msg: UtpMessage) -> anyhow::Result<()> {
        let mut syn = Syn {
            remote,
            header: msg.header,
        };
        while let Some(acceptor) = self.accept_queue.try_next_acceptor() {
            match self.match_syn_with_accept(syn, acceptor) {
                MatchSynWithAccept::Matched => return Ok(()),
                MatchSynWithAccept::SynInvalid(sender) => {
                    self.accept_queue.next_available_acceptor = Some(sender);
                    return Ok(());
                }
                MatchSynWithAccept::ReceiverDead(s) => syn = s,
                MatchSynWithAccept::Full(s, acceptor) => {
                    self.accept_queue.next_available_acceptor = Some(acceptor);
                    syn = s;
                }
            }
        }
        match self.accept_queue.try_cache_syn(syn) {
            true => Ok(()),
            false => {
                trace!("dropping SYN, no more space to cache them and no acceptors available");
                Ok(())
            }
        }
    }

    #[tracing::instrument(level = "trace", name = "on_recv", skip_all, fields(
        from=?addr,
        conn_id=?message.header.connection_id,
        type=?message.header.get_type(),
        seq_nr=?message.header.seq_nr,
        ack_nr=?message.header.ack_nr,
        payload=message.payload().len()
    ))]
    fn on_recv(&mut self, addr: SocketAddr, message: UtpMessage) -> anyhow::Result<()> {
        let key = (addr, message.header.connection_id);

        if let Some(tx) = self.streams.get(&key) {
            if tx.send(message).is_err() {
                trace!(
                    ?key,
                    "stream dead, but wasn't cleaned up yet, this is probably a race"
                );
                self.streams.remove(&key);
            }
            return Ok(());
        }

        trace!(?key, "no matching live streams");

        match message.header.get_type() {
            Type::ST_STATE => {
                self.on_maybe_connect_ack(addr, message)?;
            }
            Type::ST_SYN => {
                self.on_syn(addr, message)?;
            }
            _ => {
                trace!(?message, ?addr, "dropping packet");
            }
        }
        Ok(())
    }
}

pub struct UtpSocket<T, E> {
    // The underlying transport, usually UDP.
    pub(crate) transport: T,
    // When was the socket created. All the uTP "timestamp_microsends" are relative to it.
    pub(crate) created: Instant,
    pub(crate) control_requests: UnboundedSender<ControlRequest>,
    accept_requests: mpsc::Sender<Acceptor<T, E>>,

    pub(crate) env: E,

    local_addr: SocketAddr,
    opts: ValidatedSocketOpts,

    pub(crate) cancellation_token: CancellationToken,
}

impl<T: Transport, E: UtpEnvironment> std::fmt::Debug for UtpSocket<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpSocket")
            .field("addr", &self.local_addr)
            .finish_non_exhaustive()
    }
}

pub type UtpSocketUdp = UtpSocket<tokio::net::UdpSocket, DefaultUtpEnvironment>;

fn set_udp_rcvbuf(
    sock: tokio::net::UdpSocket,
    bufsize: usize,
) -> anyhow::Result<tokio::net::UdpSocket> {
    let sock = sock.into_std()?;
    let sock = socket2::Socket::from(sock);
    let previous = sock.recv_buffer_size();
    if let Err(e) = sock.set_recv_buffer_size(bufsize) {
        tracing::warn!("error setting UDP socket rcv buf size: {e:#}");
    } else {
        let current = sock.recv_buffer_size();
        tracing::info!(
            expected = bufsize,
            ?previous,
            ?current,
            "set UDP rcv buf size"
        )
    }
    let sock: std::net::UdpSocket = sock.into();
    Ok(tokio::net::UdpSocket::from_std(sock)?)
}

impl UtpSocketUdp {
    pub async fn new_udp(bind_addr: SocketAddr) -> anyhow::Result<Arc<Self>> {
        Self::new_udp_with_opts(bind_addr, Default::default()).await
    }

    pub async fn new_udp_with_opts(
        bind_addr: SocketAddr,
        opts: SocketOpts,
    ) -> anyhow::Result<Arc<Self>> {
        let mut sock = tokio::net::UdpSocket::bind(bind_addr)
            .await
            .context("error binding")?;

        if bind_addr.is_ipv4() {
            if let Err(e) = sock.set_dontfrag_v4(true) {
                warn!("error setting IPV4_DONTFRAG: {e:#}");
            }
        } else if let Err(e) = sock.set_dontfrag_v6(true) {
            debug!("error setting IPV6_DONTFRAG: {e:#}");
        }

        if let Some(so_recvbuf) = opts.udp_socket_rx_bufsize_bytes {
            sock = set_udp_rcvbuf(sock, so_recvbuf)?;
        }

        Self::new_with_opts(sock, Default::default(), opts)
    }
}

impl<T: Transport, Env: UtpEnvironment> UtpSocket<T, Env> {
    pub fn new_with_opts(transport: T, env: Env, opts: SocketOpts) -> anyhow::Result<Arc<Self>> {
        let parent_span = opts.parent_span.clone();
        let (sock, dispatcher) = Self::new_with_opts_and_dispatcher(transport, env, opts)?;
        let span = error_span!(parent: parent_span, "utp_socket", addr=?sock.transport.bind_addr());
        spawn_with_cancel(
            span,
            sock.cancellation_token.clone(),
            dispatcher.run_forever(),
        );
        Ok(sock)
    }

    pub(crate) fn new_with_opts_and_dispatcher(
        transport: T,
        env: Env,
        opts: SocketOpts,
    ) -> anyhow::Result<(Arc<Self>, Dispatcher<T, Env>)> {
        let validated_opts = opts.validate().context("error validating socket options")?;
        let sock = transport;
        let local_addr = sock.bind_addr();

        let (accept_tx, accept_rx) = mpsc::channel(ACCEPT_QUEUE_MAX_ACCEPTORS);
        let (control_tx, control_rx) = unbounded_channel();

        let sock = Arc::new(Self {
            transport: sock,
            created: env.now(),
            control_requests: control_tx,
            local_addr,
            opts: validated_opts,
            env: env.copy(),
            accept_requests: accept_tx,
            cancellation_token: opts.cancellation_token.clone(),
        });

        let dispatcher = Dispatcher {
            streams: Default::default(),
            connecting: Default::default(),
            next_connection_id: env.random_u16().into(),
            control_rx,
            socket: sock.clone(),
            env,
            accept_queue: AcceptQueue {
                syns: Default::default(),
                next_available_acceptor: None,
                rx: accept_rx,
            },
        };

        Ok((sock, dispatcher))
    }

    pub(crate) fn opts(&self) -> &ValidatedSocketOpts {
        &self.opts
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.local_addr
    }

    #[tracing::instrument(level = "debug", name="utp_socket:accept", skip(self), fields(local=?self.local_addr))]
    pub async fn accept(self: &Arc<Self>) -> anyhow::Result<UtpStream> {
        let (tx, rx) = oneshot::channel();

        METRICS.accepting.increment(1);
        let _accepting_guard = FnDropGuard::new(|| METRICS.accepting.decrement(1));

        self.accept_requests
            .send(RequestWithSpan::new(tx))
            .await
            .context("dispatcher dead")?;

        let stream = rx.await.context("dispatcher dead")?;

        METRICS.accepts.increment(1);

        let stream = stream.start();
        trace!("accepted");
        Ok(stream)
    }

    #[tracing::instrument(level = "debug", name="utp_socket:connect", skip(self), fields(local=?self.local_addr))]
    pub async fn connect(self: &Arc<Self>, remote: SocketAddr) -> anyhow::Result<UtpStream> {
        let (tx, rx) = oneshot::channel();
        let token = NEXT_CONNECT_TOKEN.fetch_add(1, Ordering::Relaxed);
        METRICS.connection_attempts.increment(1);
        METRICS.connecting.increment(1);

        let mut fail_guard = FnDropGuard::new(|| {
            METRICS.connection_failures.increment(1);
        });

        let _connecting_guard = FnDropGuard::new(|| {
            METRICS.connecting.decrement(1);
        });

        self.control_requests
            .send(ControlRequest::ConnectRequest(
                remote,
                token,
                RequestWithSpan::new(tx),
            ))
            .context("dispatcher dead")?;

        let mut send_drop_guard = DropGuardSendBeforeDeath::new(
            ControlRequest::ConnectDropped(remote, token),
            &self.control_requests,
        );

        let stream_or_err = rx.await.context("dispatcher dead")?;
        send_drop_guard.disarm();
        if stream_or_err.is_ok() {
            fail_guard.disarm();
            METRICS.connection_successes.increment(1);
        }

        stream_or_err
    }

    /// Returns true if saw Poll::Pending
    pub(crate) fn try_poll_send_to(
        &self,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> std::io::Result<bool> {
        // TODO: uncomment to simulate packet loss.
        // if rand::Rng::gen_bool(&mut rand::thread_rng(), 0.01) {
        //     return Ok(true);
        // }
        match self.transport.poll_send_to(cx, buf, addr) {
            Poll::Ready(Ok(sz)) => {
                if sz != buf.len() {
                    warn!(
                        actual_len = sz,
                        expected_len = buf.len(),
                        "sent a broken packet"
                    );
                }
            }
            Poll::Ready(Err(e)) => {
                METRICS.send_errors.increment(1);
                debug!(
                    "error sending to UDP socket addr={}, len={}: {e:#}",
                    addr,
                    buf.len()
                );
                return Err(e);
            }
            Poll::Pending => {
                METRICS.send_poll_pending.increment(1);
                debug_every_ms!(5000, "UDP socket full, could not send packet");
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use anyhow::{bail, Context};
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        try_join,
    };
    use tracing::{error_span, info, Instrument};

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
            info!("received 42, closing echo");
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
        }
        .instrument(error_span!("connect"));
        let accept = async {
            echo(server.accept().await.context("error accepting")?)
                .await
                .context("error running echo accept")
        }
        .instrument(error_span!("accept"));

        tokio::time::timeout(
            Duration::from_secs(1),
            async move { try_join!(connect, accept) },
        )
        .await
        .context("timeout")??;
        Ok(())
    }
}
