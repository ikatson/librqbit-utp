use std::{
    collections::{hash_map::Entry, HashMap, VecDeque},
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    task::Poll,
    time::Instant,
};

use crate::{
    congestion::CongestionController,
    constants::{
        DEFAULT_CONSERVATIVE_OUTGOING_MTU, DEFAULT_INCOMING_MTU, IPV4_HEADER, MIN_UDP_HEADER,
        UTP_HEADER_SIZE,
    },
    message::UtpMessage,
    packet_pool::Packet,
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
    stream::StreamArgs,
    traits::{DefaultUtpEnvironment, Transport, UtpEnvironment},
    utils::{spawn_print_error, DropGuardSendBeforeDeath},
    UtpStream,
};
use anyhow::{bail, Context};
use tokio::sync::{
    mpsc::{self, unbounded_channel, UnboundedReceiver, UnboundedSender},
    oneshot,
};
use tracing::{debug, error_span, trace, trace_span, warn};

type ConnectionId = SeqNr;

type Key = (SocketAddr, ConnectionId);

const DEFAULT_MAX_RX_BUF_SIZE_PER_VSOCK: usize = 1024 * 1024;

#[derive(Default, Clone, Copy)]
pub enum CongestionControllerKind {
    Reno,
    #[default]
    Cubic,
}

#[derive(Default, Clone, Copy)]
pub struct CongestionConfig {
    pub kind: CongestionControllerKind,
    pub tracing: bool,
}

impl CongestionConfig {
    pub(crate) fn create(&self, now: Instant) -> Box<dyn CongestionController> {
        use crate::congestion::cubic::Cubic;
        use crate::congestion::reno::Reno;
        use crate::congestion::tracing::TracingController;

        match (self.kind, self.tracing) {
            (CongestionControllerKind::Reno, true) => Box::new(TracingController::new(Reno::new())),
            (CongestionControllerKind::Reno, false) => Box::new(Reno::new()),
            (CongestionControllerKind::Cubic, true) => {
                Box::new(TracingController::new(Cubic::new(now)))
            }
            (CongestionControllerKind::Cubic, false) => Box::new(Cubic::new(now)),
        }
    }
}

#[derive(Default)]
pub struct SocketOpts {
    // The MTU to base calculations on.
    pub mtu: Option<usize>,

    // If MTU is not provided, this address will be used to detect MTU
    // once.
    pub mtu_autodetect_host: Option<IpAddr>,

    // How much memory to pre-allocate for incoming packet pool.
    pub packet_pool_max_memory: Option<usize>,

    // For flow control, if the user isn't reading, when to start dropping packets.
    // This is an approximate number in bytes.
    pub rx_bufsize_approx: Option<usize>,

    // How many out-of-order packets to track in the RX window.
    pub max_rx_out_of_order_packets: Option<usize>,

    // How many bytes to allocate for each virtual socket's TX.
    pub tx_bytes: Option<usize>,

    // Disable Nagle's algorithm (buffering outgoing packets)
    pub disable_nagle: bool,

    pub congestion: CongestionConfig,
}

impl SocketOpts {
    fn validate(&self) -> anyhow::Result<ValidatedSocketOpts> {
        #[derive(Clone, Copy)]
        struct MtuCalc {
            max_packet_size: usize,
            max_payload_size: usize,
        }

        fn calc(mtu: usize) -> anyhow::Result<MtuCalc> {
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
            Ok(MtuCalc {
                max_packet_size,
                max_payload_size,
            })
        }

        let (incoming, outgoing) = match self.mtu {
            Some(mtu) => {
                let v = calc(mtu)?;
                (v, v)
            }
            None => {
                let autodetect_host = self
                    .mtu_autodetect_host
                    .unwrap_or(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)));
                match ::mtu::interface_and_mtu(autodetect_host) {
                    Ok((iface, mtu)) => {
                        trace!(?autodetect_host, iface, mtu, "autodetected MTU");
                        let v = calc(mtu)?;
                        (v, v)
                    }
                    Err(e) => {
                        debug!(?autodetect_host, "error detecting MTU: {:#}", e);
                        let incoming = calc(DEFAULT_INCOMING_MTU).unwrap();
                        let outgoing = calc(DEFAULT_CONSERVATIVE_OUTGOING_MTU).unwrap();
                        (incoming, outgoing)
                    }
                }
            }
        };

        let max_user_rx_buffered_packets = self
            .rx_bufsize_approx
            .unwrap_or(DEFAULT_MAX_RX_BUF_SIZE_PER_VSOCK)
            / incoming.max_packet_size;

        if max_user_rx_buffered_packets == 0 {
            bail!("max_user_rx_buffered_packets = 0. Increase rx_bufsize_approx, or decrease MTU")
        }

        let max_rx_out_of_order_packets = self.max_rx_out_of_order_packets.unwrap_or(64);
        if max_rx_out_of_order_packets == 0 {
            bail!("invalid configuration: virtual_socket_tx_packets = 0");
        }
        let virtual_socket_tx_bytes = self.tx_bytes.unwrap_or(1024 * 1024);
        if virtual_socket_tx_bytes == 0 {
            bail!("invalid configuration: virtual_socket_tx_bytes = 0")
        }

        Ok(ValidatedSocketOpts {
            max_incoming_packet_size: incoming.max_packet_size,
            max_outgoing_payload_size: outgoing.max_payload_size,
            max_user_rx_buffered_packets,
            max_rx_out_of_order_packets,
            virtual_socket_tx_bytes,
            nagle: !self.disable_nagle,
            congestion: self.congestion,
        })
    }
}

#[derive(Clone, Copy, Default)]
pub(crate) struct ValidatedSocketOpts {
    pub max_incoming_packet_size: usize,
    pub max_outgoing_payload_size: usize,

    pub max_user_rx_buffered_packets: usize,

    pub max_rx_out_of_order_packets: usize,
    pub virtual_socket_tx_bytes: usize,

    pub nagle: bool,

    pub congestion: CongestionConfig,
}

pub(crate) struct StreamRequester {
    created_span: tracing::Span,
    tx: oneshot::Sender<UtpStream>,
}

impl StreamRequester {
    fn new(tx: oneshot::Sender<UtpStream>) -> Self {
        Self {
            created_span: tracing::Span::current(),
            tx,
        }
    }
}

pub(crate) enum ControlRequest {
    ConnectRequest(SocketAddr, ConnectToken, StreamRequester),
    ConnectDropped(SocketAddr, ConnectToken),

    Shutdown {
        remote: SocketAddr,
        conn_id_1: SeqNr,
        conn_id_2: SeqNr,
    },
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
            ControlRequest::Shutdown {
                remote,
                conn_id_1,
                conn_id_2,
            } => {
                write!(f, "Shutdown({remote}, {conn_id_1}, {conn_id_2})")
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
    requester: StreamRequester,
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

enum MatchSynWithAccept {
    Matched,
    SynInvalid(StreamRequester),
    ReceiverDead(Syn),
}

struct AcceptQueue {
    syns: VecDeque<Syn>,
    next_available_acceptor: Option<StreamRequester>,
    rx: mpsc::Receiver<StreamRequester>,
}

impl AcceptQueue {
    fn syn_queue_empty(&mut self) -> bool {
        self.syns.is_empty()
    }

    fn try_next_acceptor(&mut self) -> Option<StreamRequester> {
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

    accept_queue: AcceptQueue,

    // TODO: we need to insert here only once!
    pub(crate) streams: HashMap<Key, UnboundedSender<UtpMessage>>,
    connecting: HashMap<SocketAddr, ConnectingPerAddr>,
    control_rx: UnboundedReceiver<ControlRequest>,
    next_connection_id: SeqNr,
}

impl<T: Transport, E: UtpEnvironment> Dispatcher<T, E> {
    pub(crate) async fn run_forever(mut self) -> anyhow::Result<()> {
        let mut read_buf = [0u8; 16384];

        loop {
            self.run_once(&mut read_buf).await?;
        }
    }

    async fn run_once(&mut self, read_buf: &mut [u8]) -> anyhow::Result<()> {
        // This should get us into a state where either there's no SYNs or no accepts available.
        self.cleanup_accept_queue()?;

        tokio::select! {
            accept = self.accept_queue.rx.recv(), if !self.accept_queue.syn_queue_empty() => {
                let accept = accept.unwrap();
                assert!(self.accept_queue.next_available_acceptor.is_none());
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
                let packet = Packet::new(&read_buf[..len]);
                self.on_recv(addr, packet, len)?;
            }
        }

        Ok(())
    }

    // Get into a state where we can't match acceptors with cached SYNs anymore.
    fn cleanup_accept_queue(&mut self) -> anyhow::Result<()> {
        while !self.accept_queue.syns.is_empty() {
            let acceptor = match self.accept_queue.try_next_acceptor() {
                Some(acc) => acc,
                None => return Ok(()),
            };
            let syn = self.accept_queue.syns.pop_front().unwrap();
            match self.match_syn_with_accept(syn, acceptor) {
                MatchSynWithAccept::Matched => continue,
                MatchSynWithAccept::SynInvalid(sender) => {
                    self.accept_queue.next_available_acceptor = Some(sender);
                }
                MatchSynWithAccept::ReceiverDead(syn) => {
                    self.accept_queue.syns.push_front(syn);
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

    // This would only block if the socket is full. It will block the dispatcher but it's a resonable tradeoff.
    #[tracing::instrument(level = "trace", skip(self))]
    async fn on_control(&mut self, msg: ControlRequest) {
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
                    extensions: Default::default(),
                };
                let mut buf = [0u8; UTP_HEADER_SIZE];
                header.serialize(&mut buf).unwrap();
                match self.socket.transport.send_to(&buf, addr).await {
                    Ok(len) if len == buf.len() => {}
                    Ok(len) => {
                        warn!(
                            len,
                            expected_len = buf.len(),
                            ?addr,
                            "did not send full length, dropping"
                        );
                        return;
                    }
                    Err(e) => {
                        debug!(?addr, "error sending, dropping connect(): {e:#}");
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
    }

    #[tracing::instrument(level = "trace", skip_all, fields(addr, seq_nr=?msg.header.seq_nr, ack_nr=?msg.header.ack_nr))]
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
        let args = StreamArgs::new_outgoing(&msg.header, conn.start, now)
            .with_parent_span(conn.requester.created_span.clone());

        let stream = UtpStream::new(&self.socket, addr, rx, args);
        let key1 = (addr, msg.header.connection_id);
        let key2 = (addr, msg.header.connection_id + 1);

        if self.streams.contains_key(&key1) || self.streams.contains_key(&key2) {
            warn!("clashing sequence numbers, dropping");
            // TODO: send RST or FIN
            return Ok(());
        }

        if conn.requester.tx.send(stream).is_ok() {
            trace!("created stream and passed to connector");
            self.streams.insert(key1, tx.clone());
            self.streams.insert(key2, tx);
        } else {
            debug!("connecting receiver is dead. dropping");
        }

        Ok(())
    }

    fn match_syn_with_accept(&mut self, syn: Syn, accept: StreamRequester) -> MatchSynWithAccept {
        let key1 = (syn.remote, syn.header.connection_id);
        let key2 = (syn.remote, syn.header.connection_id + 1);
        if self.streams.contains_key(&key1) || self.streams.contains_key(&key2) {
            trace!("invalid SYN, it clashes, dropping");
            return MatchSynWithAccept::SynInvalid(accept);
        }

        let args = StreamArgs::new_incoming(self.env.random_u16().into(), &syn.header)
            .with_parent_span(accept.created_span.clone());
        let (tx, rx) = unbounded_channel();
        let stream = UtpStream::new(&self.socket, syn.remote, rx, args);

        if accept.tx.send(stream).is_ok() {
            trace!("created stream and passed to acceptor");
            self.streams.insert(key1, tx.clone());
            self.streams.insert(key2, tx);
            MatchSynWithAccept::Matched
        } else {
            MatchSynWithAccept::ReceiverDead(syn)
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
            }
        }
        match self.accept_queue.try_cache_syn(syn) {
            true => Ok(()),
            false => {
                warn!("dropping SYN, no more space to cache them and no acceptors available");
                // TODO: send anything?
                Ok(())
            }
        }
    }

    #[tracing::instrument(level = "trace", name = "on_recv", skip_all, fields(from=?addr))]
    fn on_recv(&mut self, addr: SocketAddr, packet: Packet, len: usize) -> anyhow::Result<()> {
        trace!("received");

        let message = match UtpMessage::deserialize(packet, len) {
            Some(msg) => msg,
            None => {
                debug!(len, "error desserializing and validating UTP message");
                return Ok(());
            }
        };

        let span = trace_span!(
            "msg",
            conn_id=?message.header.connection_id,
            type=?message.header.get_type(),
            seq_nr=?message.header.seq_nr,
            ack_nr=?message.header.ack_nr,
            payload=message.payload().len()
        );
        let _span = span.entered();

        trace!("parsed");

        let key = (addr, message.header.connection_id);

        if let Some(tx) = self.streams.get(&key) {
            if tx.send(message).is_err() {
                warn!(
                    ?key,
                    "stream dead. this branch should be rare and is probably a bug"
                );
                // TODO: we should clean up the other one too? it should have been deleted by
                // the UtpStream drop guards, maybe it will.
                self.streams.remove(&key);
            }
            return Ok(());
        }

        trace!(
            exist = ?self.streams.keys().collect::<Vec<_>>(),
            ?key,
            "no matching live streams"
        );

        match message.header.get_type() {
            Type::ST_STATE => {
                self.on_maybe_connect_ack(addr, message)?;
            }
            Type::ST_SYN => {
                self.on_syn(addr, message)?;
            }
            _ => {
                debug!("dropping packet");
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
    pub(crate) control_requests: UnboundedSender<ControlRequest>,
    accept_requests: mpsc::Sender<StreamRequester>,

    pub(crate) env: E,

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

pub type UtpSocketUdp = UtpSocket<tokio::net::UdpSocket, DefaultUtpEnvironment>;

impl UtpSocketUdp {
    pub async fn new_udp(bind_addr: SocketAddr) -> anyhow::Result<Arc<Self>> {
        Self::new_udp_with_opts(bind_addr, Default::default()).await
    }

    pub async fn new_udp_with_opts(
        bind_addr: SocketAddr,
        opts: SocketOpts,
    ) -> anyhow::Result<Arc<Self>> {
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

        let (accept_tx, accept_rx) = mpsc::channel(ACCEPT_QUEUE_MAX_ACCEPTORS);
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
        self.accept_requests
            .send(StreamRequester::new(tx))
            .await
            .context("dispatcher dead")?;

        let stream = rx.await.context("dispatcher dead")?;
        Ok(stream)
    }

    #[tracing::instrument(level = "debug", name="utp_socket:connect", skip(self), fields(local=?self.local_addr))]
    pub async fn connect(self: &Arc<Self>, remote: SocketAddr) -> anyhow::Result<UtpStream> {
        let (tx, rx) = oneshot::channel();
        let token = NEXT_CONNECT_TOKEN.fetch_add(1, Ordering::Relaxed);
        self.control_requests
            .send(ControlRequest::ConnectRequest(
                remote,
                token,
                StreamRequester::new(tx),
            ))
            .context("dispatcher dead")?;

        let mut guard = DropGuardSendBeforeDeath::new(
            ControlRequest::ConnectDropped(remote, token),
            &self.control_requests,
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
                debug!("UDP socket full, could not send packet");
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

        try_join!(connect, accept)?;
        Ok(())
    }
}
