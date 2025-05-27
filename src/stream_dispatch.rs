#[cfg(test)]
mod tests;

use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::Arc,
    task::Poll,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use tokio::{sync::mpsc::UnboundedReceiver, time::Sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error_span, event, trace, trace_span, Level};

use crate::{
    congestion::CongestionController,
    constants::{
        calc_pipe_expiry, ACK_DELAY, HARD_IMMEDIATE_ACK_EVERY_RMSS, RECOVERY_TRACING_LOG_LEVEL,
        RTTE_TRACING_LOG_LEVEL, SOFT_IMMEDIATE_ACK_EVERY_RMSS, SYNACK_RESEND_INTERNAL,
    },
    message::UtpMessage,
    metrics::METRICS,
    raw::{Type, UtpHeader},
    recovery::Recovery,
    rtte::RttEstimator,
    seq_nr::SeqNr,
    socket::{ControlRequest, ValidatedSocketOpts},
    spawn_utils::spawn_with_cancel,
    stream::UtpStream,
    stream_rx::{AssemblerAddRemoveResult, UserRx},
    stream_tx::{UserTx, UtpStreamWriteHalf},
    stream_tx_segments::{OnAckResult, Segments},
    traits::{Transport, UtpEnvironment},
    utils::{update_optional_waker, DropGuardSendBeforeDeath},
    UtpSocket,
};

// TODO: as FIN works differently from TCP, we need to refactor states to simplify them.

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VirtualSocketState {
    SynReceived,
    SynAckSent { expires_at: Instant, count: usize },

    Established,

    // We sent FIN, not yet ACKed
    FinWait1 { our_fin: SeqNr },

    // Our fin was ACKed
    FinWait2,

    // Both sides closed, we are waiting for final ACK.
    // After this we just kill the socket.
    LastAck { our_fin: SeqNr, remote_fin: SeqNr },

    // We are fully done, no more packets to be sent or received.
    Closed,
}

impl VirtualSocketState {
    fn is_closed(&self, wait_for_last_ack: bool) -> bool {
        match self {
            VirtualSocketState::Closed => true,
            VirtualSocketState::LastAck { .. } if !wait_for_last_ack => true,
            _ => false,
        }
    }

    fn name(&self) -> &'static str {
        match self {
            VirtualSocketState::SynReceived => "syn-received",
            VirtualSocketState::SynAckSent { .. } => "syn-ack-sent",
            VirtualSocketState::Established => "established",
            VirtualSocketState::FinWait1 { .. } => "fin-wait-1",
            VirtualSocketState::FinWait2 => "fin-wait-2",
            VirtualSocketState::LastAck { .. } => "last-ack",
            VirtualSocketState::Closed => "closed",
        }
    }

    fn transition_to_fin_sent(&mut self, our_fin: SeqNr) -> bool {
        match *self {
            VirtualSocketState::Established
            | VirtualSocketState::SynReceived
            | VirtualSocketState::SynAckSent { .. } => {
                trace!("state {} -> fin-wait-1", self.name());
                *self = VirtualSocketState::FinWait1 { our_fin }
            }
            _ => return false,
        };
        true
    }

    // True if we are not sending any more data.
    fn is_local_fin_or_later(&self) -> bool {
        match self {
            VirtualSocketState::SynReceived
            | VirtualSocketState::SynAckSent { .. }
            | VirtualSocketState::Established => false,
            VirtualSocketState::Closed
            | VirtualSocketState::FinWait1 { .. }
            | VirtualSocketState::FinWait2
            | VirtualSocketState::LastAck { .. } => true,
        }
    }

    fn our_fin_if_unacked(&self) -> Option<SeqNr> {
        match *self {
            VirtualSocketState::FinWait1 { our_fin }
            | VirtualSocketState::LastAck { our_fin, .. } => Some(our_fin),
            _ => None,
        }
    }

    fn is_remote_fin_or_later(&self) -> bool {
        matches!(
            self,
            VirtualSocketState::LastAck { .. } | VirtualSocketState::Closed
        )
    }
}

// An equivalent of a TCP socket for uTP.
struct VirtualSocket<T, Env> {
    state: VirtualSocketState,
    socket: Arc<UtpSocket<T, Env>>,
    socket_created: Instant,
    socket_opts: ValidatedSocketOpts,

    remote: SocketAddr,
    conn_id_send: SeqNr,

    // Triggers delay-based operations
    timers: Timers,

    // The last seen value of uTP's "last_remote_timestamp"
    last_remote_timestamp: u32,
    last_remote_window: u32,

    // The seq_nr to use in outgoing packets. It gets incremented AFTER the
    // following cases:
    // 1. Initial SYN is sent.
    // 2. ST_DATA is sent
    // 3. ST_FIN is sent.
    seq_nr: SeqNr,

    // This is used to rewind state back for retransmission.
    // All packets after this number will get resent (ST_DATA, ST_FIN).
    last_sent_seq_nr: SeqNr,

    // Last remote sequence number that we fully processed.
    last_consumed_remote_seq_nr: SeqNr,

    // Last ACK that we sent out. This is different from "last_consumed_remote_seq_nr" because we don't ACK
    // every packet. This must be <= last_consumed_remote_seq_nr.
    last_sent_ack_nr: SeqNr,

    last_sent_window: u32,

    // How many bytes we have consumed but not sent an ACK yet.
    // Used for immediate ACKs on 2 * RMSS threshold.
    consumed_but_unacked_bytes: usize,

    // Incoming queue. The main UDP socket writes here, and we need to consume these
    // as fast as possible.
    //
    // TODO: make bounded
    rx: UnboundedReceiver<UtpMessage>,
    user_rx: UserRx,

    // The user ppayload that we haven't yet segmented into uTP messages.
    // This is what "UtpStream::poll_write" writes to.
    user_tx: Arc<UserTx>,
    // Unacked segments. Ready to send or retransmit.
    user_tx_segments: Segments,

    rtte: RttEstimator,
    congestion_controller: Box<dyn CongestionController>,

    recovery: Recovery,

    this_poll: ThisPoll,

    env: Env,

    drop_guard: DropGuardSendBeforeDeath<ControlRequest>,
    parent_span: Option<tracing::Span>,

    #[cfg(feature = "per-connection-metrics")]
    metrics: crate::metrics::PerConnectionMetrics,
}

// Updated on every poll
struct ThisPoll {
    // Set here once per poll for consistent reproducible calculations.
    now: Instant,

    // Temp buffer used for writing messages to socket, so that we dno't alloc and zero
    // it every time.
    tmp_buf: Vec<u8>,

    // Set if the transport can't send anything this poll.
    transport_pending: bool,
}

#[derive(Default)]
struct ProcessIncomingMessageResult {
    on_ack_result: OnAckResult,
}

impl ProcessIncomingMessageResult {
    pub(crate) fn update(&mut self, other: &ProcessIncomingMessageResult) {
        self.on_ack_result.update(&other.on_ack_result);
    }
}

// TODO: this is nasty, but we can't make these methods as it borrows a bunch of fields, and this whole
// section is run while user_tx_segments.iter_mut() is already borrowed.
macro_rules! on_packet_sent {
    ($self:expr, $header:expr) => {{
        // Each packet sent can act as ACK so update related state.
        $self.last_sent_ack_nr = $header.ack_nr;
        $self.last_sent_window = $header.wnd_size;
        $self.consumed_but_unacked_bytes = 0;
        $self.timers.reset_delayed_ack_timer();
        if matches!($header.get_type(), Type::ST_DATA) {
            $self.timers.remote_inactivity_timer =
                Some($self.this_poll.now + $self.socket_opts.remote_inactivity_timeout);
        }
    }};
}

macro_rules! send_data {
    ($self:expr, $cx:expr, $header:expr, $segment_iter_item:expr) => {{
        if $segment_iter_item.retransmit_count()
            == $self.socket_opts.max_segment_retransmissions.get()
        {
            METRICS.max_retransmissions_reached.increment(1);
            anyhow::bail!("max number of retransmissions reached");
        }

        $header.set_type(Type::ST_DATA);
        $header.seq_nr = $segment_iter_item.seq_nr();
        $header.timestamp_microseconds =
            ($self.env.now() - $self.socket_created).as_micros() as u32;
        $header.timestamp_difference_microseconds = $header
            .timestamp_microseconds
            .wrapping_sub($self.last_remote_timestamp);

        let len = $header
            .serialize_with_payload(&mut $self.this_poll.tmp_buf, |b| {
                let offset = $segment_iter_item.payload_offset();
                let len = $segment_iter_item.payload_size();
                let g = $self.user_tx.locked.lock();
                g.fill_buffer_from_ring_buffer(b, offset, len)
                    .context("error filling output buffer from user_tx")?;
                Ok(len)
            })
            .context("bug: wasn't able to serialize the buffer")?;

        $self.this_poll.transport_pending =
            $self
                .socket
                .try_poll_send_to($cx, &$self.this_poll.tmp_buf[..len], $self.remote)?;
        if !$self.this_poll.transport_pending {
            // This time better be as precise as possible as we are using it to
            // calculate recovery pipe (estimate of how many packets are in transit).
            // see rfc6675.
            $segment_iter_item.on_sent($self.env.now());
            on_packet_sent!($self, $header);

            #[cfg(feature = "per-connection-metrics")]
            $self.metrics.sent_bytes.increment(len as u64);

            if $segment_iter_item.seq_nr() > $self.last_sent_seq_nr {
                $self.last_sent_seq_nr = $segment_iter_item.seq_nr();
                $self.seq_nr = $segment_iter_item.seq_nr() + 1;
            }

            // rfc6298 5.1
            $self
                .timers
                .retransmit
                .arm($self.this_poll.now, $self.rtte.retransmission_timeout());
        }
        !$self.this_poll.transport_pending
    }};
}

impl<T: Transport, Env: UtpEnvironment> VirtualSocket<T, Env> {
    async fn run_forever(self) -> anyhow::Result<()> {
        self.await.context("error running utp stream event loop")
    }

    fn timestamp_microseconds(&self) -> u32 {
        (self.this_poll.now - self.socket_created).as_micros() as u32
    }

    /// Create a stub header for a new outgoing message.
    pub(crate) fn outgoing_header(&self) -> UtpHeader {
        let mut header = UtpHeader::default();
        header.connection_id = self.conn_id_send;
        header.timestamp_microseconds = self.timestamp_microseconds();
        header.timestamp_difference_microseconds = header
            .timestamp_microseconds
            .wrapping_sub(self.last_remote_timestamp);
        header.seq_nr = self.seq_nr;
        header.ack_nr = self.last_consumed_remote_seq_nr;
        header.wnd_size = self.rx_window();
        header
    }

    // TODO: implement this better
    // https://datatracker.ietf.org/doc/html/rfc9293#section-3.8.6.2.2
    fn rx_window(&self) -> u32 {
        let wnd = self.user_rx.remaining_rx_window() as u32;
        if (wnd as usize) < self.socket_opts.max_incoming_payload_size.get() {
            return 0;
        }
        let rmss = self.socket_opts.max_incoming_payload_size.get() as u32;
        wnd - (wnd % rmss)
    }

    fn send_tx_queue(&mut self, cx: &mut std::task::Context<'_>) -> anyhow::Result<()> {
        // No reason to send anything, we'll get polled next time.
        if self.this_poll.transport_pending {
            return Ok(());
        }

        let mut header = self.outgoing_header();

        // Retransmit timer expired, rewind state backwards.
        if self.timers.retransmit.expired(self.this_poll.now) {
            debug!("retransmit timer expired");
            METRICS.rto_timeouts_count.increment(1);

            if let Some(mut seg) = self.user_tx_segments.iter_mut_for_sending(None).next() {
                if send_data!(self, cx, header, seg) {
                    debug!(
                        %header.seq_nr,
                        %header.ack_nr,
                        payload_size = seg.payload_size(),
                        "RTO expired: sent ST_DATA"
                    );
                    self.congestion_controller
                        .on_retransmission_timeout(self.this_poll.now);
                    self.rtte.on_rto_timeout();
                    self.recovery.on_rto_timeout(self.last_sent_seq_nr);
                    // Rewind back last sent seq_nr so that normal sending resumes.
                    self.last_sent_seq_nr = seg.seq_nr();
                } else {
                    // This will retry on next poll.
                    return Ok(());
                }
            } else {
                // There's no data to send. Maybe we need to resend FIN? Check.
                match self.state.our_fin_if_unacked() {
                    Some(our_fin_seq_nr) if self.last_sent_seq_nr == our_fin_seq_nr => {
                        debug!("RTO: rewinding self.last_sent_seq_nr to retransmit FIN");
                        self.last_sent_seq_nr -= 1;
                    }
                    _ => {
                        debug!("retransmit timer expired, but nothing to send");
                        self.timers.retransmit.turn_off();
                    }
                }
            }
        }

        if self.user_tx_segments.is_empty() {
            return Ok(());
        }

        let mut in_recovery = false;

        // If we are in recovery, retransmit as many unacked packets as we are allowed by the recovery algorithm.
        if let Some(rec) = self.recovery.recovering_mut() {
            in_recovery = true;
            let high_rxt = rec.high_rxt;
            let recovery_point = rec.recovery_point();
            let sack_depth = self.user_tx_segments.sack_depth();
            let mut it = self
                .user_tx_segments
                .iter_mut_for_sending(None)
                .take(sack_depth + 1)
                .skip_while(|seg| seg.seq_nr() <= high_rxt)
                .take_while(|seg| seg.seq_nr() <= recovery_point)
                .filter(|s| !s.is_delivered());

            let mut cwnd = rec.cwnd();
            let mut sent = 0;
            let mss = self.socket_opts.max_outgoing_payload_size.get();
            while rec.total_retransmitted_segments() == 0 || cwnd > mss {
                let mut seg = match it.next() {
                    Some(seg) => seg,
                    None => break,
                };

                // we MUST transmit the first segment no matter what.
                if rec.total_retransmitted_segments() > 0 {
                    if !seg.is_lost() {
                        event!(
                            RECOVERY_TRACING_LOG_LEVEL,
                            "skipping segment that is not lost"
                        );
                        continue;
                    }

                    if !seg.has_sacks_after_it() {
                        event!(
                            RECOVERY_TRACING_LOG_LEVEL,
                            seq_nr = ?seg.seq_nr(),
                            "stopping iteration. has no sacks after it"
                        );
                        break;
                    }
                }

                if send_data!(self, cx, header, seg) {
                    event!(
                        RECOVERY_TRACING_LOG_LEVEL,
                        %header.seq_nr,
                        %header.ack_nr,
                        payload_size = seg.payload_size(),
                        pipe = rec.pipe_estimate.pipe,
                        cwnd,
                        is_lost = seg.is_lost(),
                        is_expired = seg.is_expired(),
                        "RECOVERY: sent ST_DATA"
                    );
                    rec.high_rxt = seg.seq_nr();
                    rec.increment_total_transmitted_segments();
                    rec.pipe_estimate.pipe += seg.payload_size();
                    cwnd = cwnd.saturating_sub(seg.payload_size());
                    sent += 1;
                } else {
                    return Ok(());
                }
            }

            event!(
                RECOVERY_TRACING_LOG_LEVEL,
                ?sent,
                pipe = rec.pipe_estimate.pipe,
                remaining_cwnd = cwnd,
                "recovery sending"
            );

            if cwnd < mss {
                match rec.pipe_estimate.recalc_timer {
                    Some(t) => {
                        self.timers.recovery_pipe_expiry = Some(t);
                    }
                    None if sent > 0 => {
                        self.timers.recovery_pipe_expiry =
                            Some(self.this_poll.now + calc_pipe_expiry(self.rtte.roundtrip_time()));
                    }
                    _ => {}
                }
            }

            // Retransmit FIN if needed.
            if let Some(our_fin) = self.state.our_fin_if_unacked() {
                if rec.high_rxt == our_fin - 1 {
                    // Rewind last_sent_seq_nr to re-send FIN
                    self.last_sent_seq_nr = our_fin - 1;
                    rec.high_rxt = our_fin;
                    rec.increment_total_transmitted_segments();
                    // No reason to do anything further.
                    return Ok(());
                }
            }
        }

        let mut remaining_cwnd = self
            .recovery
            .remaining_cwnd(self.last_remote_window)
            .unwrap_or_else(|| {
                self.congestion_controller
                    .window()
                    .min(self.last_remote_window as usize)
                    .saturating_sub(
                        self.user_tx_segments
                            .calc_flight_size(self.last_sent_seq_nr),
                    )
            });

        let mut sent_count = 0;

        // Send the stuff we haven't sent yet, up to sender's window.
        for mut item in self
            .user_tx_segments
            .iter_mut_for_sending(Some(self.last_sent_seq_nr + 1))
        {
            if remaining_cwnd < item.payload_size() {
                METRICS.send_window_exhausted.increment(1);
                trace_every_ms!(100, "remote recv window exhausted");
                break;
            }

            if send_data!(self, cx, header, item) {
                trace!(
                    %header.seq_nr,
                    %header.ack_nr,
                    payload_size = item.payload_size(),
                    remaining_cwnd,
                    "sent ST_DATA"
                );
                remaining_cwnd -= item.payload_size();
                sent_count += 1;
            } else {
                break;
            }

            if self.this_poll.transport_pending {
                break;
            }
        }

        if sent_count == 0 {
            trace!(remaining_cwnd, "did not send anything");
        } else if in_recovery {
            METRICS
                .recovery_transmitted_new_segments_count
                .increment(sent_count);
            event!(
                RECOVERY_TRACING_LOG_LEVEL,
                sent_count,
                remaining_cwnd = remaining_cwnd,
                "sent in recovery"
            );
        } else {
            trace!(remaining_cwnd, "remaining recv_wnd after sending");
        }

        Ok(())
    }

    fn maybe_send_ack(&mut self, cx: &mut std::task::Context<'_>) -> anyhow::Result<bool> {
        if self.immediate_ack_to_transmit() {
            METRICS.immediate_acks.increment(1);
            return self.send_ack(cx);
        }
        if self.delayed_ack_expired() {
            if self.ack_to_transmit() {
                METRICS.delayed_acks.increment(1);
                trace!("delayed ack expired, sending ACK");
                self.send_ack(cx)
            } else {
                self.timers.reset_delayed_ack_timer();
                Ok(false)
            }
        } else {
            Ok(false)
        }
    }

    fn on_packet_sent(&mut self, header: &UtpHeader) {
        on_packet_sent!(self, header)
    }

    fn send_control_packet(
        &mut self,
        cx: &mut std::task::Context<'_>,
        header: UtpHeader,
    ) -> anyhow::Result<bool> {
        if self.this_poll.transport_pending {
            return Ok(false);
        }

        trace!(
            seq_nr = %header.seq_nr,
            ack_nr = %header.ack_nr,
            wnd_size = %header.wnd_size,
            type = ?header.get_type(),
            "sending"
        );

        let len = header
            .serialize(&mut self.this_poll.tmp_buf)
            .context("bug")?;

        self.this_poll.transport_pending =
            self.socket
                .try_poll_send_to(cx, &self.this_poll.tmp_buf[..len], self.remote)?;

        let sent = !self.this_poll.transport_pending;

        if sent {
            METRICS.sent_control_packets.increment(1);
            #[cfg(feature = "per-connection-metrics")]
            {
                self.metrics.sent_bytes.increment(len as u64);
            }
            self.on_packet_sent(&header);
        } else {
            METRICS.unsent_control_packets.increment(1);
        }

        Ok(sent)
    }

    fn send_ack(&mut self, cx: &mut std::task::Context<'_>) -> anyhow::Result<bool> {
        let mut header = self.outgoing_header();
        header.extensions.selective_ack = self.user_rx.selective_ack();
        self.send_control_packet(cx, header)
    }

    fn maybe_send_fin(&mut self, cx: &mut std::task::Context<'_>) -> anyhow::Result<()> {
        if self.this_poll.transport_pending {
            return Ok(());
        }

        let seq_nr = match self.state.our_fin_if_unacked() {
            Some(seq_nr) => seq_nr,
            None => return Ok(()),
        };

        // Only send fin after all the outstanding data was sent.
        if seq_nr - self.last_sent_seq_nr != 1 {
            return Ok(());
        }

        let mut fin = self.outgoing_header();
        fin.set_type(Type::ST_FIN);
        fin.seq_nr = seq_nr;
        if self.send_control_packet(cx, fin)? {
            self.timers
                .retransmit
                .arm(self.this_poll.now, self.rtte.retransmission_timeout());
            self.timers.remote_inactivity_timer =
                Some(self.this_poll.now + self.socket_opts.remote_inactivity_timeout);
            self.last_sent_seq_nr = seq_nr;
        }
        Ok(())
    }

    fn split_tx_queue_into_segments(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> anyhow::Result<()> {
        let mut g = self.user_tx.locked.lock();

        if g.is_empty() {
            update_optional_waker(&mut g.dispatcher_waker, cx);
            return Ok(());
        }

        if self.state.is_remote_fin_or_later() {
            debug!(?self.state, "there is still unsent data, but the remote closed, so not segmenting further");
            return Ok(());
        }

        let segmented_len = self.user_tx_segments.total_len_bytes();

        if g.len() < self.user_tx_segments.total_len_bytes() {
            bail!(
                "bug in buffer computations: user_tx_buflen={} segmented_len={}",
                g.len(),
                segmented_len
            );
        }

        let mut remaining = g.len() - segmented_len;

        // TODO: need to refactor this to play nicely with recovery etc
        let mut remote_window_remaining = self.last_remote_window as usize;

        trace!(
            remote_window_remaining,
            congestion_controller_window = self.congestion_controller.window(),
            remaining,
            self.last_remote_window
        );

        while !self.user_tx_segments.is_full() && remaining > 0 && remote_window_remaining > 0 {
            let max_payload_size = self
                .socket_opts
                .max_outgoing_payload_size
                .get()
                .min(remote_window_remaining);
            let payload_size = max_payload_size.min(remaining);

            // Run Nagle algorithm to prevent sending too many small segments.
            {
                let can_send_full_payload = payload_size == max_payload_size;
                let data_in_flight = !self.user_tx_segments.is_empty();

                if self.socket_opts.nagle && !can_send_full_payload && data_in_flight {
                    trace!(payload_size, max_payload_size, "nagle: buffering more data");
                    break;
                }
            }

            if !self.user_tx_segments.enqueue(payload_size) {
                bail!("bug, can't enqueue next segment")
            }
            remaining -= payload_size;
            remote_window_remaining -= payload_size;
            trace!(bytes = payload_size, "segmented");
        }

        trace!(
            remaining,
            remote_window_remaining,
            user_tx_segments_segments = self.user_tx_segments.total_len_packets(),
            user_tx_segments_bytes = self.user_tx_segments.total_len_bytes(),
            user_tx_segments_full = self.user_tx_segments.is_full(),
            "split_tx_queue_into_segments finished",
        );

        Ok(())
    }

    fn just_before_death(
        &mut self,
        cx: &mut std::task::Context<'_>,
        error: Option<&anyhow::Error>,
    ) {
        if let Some(err) = error {
            trace!("just_before_death: {err:#}");
        } else {
            trace!("just_before_death: no error");
        }

        if let Some(e) = error {
            self.user_rx.enqueue_error(format!("{e:#}"));
        }

        // This will close the reader.
        self.user_rx.mark_vsock_closed();

        // This will close the writer.
        self.user_tx.mark_vsock_closed();

        if error.is_some() && !self.state.is_local_fin_or_later() {
            let mut fin = self.outgoing_header();
            fin.set_type(Type::ST_FIN);
            fin.seq_nr = self.seq_nr;
            self.seq_nr += 1;
            if let Err(e) = self.send_control_packet(cx, fin) {
                debug!("error sending FIN: {e:#}")
            }
        }
    }

    fn state_is_closed(&self) -> bool {
        self.state.is_closed(self.socket_opts.wait_for_last_ack)
    }

    fn process_all_incoming_messages(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> anyhow::Result<()> {
        let mut result = ProcessIncomingMessageResult::default();

        let mut counter = 0u32;
        while let Poll::Ready(msg) = self.rx.poll_recv(cx) {
            let msg = match msg {
                Some(msg) => msg,
                None => {
                    // This could happen only when we were removed from socket.streams(), of if socket dispatcher is dead.
                    // This should not have happened!
                    // But if it did, we won't be able to receive any more messages.
                    // We need to send FIN in this case, but we can't wait for its ACK to arrive back, cause we can't
                    // receive any messages!

                    debug!("can't receive messages anymore. Transitioning to Closed");
                    self.transition_to_fin_sent();
                    self.maybe_send_fin(cx)?;
                    log_if_changed!(Level::DEBUG, "state", self, |s| s.state, |s| s.state =
                        VirtualSocketState::Closed);
                    return Ok(());
                }
            };
            result.update(&self.process_incoming_message(cx, msg)?);
            counter += 1;
            if self.state_is_closed() {
                break;
            }
        }

        METRICS.recved_packets_in_batch.record(counter);

        if result.on_ack_result.acked_segments_count > 0
            || result.on_ack_result.newly_sacked_segment_count > 0
        {
            // TODO: figure out why did I put this here?
            self.timers.remote_inactivity_timer = None;

            // Update RTT and RTO if not in recovery. In recovery we get very delayed info
            // for packets beyond sack depth.
            if let (false, Some(rtt)) =
                (self.recovery.is_recovering(), result.on_ack_result.new_rtt)
            {
                METRICS.rtt.record(rtt.as_secs_f64());
                log_every_ms_if_changed!(
                    500,
                    RTTE_TRACING_LOG_LEVEL,
                    "rtte:sample",
                    self,
                    |s| s.rtte,
                    |s| s.rtte.sample(rtt)
                );
            }

            // Reset retransmit timer.
            if self.user_tx_segments.is_empty() && self.state.our_fin_if_unacked().is_none() {
                // rfc6298 5.2. If all outstanding data ACKed, turn off the timer.
                self.timers.retransmit.turn_off();
            } else {
                // rfc6298 5.3. When an ACK is received that acknowledges new data, restart the
                // retransmission timer.
                self.timers
                    .retransmit
                    .arm(self.this_poll.now, self.rtte.retransmission_timeout());
            }
        }

        if result.on_ack_result.acked_segments_count > 0 {
            // Cleanup user side of TX queue, remove the ACKed bytes from the front of it,
            // and notify the writer.
            {
                let mut g = self.user_tx.locked.lock();
                g.truncate_front(result.on_ack_result.acked_bytes)?;

                let waker = g.writer_waker.take();

                // Waking under lock may slow things down.
                drop(g);

                if let Some(w) = waker {
                    w.wake();
                }
            }

            trace!(?result.on_ack_result, "removed ACKed tx messages");
        }

        if let Some(rec) = self.recovery.recovering_mut() {
            rec.pipe_estimate = self.user_tx_segments.calc_pipe(
                rec.high_rxt,
                self.last_sent_seq_nr,
                self.rtte.roundtrip_time(),
                self.this_poll.now,
            );
        }

        #[cfg(feature = "per-connection-metrics")]
        {
            let cwnd = self
                .recovery
                .cwnd()
                .unwrap_or_else(|| self.congestion_controller.window());

            let sshthresh = self.congestion_controller.sshthresh();
            let flight_size = self
                .user_tx_segments
                .calc_flight_size(self.last_sent_seq_nr);
            self.metrics.cwnd.record(cwnd as f64);
            if sshthresh < usize::MAX {
                self.metrics.sshthresh.record(sshthresh as f64);
            }
            self.metrics.flight_size.record(flight_size as f64);
        }

        Ok(())
    }

    fn process_incoming_message(
        &mut self,
        cx: &mut std::task::Context<'_>,
        msg: UtpMessage,
    ) -> anyhow::Result<ProcessIncomingMessageResult> {
        // We are not using tracing::instrument here as it makes rust-analyzer work worse
        let span = trace_span!("msg",
            seq_nr=%msg.header.seq_nr,
            ack_nr=%msg.header.ack_nr,
            len=msg.payload().len(),
            msgtype=?msg.header.get_type()
        );
        let _span_g = span.enter();

        trace!("processing message");

        // Process state changes and invalid packets.
        use Type::*;
        use VirtualSocketState::*;
        let hdr = &msg.header;

        let previously_seen_remote_fin = self.state.is_remote_fin_or_later();

        match (self.state, hdr.get_type()) {
            // From real world packets: if ST_RESET acks our FIN, it's ok
            (LastAck { our_fin, .. }, ST_RESET) if hdr.ack_nr == our_fin => {
                self.state = Closed;
                return Ok(Default::default());
            }
            (_, ST_RESET) => {
                self.state = Closed;
                bail!("ST_RESET received")
            }
            (_, ST_SYN) => {
                trace!("unexpected ST_SYN, ignoring");
                return Ok(Default::default());
            }
            (Closed, _) => {
                bail!("bug: received a packet in Closed state, we shouldn't have reached here");
            }
            (SynReceived, _) => {
                bail!("bug: unexpected packet in SynReceived state. We should have sent the SYN-ACK first.")
            }
            (SynAckSent { .. }, ST_DATA | ST_STATE) => {
                if hdr.ack_nr != self.seq_nr - 1 {
                    trace!("dropping packet, we expected a different ack_nr");
                    return Ok(Default::default());
                }

                trace!("state: syn-ack-sent -> established");
                self.timers.remote_inactivity_timer = None;
                self.state = Established;
            }
            (SynAckSent { .. }, ST_FIN) => {
                trace!("state: syn-ack-sent -> closed");
                self.state = Closed;
            }

            (Established, ST_DATA | ST_STATE) => {}

            // Fin received in an expected state, but its sequence number is wrong
            (Established | FinWait1 { .. } | FinWait2, ST_FIN)
                if hdr.seq_nr != self.last_consumed_remote_seq_nr + 1 =>
            {
                debug!(
                    hdr=%hdr.short_repr(),
                    "dropping FIN, expected seq_nr to be {}",
                    self.last_consumed_remote_seq_nr + 1
                );
                return Ok(Default::default());
            }

            (Established, ST_FIN) => {
                trace!("state: established -> last-ack");
                let our_fin = self.seq_nr;
                self.seq_nr += 1;
                self.state = LastAck {
                    our_fin,
                    remote_fin: hdr.seq_nr,
                }
            }

            (FinWait1 { our_fin }, ST_FIN) if hdr.ack_nr == our_fin => {
                trace!("state: fin-wait-1 -> closed");
                self.state = Closed;
            }
            (FinWait1 { our_fin }, ST_FIN) => {
                trace!("state: fin-wait-1 -> last-ack");
                self.state = LastAck {
                    our_fin,
                    remote_fin: hdr.seq_nr,
                }
            }

            (FinWait1 { our_fin }, ST_DATA | ST_STATE) if hdr.ack_nr == our_fin => {
                trace!("state: fin-wait-1 -> fin-wait-2");
                self.timers.remote_inactivity_timer = None;
                self.state = FinWait2;
                if hdr.htype == ST_STATE && hdr.seq_nr - self.last_consumed_remote_seq_nr == 1 {
                    // some clients sends back FIN + ACK as STATE with seq_nr + 1. This is proably one of them.
                    trace!("treating ST_STATE as FIN");
                    self.state = Closed;
                }
            }
            (FinWait1 { .. }, ST_DATA | ST_STATE) => {}
            (FinWait2, ST_FIN) => {
                trace!("state: fin-wait-2 -> closed");
                self.timers.remote_inactivity_timer = None;
                self.state = Closed;
            }
            (FinWait2, ST_DATA | ST_STATE) => {}

            (LastAck { our_fin, .. }, _) if hdr.ack_nr == our_fin => {
                trace!("state: last-ack -> closed");
                self.timers.remote_inactivity_timer = None;
                self.state = Closed;
            }

            (LastAck { remote_fin, .. }, _) if hdr.seq_nr > remote_fin => {
                if hdr.htype == ST_DATA {
                    debug!(hdr=%hdr.short_repr(), ?remote_fin, "received higher seq nr than remote FIN, dropping packet");
                    return Ok(Default::default());
                } else {
                    // We COULD drop this packet, but turns out it happens very often in the wild.
                    debug!(hdr=%hdr.short_repr(), ?remote_fin, "received higher seq nr than remote FIN, ignoring this");
                }
            }

            (LastAck { .. }, _) => {}
        }

        let result = ProcessIncomingMessageResult {
            on_ack_result: self
                .user_tx_segments
                .remove_up_to_ack(self.this_poll.now, &msg.header),
        };

        self.congestion_controller
            .set_remote_window(msg.header.wnd_size as usize);
        self.congestion_controller.on_ack(
            self.this_poll.now,
            result.on_ack_result.acked_bytes,
            &self.rtte,
        );
        self.last_remote_timestamp = msg.header.timestamp_microseconds;
        self.last_remote_window = msg.header.wnd_size;
        #[cfg(feature = "per-connection-metrics")]
        {
            self.metrics
                .last_remote_window
                .record(self.last_remote_window);
            self.metrics.received_packets.increment(1);
        }

        self.recovery.on_ack(
            &msg.header,
            &result.on_ack_result,
            &mut self.user_tx_segments,
            self.last_sent_seq_nr,
            &mut *self.congestion_controller,
            self.this_poll.now,
            self.rtte.roundtrip_time(),
        );

        let offset = msg.header.seq_nr - (self.last_consumed_remote_seq_nr + 1);
        if offset < 0 {
            trace!(
                %self.last_consumed_remote_seq_nr,
                "dropping message, we already ACKed it"
            );
            METRICS.incoming_already_acked_data_packets.increment(1);
            return Ok(result);
        }

        match msg.header.get_type() {
            ST_DATA => {
                trace!(payload_size = msg.payload().len(), "received ST_DATA");
                trace!(
                    offset,
                    %self.last_consumed_remote_seq_nr,
                    "adding ST_DATA message to assember"
                );

                let assembler_was_empty = self.user_rx.assembler_empty();
                let hdr = msg.header;

                match self
                    .user_rx
                    .add_remove(cx, msg, offset as usize)
                    .context("fatal error in user_rx")?
                {
                    AssemblerAddRemoveResult::Consumed {
                        sequence_numbers,
                        bytes,
                    } => {
                        if sequence_numbers > 0 {
                            trace!(sequence_numbers, "consumed messages");
                            METRICS
                                .consumed_data_seq_nrs
                                .increment(sequence_numbers as u64);
                            METRICS.consumed_bytes.increment(bytes as u64);
                        } else {
                            METRICS.out_of_order_packets.increment(1);
                            debug_every_ms!(500, header=%hdr.short_repr(), offset, ?self.last_consumed_remote_seq_nr, "out of order");
                        }

                        self.last_consumed_remote_seq_nr += sequence_numbers as u16;
                        self.consumed_but_unacked_bytes += bytes;
                        trace!(self.consumed_but_unacked_bytes);
                    }
                    AssemblerAddRemoveResult::Unavailable(_) => {
                        debug_every_ms!(500, header=%hdr.short_repr(), offset,
                            ?self.last_consumed_remote_seq_nr, "cannot reassemble message, ignoring it");
                    }
                    AssemblerAddRemoveResult::AlreadyPresent => {
                        debug_every_ms!(500, header=%hdr.short_repr(), offset,
                            ?self.last_consumed_remote_seq_nr, "already present in assembler");
                    }
                }

                // Per RFC 5681, we should send an immediate ACK when either:
                //  1) an out-of-order segment is received, or
                //  2) a segment arrives that fills in all or part of a gap in sequence space.
                if !self.user_rx.assembler_empty() || !assembler_was_empty {
                    trace!(
                        assembler_not_empty = !self.user_rx.assembler_empty(),
                        assembler_was_empty,
                        immediate_ack_to_transmit = self.immediate_ack_to_transmit(),
                        "forcing immediate ACK"
                    );
                    self.force_immediate_ack();
                }

                if !self.immediate_ack_to_transmit() && self.consumed_but_unacked_bytes > 0 {
                    let delay = if self.consumed_but_unacked_bytes
                        >= SOFT_IMMEDIATE_ACK_EVERY_RMSS
                            * self.socket_opts.max_incoming_payload_size.get()
                    {
                        Duration::from_millis(1)
                    } else {
                        ACK_DELAY
                    };
                    self.arm_ack_delay_timer(delay);
                }
            }
            ST_STATE => {}
            ST_RESET => bail!("ST_RESET received"),
            ST_FIN => {
                if let Some(close_reason) = msg.header.extensions.close_reason {
                    debug!("remote closed with {close_reason:?}");
                }

                self.force_immediate_ack();

                if !previously_seen_remote_fin {
                    self.last_consumed_remote_seq_nr = hdr.seq_nr;
                    self.user_rx.add_remove(cx, msg, offset as usize)?;
                    self.user_tx.mark_vsock_closed();
                }
            }
            ST_SYN => {
                debug!("ignoring unexpected ST_SYN packet: {:?}", msg.header);
            }
        }
        Ok(result)
    }

    fn should_send_window_update(&self) -> bool {
        if self.state.is_remote_fin_or_later() {
            return false;
        }
        // if we need to send a window update, also send immediately.
        // TODO: this is clumsy, but should do the job in case the reader was fully flow controlled.
        let rmss = self.socket_opts.max_incoming_payload_size.get() as u32;
        let current = self.rx_window();
        if current < rmss && self.last_sent_window >= rmss
            || self.last_sent_window < rmss && current >= rmss
        {
            trace!(
                self.last_sent_window,
                rmss,
                current = self.rx_window(),
                "need to send a window update"
            );
            return true;
        }
        false
    }

    /// Return whether to send ACK immediately due to the amount of unacknowledged data.
    /// https://datatracker.ietf.org/doc/html/rfc9293#section-3.8.6.3
    fn immediate_ack_to_transmit(&self) -> bool {
        self.should_send_window_update()
            || self.consumed_but_unacked_bytes
                >= HARD_IMMEDIATE_ACK_EVERY_RMSS * self.socket_opts.max_incoming_payload_size.get()
    }

    fn force_immediate_ack(&mut self) {
        self.consumed_but_unacked_bytes =
            HARD_IMMEDIATE_ACK_EVERY_RMSS * self.socket_opts.max_incoming_payload_size.get();
    }

    fn ack_to_transmit(&self) -> bool {
        self.last_consumed_remote_seq_nr > self.last_sent_ack_nr
    }

    fn delayed_ack_expired(&self) -> bool {
        match self.timers.ack_delay_timer {
            // This means we need to ack the packet immediately.
            AckDelayTimer::Idle => true,
            AckDelayTimer::Waiting(t) => t <= self.this_poll.now,
        }
    }

    fn arm_ack_delay_timer(&mut self, delay: Duration) {
        let new_expires = self.this_poll.now + delay;
        self.timers.ack_delay_timer = match self.timers.ack_delay_timer {
            AckDelayTimer::Idle => AckDelayTimer::Waiting(new_expires),
            AckDelayTimer::Waiting(expires) if expires < new_expires => {
                AckDelayTimer::Waiting(new_expires)
            }
            AckDelayTimer::Waiting(expires) => AckDelayTimer::Waiting(expires),
        };
    }

    // When do we need to send smth timer-based next time.
    fn next_poll_send_to_at(&mut self) -> PollAt {
        let want_ack = self.ack_to_transmit();

        let delayed_ack_poll_at = match (want_ack, self.timers.ack_delay_timer) {
            (false, _) => PollAt::Ingress,
            (true, AckDelayTimer::Idle) => PollAt::Ingress,
            (true, AckDelayTimer::Waiting(t)) => {
                trace!(expires_in=?t - self.this_poll.now, "delayed ACK timer");
                PollAt::Time(t)
            }
        };

        let inactivity_poll = self
            .timers
            .remote_inactivity_timer
            .map_or(PollAt::Ingress, PollAt::Time);

        let pipe_expiry_timer = self
            .timers
            .recovery_pipe_expiry
            .take()
            .map_or(PollAt::Ingress, PollAt::Time);

        // We wait for the earliest of our timers to fire.
        self.timers
            .retransmit
            .poll_at(self.this_poll.now)
            .min(delayed_ack_poll_at)
            .min(inactivity_poll)
            .min(pipe_expiry_timer)
    }

    fn maybe_send_syn_ack(&mut self, cx: &mut std::task::Context<'_>) -> anyhow::Result<()> {
        let sent_count = match self.state {
            VirtualSocketState::SynReceived => 0,
            VirtualSocketState::SynAckSent { expires_at, count }
                if expires_at < self.this_poll.now =>
            {
                count
            }
            _ => return Ok(()),
        };
        if sent_count == self.socket_opts.max_segment_retransmissions.get() {
            bail!("too many syn-acks sent")
        }
        if self.send_ack(cx)? {
            self.state = VirtualSocketState::SynAckSent {
                expires_at: self.this_poll.now + SYNACK_RESEND_INTERNAL,
                count: sent_count + 1,
            };

            if sent_count > 0 {
                METRICS.synack_retransmissions.increment(1);
            }
            self.timers.arm_in(cx, SYNACK_RESEND_INTERNAL);
        }
        Ok(())
    }

    fn transition_to_fin_sent(&mut self) {
        log_if_changed!(Level::DEBUG, "state", self, |s| s.state, |s| {
            if s.state.transition_to_fin_sent(s.seq_nr) {
                s.seq_nr += 1;
            }
        });
    }

    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> Poll<anyhow::Result<()>> {
        macro_rules! bail_if_err {
            ($e:expr) => {
                match $e {
                    Ok(val) => val,
                    Err(e) => {
                        self.just_before_death(cx, Some(&e));
                        return Poll::Ready(Err(e));
                    }
                }
            };
        }

        macro_rules! pending_if_cannot_send {
            ($e:expr) => {{
                let val = bail_if_err!($e);
                if self.this_poll.transport_pending {
                    return Poll::Pending;
                }
                val
            }};
        }

        // Doing this once here not to upgrade too often below.
        self.this_poll.transport_pending = false;
        self.this_poll.now = self.env.now();

        const MAX_ITERS: usize = 2;

        for _ in 0..MAX_ITERS {
            pending_if_cannot_send!(self.maybe_send_syn_ack(cx));

            // Read incoming stream.
            pending_if_cannot_send!(self.process_all_incoming_messages(cx));

            // Flow control: flush as many out of order messages to user RX as possible.
            bail_if_err!(self.user_rx.flush(cx));

            if self
                .timers
                .remote_inactivity_timer
                .is_some_and(|expires| expires <= self.this_poll.now)
            {
                METRICS.inactivity_timeouts.increment(1);
                let err =
                    anyhow::anyhow!("remote was inactive for too long. state: {:?}", self.state);
                self.just_before_death(cx, Some(&err));
                return Poll::Ready(Err(err));
            }

            bail_if_err!(self.split_tx_queue_into_segments(cx));

            // (Re)send tx queue.
            pending_if_cannot_send!(self.send_tx_queue(cx));

            if (self.user_rx.is_reader_dropped() && self.user_tx.is_writer_dropped())
                || self.user_tx.is_writer_shutdown()
            {
                self.transition_to_fin_sent();
            }

            // (Re)send a pending FIN if needed.
            pending_if_cannot_send!(self.maybe_send_fin(cx));

            // Send an ACK if nothing sent yet and sending an ACK is necessary.
            pending_if_cannot_send!(self.maybe_send_ack(cx));

            // Quit normally if both sides sent FIN and ACKed each other.
            if self.state_is_closed() {
                self.just_before_death(cx, None);
                return Poll::Ready(Ok(()));
            }

            // If we are done and there's nothing outstanding to send, give the remote last chance to
            // send a meaningful update (their FIN) or die.
            if self.state.is_local_fin_or_later() && self.user_tx_segments.is_empty() {
                let next_exp = self.this_poll.now + Duration::from_secs(1);
                match self.timers.remote_inactivity_timer {
                    Some(time) if time < next_exp => {}
                    _ => {
                        trace!("both halves are dead, arming inactivity timer in 1 second");
                        self.timers.remote_inactivity_timer = Some(next_exp)
                    }
                }
            }

            // If there's a timer-based next poll to run, arm the timer.
            match self.next_poll_send_to_at() {
                PollAt::Time(instant) => {
                    let duration = instant - self.this_poll.now;
                    trace!(sleep = ?duration, "arming timer");
                    if self.timers.arm_in(cx, duration) {
                        return Poll::Pending;
                    } else {
                        trace!(deadline = ?instant - self.this_poll.now, "failed arming timer, continuing poll loop");
                        continue;
                    }
                }
                PollAt::Ingress => return Poll::Pending,
            }
        }

        Poll::Ready(Err(anyhow::anyhow!(
            "bug: too many iterations in dispatcher",
        )))
    }
}

impl<T, E> Drop for VirtualSocket<T, E> {
    fn drop(&mut self) {
        METRICS.live_virtual_sockets.decrement(1);
        self.user_tx.mark_vsock_closed();
        self.user_rx.mark_vsock_closed();
    }
}

// See field descriptions / meanings in struct VirtualSocket
pub struct StreamArgs {
    conn_id_recv: SeqNr,
    conn_id_send: SeqNr,
    last_remote_timestamp: u32,

    seq_nr: SeqNr,
    last_sent_seq_nr: SeqNr,
    last_consumed_remote_seq_nr: SeqNr,
    last_sent_ack_nr: SeqNr,

    rtt: Option<Duration>,
    remote_window: u32,

    state: VirtualSocketState,
    parent_span: Option<tracing::Span>,
}

impl StreamArgs {
    pub fn new_outgoing(
        remote_ack: &UtpHeader,
        syn_sent_ts: Instant,
        ack_received_ts: Instant,
    ) -> Self {
        let conn_id = remote_ack.connection_id;
        Self {
            conn_id_recv: conn_id,
            conn_id_send: conn_id + 1,
            last_remote_timestamp: remote_ack.timestamp_microseconds,
            remote_window: remote_ack.wnd_size,

            // The next ST_DATA must be +1 from initial SYN.
            seq_nr: remote_ack.ack_nr + 1,
            last_sent_seq_nr: remote_ack.ack_nr,

            // On connect, the client sends a number that represents the next ST_DATA number.
            // Pretend that we saw the non-existing previous one.
            last_sent_ack_nr: remote_ack.seq_nr - 1,
            last_consumed_remote_seq_nr: remote_ack.seq_nr - 1,

            rtt: Some(ack_received_ts - syn_sent_ts),

            state: VirtualSocketState::Established,

            parent_span: None,
        }
    }

    pub fn new_incoming(next_seq_nr: SeqNr, remote_syn: &UtpHeader) -> Self {
        Self {
            conn_id_recv: remote_syn.connection_id + 1,
            conn_id_send: remote_syn.connection_id,
            last_remote_timestamp: remote_syn.timestamp_microseconds,
            remote_window: 0, // remote_syn.wnd_size should be 0 anyway. We can't send anything first.

            seq_nr: next_seq_nr,
            last_sent_seq_nr: next_seq_nr - 1,
            // The connecting client will send the next ST_DATA packet with seq_nr + 1.
            last_consumed_remote_seq_nr: remote_syn.seq_nr,
            last_sent_ack_nr: remote_syn.seq_nr,

            // For RTTE
            rtt: None,

            state: VirtualSocketState::SynReceived,
            parent_span: None,
        }
    }

    pub fn with_parent_span(mut self, parent_span: tracing::Span) -> Self {
        self.parent_span = Some(parent_span);
        self
    }
}

pub(crate) struct UtpStreamStarter<T, E> {
    stream: UtpStream,
    vsock: VirtualSocket<T, E>,
    cancellation_token: CancellationToken,
}

impl<T: Transport, E: UtpEnvironment> UtpStreamStarter<T, E> {
    pub fn start(self) -> UtpStream {
        let Self {
            stream,
            vsock,
            cancellation_token,
        } = self;

        let parent = vsock.parent_span.as_ref().and_then(|s| s.id());
        let span = error_span!(parent: parent, "utp_stream", remote=?vsock.remote);
        spawn_with_cancel(span, cancellation_token, vsock.run_forever());
        stream
    }

    // Exposed for tests.
    pub fn new(
        socket: &Arc<UtpSocket<T, E>>,
        remote: SocketAddr,
        rx: UnboundedReceiver<UtpMessage>,
        args: StreamArgs,
    ) -> UtpStreamStarter<T, E> {
        let StreamArgs {
            conn_id_recv,
            conn_id_send,
            last_remote_timestamp,
            seq_nr,
            last_sent_seq_nr,
            last_consumed_remote_seq_nr,
            last_sent_ack_nr,
            rtt,
            remote_window,
            parent_span,
            state,
        } = args;

        let (user_rx, read_half) = UserRx::build(
            socket.opts().max_user_rx_buffered_bytes,
            socket.opts().max_incoming_payload_size,
        );

        let user_tx = UserTx::new(socket.opts().virtual_socket_tx_bytes);
        let write_half = UtpStreamWriteHalf::new(user_tx.clone());

        let env = socket.env.copy();
        let now = env.now();

        let cancellation_token = socket.cancellation_token.child_token();

        let vsock = VirtualSocket {
            state,
            env,
            socket_opts: socket.opts().clone(),
            congestion_controller: {
                let mut ctrl = socket
                    .opts()
                    .congestion
                    .create(now, socket.opts().max_incoming_payload_size.get());
                ctrl.set_remote_window(remote_window as usize);
                ctrl
            },

            socket_created: socket.created,
            remote,
            conn_id_send,
            timers: Timers {
                retransmit: RetransmitTimer::new(),
                sleep: Box::pin(tokio::time::sleep(Duration::from_secs(0))),
                ack_delay_timer: AckDelayTimer::Idle,
                recovery_pipe_expiry: None,
                remote_inactivity_timer: if rtt.is_some() {
                    None
                } else {
                    Some(now + socket.opts().remote_inactivity_timeout)
                },
            },
            last_remote_timestamp,
            last_remote_window: remote_window,
            seq_nr,
            last_sent_seq_nr,
            last_consumed_remote_seq_nr,
            last_sent_ack_nr,
            consumed_but_unacked_bytes: 0,
            rx,
            user_tx_segments: Segments::new(
                seq_nr,
                socket.opts().virtual_socket_tx_bytes.get(),
                socket.opts().max_outgoing_payload_size.get(),
            ),
            user_tx,
            rtte: {
                let mut rtte = RttEstimator::default();
                if let Some(rtt) = rtt {
                    rtte.sample(rtt);
                }
                rtte
            },
            this_poll: ThisPoll {
                now,
                tmp_buf: vec![0u8; socket.opts().max_incoming_packet_size.get()],
                transport_pending: false,
            },
            parent_span,
            drop_guard: DropGuardSendBeforeDeath::new(
                ControlRequest::Shutdown((remote, conn_id_recv)),
                &socket.control_requests,
            ),
            user_rx,
            last_sent_window: if matches!(state, VirtualSocketState::Established) {
                // Pretend we sent the window so that it doesn't trigger a window update packet
                // without any data sent.
                socket.opts().max_user_rx_buffered_bytes.get() as u32
            } else {
                0
            },

            socket: socket.clone(),
            recovery: Recovery::default(),
            #[cfg(feature = "per-connection-metrics")]
            metrics: crate::metrics::PerConnectionMetrics::new(socket.bind_addr(), remote),
        };

        METRICS.live_virtual_sockets.increment(1);

        let stream = UtpStream::new(read_half, write_half, vsock.remote);
        UtpStreamStarter {
            stream,
            vsock,
            cancellation_token,
        }
    }

    pub fn disarm(mut self) {
        self.vsock.drop_guard.disarm();
    }
}

#[derive(Debug, Clone, Copy)]
enum AckDelayTimer {
    Idle,
    Waiting(Instant),
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum RetransmitTimer {
    Idle,
    Retransmit {
        expires_at: Instant,
        delay: Duration,
    },
}

struct Timers {
    sleep: Pin<Box<Sleep>>,
    remote_inactivity_timer: Option<Instant>,
    recovery_pipe_expiry: Option<Instant>,
    retransmit: RetransmitTimer,
    ack_delay_timer: AckDelayTimer,
}

impl RetransmitTimer {
    fn new() -> RetransmitTimer {
        RetransmitTimer::Idle
    }

    fn expired(&self, now: Instant) -> bool {
        match *self {
            RetransmitTimer::Retransmit { expires_at, .. } if now >= expires_at => {
                trace!("should retransmit, timer expired");
                true
            }
            _ => false,
        }
    }

    fn poll_at(&self, now: Instant) -> PollAt {
        match *self {
            RetransmitTimer::Idle => PollAt::Ingress,
            RetransmitTimer::Retransmit { expires_at, .. } => {
                trace!(expires=?expires_at - now, "retransmit timer");
                PollAt::Time(expires_at)
            }
        }
    }

    fn turn_off(&mut self) {
        trace!("turning off retransmit timer");
        *self = RetransmitTimer::Idle
    }

    fn arm(&mut self, now: Instant, delay: Duration) {
        *self = match *self {
            // rfc6298 5.1
            RetransmitTimer::Idle => RetransmitTimer::Retransmit {
                expires_at: now + delay,
                delay,
            },
            // rfc6298 5.3
            RetransmitTimer::Retransmit { .. } => RetransmitTimer::Retransmit {
                expires_at: now + delay,
                delay,
            },
        };
    }
}

impl Timers {
    /// Schedule the timer to re-poll the dispatcher at provided deadline.
    ///
    /// Returns true if the waker was registered (i.e. it's ok to return Poll::Pending).
    fn arm_in(&mut self, cx: &mut std::task::Context<'_>, duration: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + duration;
        let mut sl = self.sleep.as_mut();
        sl.as_mut().reset(deadline);
        sl.as_mut().poll(cx) == Poll::Pending
    }

    fn reset_delayed_ack_timer(&mut self) {
        match self.ack_delay_timer {
            AckDelayTimer::Idle => {}
            AckDelayTimer::Waiting(_) => {
                trace!("stop delayed ack timer")
            }
        }
        self.ack_delay_timer = AckDelayTimer::Idle;
    }
}

// The main dispatch loop for the virtual socket is here.
impl<T: Transport, Env: UtpEnvironment> std::future::Future for VirtualSocket<T, Env> {
    type Output = anyhow::Result<()>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        self.get_mut().poll(cx)
    }
}

/// Gives an indication on the next time the socket should be polled.
#[derive(Debug, PartialOrd, Ord, PartialEq, Eq, Clone, Copy)]
pub(crate) enum PollAt {
    /// The socket needs to be polled at given [Instant][struct.Instant].
    Time(Instant),
    /// The socket does not need to be polled unless there are external changes.
    Ingress,
}
