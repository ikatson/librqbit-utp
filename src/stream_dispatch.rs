use std::{
    future::Future,
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Weak},
    task::Poll,
    time::{Duration, Instant},
};

use anyhow::{bail, Context};
use tokio::{sync::mpsc::UnboundedReceiver, time::Sleep};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error_span, trace, warn, Level};

use crate::{
    congestion::CongestionController,
    constants::{
        ACK_DELAY, CONGESTION_TRACING_LOG_LEVEL, RTTE_TRACING_LOG_LEVEL, SYNACK_RESEND_INTERNAL,
    },
    message::UtpMessage,
    raw::{Type, UtpHeader},
    rtte::RttEstimator,
    seq_nr::SeqNr,
    socket::{ControlRequest, ValidatedSocketOpts},
    spawn_utils::spawn_with_cancel,
    stream::UtpStream,
    stream_rx::{AssemblerAddRemoveResult, UserRx},
    stream_tx::{UserTx, UtpStreamWriteHalf},
    stream_tx_segments::{OnAckResult, Segments},
    traits::{Transport, UtpEnvironment},
    utils::{log_before_and_after_if_changed, update_optional_waker, DropGuardSendBeforeDeath},
    UtpSocket,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VirtualSocketState {
    SynReceived,
    SynAckSent { seq_nr: SeqNr, expires_at: Instant },

    Established,

    // We are fully done - both sides sent and acked FINs.
    Finished,

    // We sent FIN, not yet ACKed
    FinWait1 { our_fin: SeqNr },

    // Our fin was ACKed
    FinWait2,

    // We received a FIN, but we may still send data.
    CloseWait { remote_fin: SeqNr },

    // We and remote sent FINs, but none were ACKed
    Closing { our_fin: SeqNr, remote_fin: SeqNr },

    // Both sides closed, we are waiting for final ACK.
    // After this we just kill the socket.
    LastAck { our_fin: SeqNr, remote_fin: SeqNr },
}

impl VirtualSocketState {
    fn is_done(&self) -> bool {
        matches!(self, VirtualSocketState::Finished)
    }

    fn transition_to_fin_sent(&mut self, our_fin: SeqNr) -> bool {
        match *self {
            VirtualSocketState::Established => *self = VirtualSocketState::FinWait1 { our_fin },
            VirtualSocketState::CloseWait { remote_fin } => {
                *self = VirtualSocketState::LastAck {
                    our_fin,
                    remote_fin,
                }
            }
            _ => return false,
        };
        true
    }

    // True if we are not sending any more data.
    fn is_local_fin_or_later(&self) -> bool {
        match self {
            VirtualSocketState::SynReceived { .. }
            | VirtualSocketState::SynAckSent { .. }
            | VirtualSocketState::Established { .. }
            | VirtualSocketState::CloseWait { .. } => false,
            VirtualSocketState::Finished { .. }
            | VirtualSocketState::FinWait1 { .. }
            | VirtualSocketState::FinWait2 { .. }
            | VirtualSocketState::Closing { .. }
            | VirtualSocketState::LastAck { .. } => true,
        }
    }

    fn remote_fin(&self) -> Option<SeqNr> {
        match *self {
            VirtualSocketState::SynReceived { .. }
            | VirtualSocketState::SynAckSent { .. }
            | VirtualSocketState::Established { .. }
            | VirtualSocketState::Finished { .. }
            | VirtualSocketState::FinWait1 { .. }
            | VirtualSocketState::FinWait2 => None,
            VirtualSocketState::CloseWait { remote_fin }
            | VirtualSocketState::Closing { remote_fin, .. }
            | VirtualSocketState::LastAck { remote_fin, .. } => Some(remote_fin),
        }
    }

    fn our_fin_if_unacked(&self) -> Option<SeqNr> {
        match *self {
            VirtualSocketState::FinWait1 { our_fin }
            | VirtualSocketState::Closing { our_fin, .. }
            | VirtualSocketState::LastAck { our_fin, .. } => Some(our_fin),
            _ => None,
        }
    }

    fn on_incoming_packet(&mut self, hdr: &UtpHeader) -> anyhow::Result<()> {
        // Upgrade to ESTABLISHED if needed
        if let VirtualSocketState::SynAckSent { seq_nr, .. } = self {
            match hdr.get_type() {
                Type::ST_DATA | Type::ST_FIN | Type::ST_STATE if hdr.ack_nr == *seq_nr - 1 => {
                    *self = VirtualSocketState::Established;
                }
                Type::ST_RESET => bail!("reset received"),
                // Probably just a duplicate SYN, ignore it
                Type::ST_SYN => return Ok(()),
                _ => {
                    warn!(hdr=?hdr, our_syn_ack_seq_nr=?seq_nr, "unexpected packet in SynAckSent");
                    bail!("invalid packet received");
                }
            }
        }

        // Maybe ACK our fin
        match *self {
            VirtualSocketState::FinWait1 { our_fin } => {
                if hdr.ack_nr == our_fin {
                    *self = VirtualSocketState::FinWait2;
                }
            }
            VirtualSocketState::Closing { our_fin, .. } => {
                // For simplicity, we don't need to wait until the last side receives our FIN.
                // This would be smth like TimeWait, but it's not worth it.
                if hdr.ack_nr == our_fin {
                    *self = VirtualSocketState::Finished;
                    return Ok(());
                }
            }
            VirtualSocketState::LastAck { our_fin, .. } => {
                if our_fin == hdr.ack_nr {
                    *self = VirtualSocketState::Finished;
                    return Ok(());
                }
            }
            _ => {}
        }

        let remote_fin = match hdr.get_type() {
            Type::ST_FIN => hdr.seq_nr,
            _ => return Ok(()),
        };

        // Process remote FIN.
        match *self {
            VirtualSocketState::SynReceived { .. }
            | VirtualSocketState::SynAckSent { .. }
            | VirtualSocketState::Established { .. } => {
                *self = VirtualSocketState::CloseWait { remote_fin };
            }
            VirtualSocketState::Finished => {}
            VirtualSocketState::FinWait1 { our_fin } => {
                *self = VirtualSocketState::Closing {
                    our_fin,
                    remote_fin,
                };
            }
            VirtualSocketState::FinWait2 { .. } => {
                *self = VirtualSocketState::Finished;
            }
            VirtualSocketState::CloseWait { .. }
            | VirtualSocketState::Closing { .. }
            | VirtualSocketState::LastAck { .. } => {
                // fin retransmission, ignore
            }
        }
        Ok(())
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UserRxMessage {
    Payload(UtpMessage),
    Error(String),
    Eof,
}

impl UserRxMessage {
    pub fn len_bytes(&self) -> usize {
        match &self {
            UserRxMessage::Payload(buf) => buf.payload().len(),
            _ => 0,
        }
    }
}

// An equivalent of a TCP socket for uTP.
struct VirtualSocket<T, Env> {
    state: VirtualSocketState,
    socket: Weak<UtpSocket<T, Env>>,
    socket_created: Instant,
    socket_opts: ValidatedSocketOpts,

    remote: SocketAddr,
    conn_id_send: SeqNr,

    // Triggers delay-based operations
    timers: Timers,

    // The last seen value of uTP's "last_remote_timestamp"
    last_remote_timestamp: u32,
    /// The timestamp of the last packet received.
    last_remote_timestamp_instant: Instant,
    last_remote_window: u32,

    // The last seq_nr we told the other end about.
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

    // Last received ACK for fast retransmit
    local_rx_last_ack: Option<SeqNr>,
    local_rx_dup_acks: u8,

    // The user ppayload that we haven't yet segmented into uTP messages.
    // This is what "UtpStream::poll_write" writes to.
    user_tx: Arc<UserTx>,
    // Unacked segments. Ready to send or retransmit.
    user_tx_segments: Segments,

    rtte: RttEstimator,
    congestion_controller: Box<dyn CongestionController>,

    this_poll: ThisPoll,

    env: Env,

    drop_guard: DropGuardSendBeforeDeath<ControlRequest>,
    parent_span: Option<tracing::Span>,
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
        header.seq_nr = self.last_sent_seq_nr;
        header.ack_nr = self.last_consumed_remote_seq_nr;
        header.wnd_size = self.rx_window();
        header
    }

    // TODO: implement this better
    // https://datatracker.ietf.org/doc/html/rfc9293#section-3.8.6.2.2
    fn rx_window(&self) -> u32 {
        self.user_rx.remaining_rx_window() as u32
    }

    // Returns true if UDP socket is full
    fn send_tx_queue(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<()> {
        if self.user_tx_segments.is_empty() {
            return Ok(());
        }

        // No reason to send anything, we'll get polled next time.
        if self.this_poll.transport_pending {
            return Ok(());
        }

        let mut last_sent = None;
        self.congestion_controller.pre_transmit(self.this_poll.now);
        let mut recv_wnd = self.effective_remote_receive_window();

        let mut header = self.outgoing_header();

        // Send only the stuff we haven't sent yet, up to sender's window.
        for mut item in self.user_tx_segments.iter_mut() {
            // Re-delivery - don't send until retransmission happens (it will rewind elf.last_sent_seq_nr).
            let already_sent = item.seq_nr() - self.last_sent_seq_nr <= 0;
            if already_sent {
                recv_wnd = recv_wnd.saturating_sub(item.payload_size());
                continue;
            }

            // Selective ACK already marked this, ignore.
            if item.is_delivered() {
                continue;
            }

            if recv_wnd < item.payload_size() {
                debug!("remote recv window exhausted, not sending anything");
                break;
            }

            header.set_type(Type::ST_DATA);
            header.seq_nr = item.seq_nr();

            let len = header
                .serialize_with_payload(&mut self.this_poll.tmp_buf, |b| {
                    let offset = item.payload_offset();
                    let len = item.payload_size();
                    // TODO: use rwlock
                    let g = self.user_tx.locked.lock();
                    g.fill_buffer_from_ring_buffer(b, offset, len)
                        .context("error filling output buffer from user_tx")?;
                    Ok(len)
                })
                .context("bug: wasn't able to serialize the buffer")?;

            self.this_poll.transport_pending |=
                socket.try_poll_send_to(cx, &self.this_poll.tmp_buf[..len], self.remote)?;

            trace!(
                %header.seq_nr,
                %header.ack_nr,
                payload_size = len,
                socket_was_full = self.this_poll.transport_pending,
                "attempted to send ST_DATA"
            );

            if self.this_poll.transport_pending {
                break;
            }

            item.on_sent(self.this_poll.now);

            recv_wnd = recv_wnd.saturating_sub(item.payload_size());

            last_sent = Some(header);
        }

        trace!(recv_wnd, "remaining recv_wnd after sending");

        if let Some(header) = last_sent {
            self.on_packet_sent(&header);
            // rfc6298 5.1
            self.timers.retransmit.set_for_retransmit(
                self.this_poll.now,
                self.rtte.retransmission_timeout(),
                false,
            );
        } else {
            trace!(recv_wnd, "did not send anything");
        }

        Ok(())
    }

    fn maybe_send_ack(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<bool> {
        if self.immediate_ack_to_transmit() {
            return self.send_ack(cx, socket);
        }
        let expired = self.delayed_ack_expired();
        if expired && self.ack_to_transmit() {
            trace!("delayed ack expired, sending ACK");
            self.send_ack(cx, socket)
        } else {
            Ok(false)
        }
    }

    fn maybe_prepare_for_retransmission(&mut self) {
        if let Some(expired_delay) = self.timers.retransmit.should_retransmit(self.this_poll.now) {
            let rewind_to = self
                .user_tx_segments
                .first_seq_nr()
                .or_else(|| self.state.our_fin_if_unacked())
                .map(|rw| rw - 1);
            let rewind_to = match rewind_to {
                Some(v) => v,
                None => {
                    trace!("nowhere to rewind to, resetting retransmit timer");
                    self.timers.retransmit.set_for_idle();
                    return;
                }
            };

            // If a retransmit timer expired, we should resend data starting at the last ACK.
            log_every_ms!(100, CONGESTION_TRACING_LOG_LEVEL, ?expired_delay, ?rewind_to, ?self.last_sent_seq_nr, "retransmitting");

            // Rewind "last sequence number sent", as if we never
            // had sent them. This will cause all data in the queue
            // to be sent again.
            self.last_sent_seq_nr = rewind_to;

            // Clear the `should_retransmit` state. If we can't retransmit right
            // now for whatever reason (like zero window), this avoids an
            // infinite polling loop where `poll_at` returns `Now` but `dispatch`
            // can't actually do anything.
            //
            // Ideally, we only clear it after we were able to send anything.
            self.timers.retransmit.set_for_idle();

            // Inform RTTE to become more conservative.
            log_before_and_after_if_changed(
                "rtte:on_retransmit",
                &mut self.rtte,
                |r| r.retransmission_timeout(),
                |r| r.on_retransmit(),
                |_, _| CONGESTION_TRACING_LOG_LEVEL,
            );

            // Inform the congestion controller that we're retransmitting.
            self.congestion_controller.on_retransmit(self.this_poll.now);
        }
    }

    fn on_packet_sent(&mut self, header: &UtpHeader) {
        // Each packet sent can act as ACK so update related state.
        self.last_sent_seq_nr = header.seq_nr;
        self.last_sent_ack_nr = header.ack_nr;
        self.last_sent_window = header.wnd_size;
        self.consumed_but_unacked_bytes = 0;
        self.timers.reset_delayed_ack_timer();
    }

    fn send_control_packet(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
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
            socket.try_poll_send_to(cx, &self.this_poll.tmp_buf[..len], self.remote)?;

        let sent = !self.this_poll.transport_pending;

        if sent {
            self.on_packet_sent(&header);
        }

        Ok(sent)
    }

    fn send_ack(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<bool> {
        let mut header = self.outgoing_header();
        header.extensions.selective_ack = self.user_rx.selective_ack();
        self.send_control_packet(cx, socket, header)
    }

    fn maybe_send_fin(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<()> {
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
        if self.send_control_packet(cx, socket, fin)? {
            self.timers.retransmit.set_for_retransmit(
                self.this_poll.now,
                self.rtte.retransmission_timeout(),
                false,
            );
            self.last_sent_seq_nr = seq_nr;
        }
        Ok(())
    }

    fn effective_remote_receive_window(&self) -> usize {
        (self.last_remote_window as usize).min(self.congestion_controller.window())
    }

    fn split_tx_queue_into_segments(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> anyhow::Result<()> {
        let mut g = self.user_tx.locked.lock();

        if g.is_empty() {
            update_optional_waker(&mut g.buffer_has_data, cx);

            if g.is_closed() {
                let changed = log_before_and_after_if_changed(
                    "state",
                    &mut self.state,
                    |s| *s,
                    |s| s.transition_to_fin_sent(self.user_tx_segments.next_seq_nr()),
                    |_, _| Level::DEBUG,
                );
                if changed {
                    trace!("writer closed");
                }
            }

            return Ok(());
        }

        let tx_offset = self
            .user_tx_segments
            .iter_mut()
            .last()
            .map(|item| item.payload_offset() + item.payload_size())
            .unwrap_or(0);

        if g.len() < tx_offset {
            bail!(
                "bug in buffer computations: user_tx_buflen={} tx_offset={}",
                g.len(),
                tx_offset
            );
        }

        let mut remaining = g.len() - tx_offset;
        self.congestion_controller.pre_transmit(self.this_poll.now);
        let mut remote_window_remaining = self
            .effective_remote_receive_window()
            .saturating_sub(self.user_tx_segments.total_len_bytes());

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

        Ok(())
    }

    fn try_send_last_message_to_user_rx(&mut self, msg: UserRxMessage) {
        self.user_rx.enqueue_last_message(msg);
    }

    fn just_before_death(&mut self, error: Option<&anyhow::Error>) {
        // TODO: send ST_REST or FIN, or FIN ACK etc?

        if let Some(err) = error {
            trace!("just_before_death: {err:#}");
        } else {
            trace!("just_before_death: no error");
        }

        self.try_send_last_message_to_user_rx(
            error
                .map(|e| UserRxMessage::Error(format!("{e:#}")))
                .unwrap_or(UserRxMessage::Eof),
        );

        // This will close the reader.
        self.user_rx.mark_stream_dead();

        // This will close the writer.
        self.user_tx.locked.lock().mark_stream_dead();
    }

    fn process_all_incoming_messages(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<()> {
        let mut on_ack_result = OnAckResult::default();
        while let Poll::Ready(msg) = self.rx.poll_recv(cx) {
            let msg = match msg {
                Some(msg) => msg,
                None => {
                    // This could happen in the following scenarios:
                    // 1. The socket is dead. In this case any attempt to send() will result in poll() erroring out,
                    //    as the Weak<socket>.upgrade() will fail.
                    //    It's useless running dispatcher in this state, we can just quit.
                    //
                    // 2. We were removed from socket.streams().
                    //    The only case this could happen is both reader and writer are dropped.
                    //
                    //    We need to send FIN in this case, but we can't wait for its ACK to arrive back, cause we can't
                    //    receive any messages!

                    trace!("no more data. transitioning to FINISHED");
                    self.state
                        .transition_to_fin_sent(self.user_tx_segments.next_seq_nr());
                    self.maybe_send_fin(cx, socket)?;
                    self.state = VirtualSocketState::Finished;
                    return Ok(());
                }
            };
            on_ack_result.update(&self.process_incoming_message(cx, msg)?);
        }

        if on_ack_result.acked_segments_count > 0 {
            // Cleanup user side of TX queue, remove the ACKed bytes from the front of it,
            // and notify the writer.
            {
                let mut g = self.user_tx.locked.lock();
                g.truncate_front(on_ack_result.acked_bytes)?;
                if let Some(w) = g.buffer_has_space.take() {
                    w.wake();
                }

                if g.is_empty() {
                    if let Some(w) = g.buffer_flushed.take() {
                        w.wake();
                    }
                }
            }

            trace!(?on_ack_result, "removed ACKed tx messages");

            // Update RTO and congestion controller.
            if let Some(rtt) = on_ack_result.new_rtt {
                log_before_and_after_if_changed(
                    "rtte:sample",
                    self,
                    |s| s.rtte.retransmission_timeout(),
                    |s| s.rtte.sample(rtt),
                    |_, _| RTTE_TRACING_LOG_LEVEL,
                );
            }
            self.congestion_controller.on_ack(
                self.this_poll.now,
                on_ack_result.acked_bytes,
                &self.rtte,
            );

            // Reset retransmit timer.
            if self.user_tx_segments.is_empty() {
                // rfc6298 5.2
                self.timers.retransmit.set_for_idle();
            } else {
                // rfc6298 5.3
                self.timers.retransmit.set_for_retransmit(
                    self.this_poll.now,
                    self.rtte.retransmission_timeout(),
                    true,
                );
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "trace", name="msg", skip_all, fields(
        seq_nr=%msg.header.seq_nr,
        ack_nr=%msg.header.ack_nr,
        len=msg.payload().len(),
        type=?msg.header.get_type()
    ))]
    fn process_incoming_message(
        &mut self,
        cx: &mut std::task::Context<'_>,
        msg: UtpMessage,
    ) -> anyhow::Result<OnAckResult> {
        trace!("processing message");

        log_before_and_after_if_changed(
            "state",
            self,
            |s| s.state,
            |s| s.state.on_incoming_packet(&msg.header),
            |_, _| Level::DEBUG,
        )?;

        let on_ack_result = self
            .user_tx_segments
            .remove_up_to_ack(self.this_poll.now, &msg.header);

        self.last_remote_timestamp = msg.header.timestamp_microseconds;
        self.last_remote_timestamp_instant = self.this_poll.now;

        let is_window_update = self.last_remote_window != msg.header.wnd_size;
        self.last_remote_window = msg.header.wnd_size;
        self.congestion_controller
            .set_remote_window(msg.header.wnd_size as usize);

        match msg.header.get_type() {
            Type::ST_DATA => {
                trace!(payload_size = msg.payload().len(), "received ST_DATA");

                let msg_seq_nr = msg.header.seq_nr;

                let offset = msg_seq_nr - (self.last_consumed_remote_seq_nr + 1);
                if offset < 0 {
                    trace!(
                        %self.last_consumed_remote_seq_nr,
                        "dropping message, we already ACKed it"
                    );
                    return Ok(on_ack_result);
                }

                trace!(
                    offset,
                    %self.last_consumed_remote_seq_nr,
                    "adding ST_DATA message to assember"
                );

                let assembler_was_empty = self.user_rx.assembler_empty();

                match self
                    .user_rx
                    .add_remove(cx, msg, offset as usize)
                    .context("fatal error in assember")?
                {
                    AssemblerAddRemoveResult::Consumed {
                        sequence_numbers,
                        bytes,
                    } => {
                        if sequence_numbers > 0 {
                            trace!(sequence_numbers, "consumed messages");
                        } else {
                            trace!("out of order");
                        }

                        self.last_consumed_remote_seq_nr += sequence_numbers as u16;
                        self.consumed_but_unacked_bytes += bytes;
                        trace!(self.consumed_but_unacked_bytes);

                        // If we got remote FIN, increase the last consumed sequence number, so that
                        // we (re)send the final ACK.
                        if let Some(remote_fin) = self.state.remote_fin() {
                            if remote_fin - self.last_consumed_remote_seq_nr == 1 {
                                self.last_consumed_remote_seq_nr += 1;
                                self.try_send_last_message_to_user_rx(UserRxMessage::Eof);
                            }
                        }
                    }
                    AssemblerAddRemoveResult::Unavailable(_) => {
                        trace!("cannot reassemble message, ignoring it");
                    }
                }

                if self.ack_to_transmit() {
                    self.timers.ack_delay_timer = match self.timers.ack_delay_timer {
                        AckDelayTimer::Idle => {
                            trace!("starting delayed ack timer");
                            AckDelayTimer::Waiting(self.this_poll.now + ACK_DELAY)
                        }
                        timer @ AckDelayTimer::Waiting(_) => timer,
                    };
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
                    self.force_immedate_ack();
                }
            }
            Type::ST_STATE => {
                // Fast retransmit in case of duplicate ACKs
                match self.local_rx_last_ack {
                    // Duplicate ACK if payload empty and ACK doesn't move send window ->
                    // Increment duplicate ACK count and set for retransmit if we just received
                    // the third duplicate ACK
                    Some(last_rx_ack)
                        if last_rx_ack == msg.header.ack_nr
                            && self.last_sent_seq_nr > msg.header.ack_nr
                            && !is_window_update =>
                    {
                        // Increment duplicate ACK count
                        self.local_rx_dup_acks = self.local_rx_dup_acks.saturating_add(1);

                        // Inform congestion controller of duplicate ACK
                        self.congestion_controller
                            .on_duplicate_ack(self.this_poll.now);

                        debug!(
                            "received duplicate ACK for seq {} (duplicate nr {}{})",
                            msg.header.ack_nr,
                            self.local_rx_dup_acks,
                            if self.local_rx_dup_acks == u8::MAX {
                                "+"
                            } else {
                                ""
                            }
                        );

                        if self.local_rx_dup_acks == 3 {
                            self.timers.retransmit.set_for_fast_retransmit();
                            debug!("started fast retransmit");
                        }
                    }
                    // No duplicate ACK -> Reset state and update last received ACK
                    _ => {
                        if self.local_rx_dup_acks > 0 {
                            self.local_rx_dup_acks = 0;
                            trace!("reset duplicate ACK count");
                        }
                        self.local_rx_last_ack = Some(msg.header.ack_nr);
                    }
                };
            }
            Type::ST_RESET => bail!("ST_RESET received"),
            Type::ST_FIN => {
                if let Some(close_reason) = msg.header.extensions.close_reason {
                    debug!("remote closed with {close_reason:?}");
                }

                // This will (re)send FINACK
                if msg.header.seq_nr - self.last_consumed_remote_seq_nr == 1 {
                    self.last_consumed_remote_seq_nr += 1;
                    self.try_send_last_message_to_user_rx(UserRxMessage::Eof);
                }
            }
            Type::ST_SYN => {
                warn!("ignoring unexpected ST_SYN packet: {:?}", msg.header);
            }
        }
        Ok(on_ack_result)
    }

    fn should_send_window_update(&self) -> bool {
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
                >= 2 * self.socket_opts.max_incoming_payload_size.get()
    }

    fn force_immedate_ack(&mut self) {
        self.consumed_but_unacked_bytes = 2 * self.socket_opts.max_incoming_payload_size.get();
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

    // When do we need to send smth timer-based next time.
    fn next_poll_send_to_at(&self) -> PollAt {
        let want_ack = self.ack_to_transmit();

        let delayed_ack_poll_at = match (want_ack, self.timers.ack_delay_timer) {
            (false, _) => PollAt::Ingress,
            (true, AckDelayTimer::Idle) => PollAt::Ingress,
            (true, AckDelayTimer::Waiting(t)) => {
                trace!(expires_in=?t - self.this_poll.now, "delayed ACK timer");
                PollAt::Time(t)
            }
        };

        // We wait for the earliest of our timers to fire.
        self.timers
            .retransmit
            .poll_at(self.this_poll.now)
            .min(delayed_ack_poll_at)
    }

    fn user_rx_is_closed(&self) -> bool {
        self.user_rx.is_closed()
    }

    fn maybe_send_syn_ack(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<()> {
        let synack_seq_nr = match self.state {
            VirtualSocketState::SynReceived => self.user_tx_segments.next_seq_nr(),
            VirtualSocketState::SynAckSent { seq_nr, expires_at }
                if expires_at < self.this_poll.now =>
            {
                seq_nr
            }
            _ => return Ok(()),
        };
        let mut syn_ack = self.outgoing_header();
        let last_sent_seq_nr = self.last_sent_seq_nr;
        syn_ack.seq_nr = synack_seq_nr;
        if self.send_control_packet(cx, socket, syn_ack)? {
            self.state = VirtualSocketState::SynAckSent {
                seq_nr: synack_seq_nr,
                expires_at: self.this_poll.now + SYNACK_RESEND_INTERNAL,
            };
            // restore last_sent_seq_nr
            self.last_sent_seq_nr = last_sent_seq_nr;
            self.timers.arm_in(cx, SYNACK_RESEND_INTERNAL);
        }
        Ok(())
    }

    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> Poll<anyhow::Result<()>> {
        macro_rules! bail_if_err {
            ($e:expr) => {
                match $e {
                    Ok(val) => val,
                    Err(e) => {
                        self.just_before_death(Some(&e));
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
        let socket = bail_if_err!(self.socket.upgrade().context("device dead"));
        let socket = &*socket;

        self.this_poll.transport_pending = false;
        self.this_poll.now = self.env.now();

        const MAX_ITERS: usize = 2;

        for _ in 0..MAX_ITERS {
            pending_if_cannot_send!(self.maybe_send_syn_ack(cx, socket));

            // Flow control: flush as many out of order messages to user RX as possible.
            bail_if_err!(self.user_rx.flush(cx));

            // Read incoming stream.
            bail_if_err!(self.process_all_incoming_messages(cx, socket));

            self.maybe_prepare_for_retransmission();

            pending_if_cannot_send!(self.split_tx_queue_into_segments(cx));

            // (Re)send tx queue.
            pending_if_cannot_send!(self.send_tx_queue(cx, socket));

            pending_if_cannot_send!(self.maybe_send_fin(cx, socket));

            pending_if_cannot_send!(self.maybe_send_ack(cx, socket));

            if self.state.is_done() {
                self.just_before_death(None);
                return Poll::Ready(Ok(()));
            }

            if self.user_rx_is_closed() && self.state.is_local_fin_or_later() {
                // TODO: run a little bit more to send the final ACK to remote FIN?
                trace!(current_state=?self.state, "both halves are dead, no reason to continue");
                self.just_before_death(None);
                return Poll::Ready(Ok(()));
            }

            match self.next_poll_send_to_at() {
                PollAt::Now => {
                    return Poll::Ready(Err(anyhow::anyhow!(
                        "bug: encountered PollAt::now after running poll()",
                    )));
                }
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
        self.user_tx.mark_stream_dead();
        self.user_rx.mark_stream_dead();
    }
}

// See field descriptions / meanings in struct VirtualSocket
pub struct StreamArgs {
    conn_id_recv: SeqNr,
    conn_id_send: SeqNr,
    last_remote_timestamp: u32,

    next_seq_nr: SeqNr,
    last_sent_seq_nr: SeqNr,
    last_consumed_remote_seq_nr: SeqNr,
    last_sent_ack_nr: SeqNr,

    syn_sent_ts: Option<Instant>,
    ack_received_ts: Option<Instant>,
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
            next_seq_nr: remote_ack.ack_nr + 1,
            last_sent_seq_nr: remote_ack.ack_nr,

            // On connect, the client sends a number that represents the next ST_DATA number.
            // Pretend that we saw the non-existing previous one.
            last_sent_ack_nr: remote_ack.seq_nr - 1,
            last_consumed_remote_seq_nr: remote_ack.seq_nr - 1,

            // For RTTE
            syn_sent_ts: Some(syn_sent_ts),
            ack_received_ts: Some(ack_received_ts),

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

            next_seq_nr,
            // The connecting client will send the next ST_DATA packet with seq_nr + 1.
            last_consumed_remote_seq_nr: remote_syn.seq_nr,
            last_sent_seq_nr: next_seq_nr - 1,
            last_sent_ack_nr: remote_syn.seq_nr,

            // For RTTE
            syn_sent_ts: None,
            ack_received_ts: None,

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

        let span = match vsock.parent_span.clone() {
            Some(s) => error_span!(parent: s, "utp_stream", conn_id_send = ?vsock.conn_id_send),
            None => error_span!(parent: None, "utp_stream", conn_id_send = ?vsock.conn_id_send),
        };

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
            next_seq_nr,
            last_sent_seq_nr,
            last_consumed_remote_seq_nr,
            last_sent_ack_nr,
            syn_sent_ts,
            ack_received_ts,
            remote_window,
            parent_span,
            state,
        } = args;

        let (user_rx, read_half) = UserRx::build(
            socket.opts().max_user_rx_buffered_bytes,
            socket.opts().max_rx_out_of_order_packets,
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
            congestion_controller: socket
                .opts()
                .congestion
                .create(now, socket.opts().max_incoming_payload_size.get()),

            socket_created: socket.created,
            remote,
            conn_id_send,
            timers: Timers {
                retransmit: RetransmitTimer::new(),
                sleep: Box::pin(tokio::time::sleep(Duration::from_secs(0))),
                ack_delay_timer: AckDelayTimer::Idle,
            },
            last_remote_timestamp,
            last_remote_timestamp_instant: now,
            last_remote_window: remote_window,
            last_sent_seq_nr,
            last_consumed_remote_seq_nr,
            last_sent_ack_nr,
            consumed_but_unacked_bytes: 0,
            rx,
            user_tx_segments: Segments::new(next_seq_nr),
            local_rx_last_ack: None,
            local_rx_dup_acks: 0,
            user_tx,
            rtte: {
                let mut rtt = RttEstimator::default();
                if let (Some(sent), Some(recv)) = (syn_sent_ts, ack_received_ts) {
                    rtt.sample(recv - sent);
                }
                rtt
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

            socket: Arc::downgrade(socket),
        };

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
    FastRetransmit,
}

struct Timers {
    sleep: Pin<Box<Sleep>>,
    retransmit: RetransmitTimer,
    ack_delay_timer: AckDelayTimer,
}

impl RetransmitTimer {
    fn new() -> RetransmitTimer {
        RetransmitTimer::Idle
    }

    fn should_retransmit(&self, timestamp: Instant) -> Option<Duration> {
        match *self {
            RetransmitTimer::Retransmit { expires_at, delay } if timestamp >= expires_at => {
                trace!("should retransmit, timer expired");
                Some(delay)
            }
            RetransmitTimer::FastRetransmit => Some(Duration::from_secs(0)),
            _ => None,
        }
    }

    fn poll_at(&self, now: Instant) -> PollAt {
        match *self {
            RetransmitTimer::Idle => PollAt::Ingress,
            RetransmitTimer::Retransmit { expires_at, .. } => {
                trace!(expires=?expires_at - now, "retransmit timer");
                PollAt::Time(expires_at)
            }
            RetransmitTimer::FastRetransmit => {
                trace!("fast restransmit: now");
                PollAt::Now
            }
        }
    }

    fn set_for_idle(&mut self) {
        *self = RetransmitTimer::Idle
    }

    fn set_for_retransmit(&mut self, now: Instant, delay: Duration, restart: bool) {
        *self = match *self {
            // rfc6298 5.1
            RetransmitTimer::Idle => RetransmitTimer::Retransmit {
                expires_at: now + delay,
                delay,
            },
            // rfc6298 5.3
            RetransmitTimer::Retransmit { .. } if restart => RetransmitTimer::Retransmit {
                expires_at: now + delay,
                delay,
            },
            // If timer already present, leave as is, even if expired.
            RetransmitTimer::Retransmit { expires_at, delay } => {
                RetransmitTimer::Retransmit { expires_at, delay }
            }
            RetransmitTimer::FastRetransmit => RetransmitTimer::FastRetransmit,
        };
    }

    fn set_for_fast_retransmit(&mut self) {
        trace!("setting for fast retransmit");
        *self = RetransmitTimer::FastRetransmit
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
    /// The socket needs to be polled immediately.
    Now,
    /// The socket needs to be polled at given [Instant][struct.Instant].
    Time(Instant),
    /// The socket does not need to be polled unless there are external changes.
    Ingress,
}

#[cfg(test)]
mod tests {
    use std::{
        future::poll_fn, num::NonZeroUsize, pin::Pin, sync::Arc, task::Poll, time::Duration,
    };

    use futures::FutureExt;
    use tokio::{
        io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
        sync::mpsc::{unbounded_channel, UnboundedSender},
    };
    use tracing::trace;

    use crate::{
        constants::{IPV4_HEADER, MIN_UDP_HEADER, UTP_HEADER_SIZE},
        message::UtpMessage,
        raw::{selective_ack::SelectiveAck, Type, UtpHeader},
        seq_nr::SeqNr,
        stream_dispatch::VirtualSocketState,
        test_util::{
            env::MockUtpEnvironment, setup_test_logging, transport::RememberingTransport, ADDR_1,
            ADDR_2,
        },
        traits::UtpEnvironment,
        SocketOpts, UtpSocket,
    };

    use super::{StreamArgs, UtpStream, UtpStreamStarter, VirtualSocket};

    fn make_msg(header: UtpHeader, payload: &str) -> UtpMessage {
        UtpMessage::new_test(header, payload.as_bytes())
    }

    struct TestVsock {
        transport: RememberingTransport,
        env: MockUtpEnvironment,
        _socket: Arc<UtpSocket<RememberingTransport, MockUtpEnvironment>>,
        vsock: VirtualSocket<RememberingTransport, MockUtpEnvironment>,
        stream: Option<UtpStream>,
        tx: UnboundedSender<UtpMessage>,
    }

    impl TestVsock {
        fn send_msg(&mut self, header: UtpHeader, payload: &str) {
            self.tx.send(make_msg(header, payload)).unwrap()
        }

        fn send_data(&mut self, seq_nr: impl Into<SeqNr>, ack_nr: impl Into<SeqNr>, payload: &str) {
            let header = UtpHeader {
                htype: Type::ST_DATA,
                connection_id: self.vsock.conn_id_send + 1,
                timestamp_microseconds: self.vsock.timestamp_microseconds(),
                timestamp_difference_microseconds: 0,
                wnd_size: 1024 * 1024,
                seq_nr: seq_nr.into(),
                ack_nr: ack_nr.into(),
                extensions: Default::default(),
            };
            self.send_msg(header, payload);
        }

        fn take_sent(&self) -> Vec<UtpMessage> {
            self.transport.take_sent_utpmessages()
        }

        async fn read_all_available(&mut self) -> std::io::Result<Vec<u8>> {
            self.stream.as_mut().unwrap().read_all_available().await
        }

        async fn process_all_available_incoming(&mut self) {
            std::future::poll_fn(|cx| {
                if self.vsock.rx.is_empty() {
                    return Poll::Ready(());
                }
                self.vsock.poll_unpin(cx).map(|r| r.unwrap())
            })
            .await
        }

        async fn poll_once(&mut self) -> Poll<anyhow::Result<()>> {
            std::future::poll_fn(|cx| {
                let res = self.vsock.poll_unpin(cx);
                Poll::Ready(res)
            })
            .await
        }

        async fn poll_once_assert_pending(&mut self) {
            let res = self.poll_once().await;
            match res {
                Poll::Pending => {}
                Poll::Ready(res) => {
                    res.unwrap();
                    panic!("unexpected finish");
                }
            };
        }
    }

    fn make_test_vsock(opts: SocketOpts, is_incoming: bool) -> TestVsock {
        let transport = RememberingTransport::new(ADDR_1);
        let env = MockUtpEnvironment::new();
        let socket = UtpSocket::new_with_opts(transport.clone(), env.clone(), opts).unwrap();
        let (tx, rx) = unbounded_channel();

        let args = if is_incoming {
            let remote_syn = UtpHeader {
                htype: Type::ST_SYN,
                ..Default::default()
            };
            StreamArgs::new_incoming(100.into(), &remote_syn)
        } else {
            let remote_ack = UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 1.into(),
                ack_nr: 100.into(),
                wnd_size: 1024 * 1024,
                ..Default::default()
            };
            StreamArgs::new_outgoing(&remote_ack, env.now(), env.now())
        };

        let UtpStreamStarter { stream, vsock, .. } =
            UtpStreamStarter::new(&socket, ADDR_2, rx, args);

        TestVsock {
            transport,
            env,
            _socket: socket,
            vsock,
            stream: Some(stream),
            tx,
        }
    }

    #[tokio::test]
    async fn test_delayed_ack_sent_once() {
        setup_test_logging();

        let mut t = make_test_vsock(Default::default(), false);
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);

        t.send_msg(
            UtpHeader {
                htype: Type::ST_DATA,
                seq_nr: 1.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                ..Default::default()
            },
            "hello",
        );
        t.poll_once_assert_pending().await;
        assert_eq!(&t.read_all_available().await.unwrap(), b"hello");
        assert_eq!(t.take_sent().len(), 0);

        // Pretend it's 1 second later.
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;
        assert_eq!(&t.read_all_available().await.unwrap(), b"");

        // Assert an ACK was sent.
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_STATE);
        assert_eq!(sent[0].header.ack_nr.0, 1);

        // Assert nothing else is sent later.
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 0);
    }

    #[tokio::test]
    async fn test_doesnt_send_until_window_updated() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), true);
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 1); // intial SYN_ACK
        assert_eq!(t.vsock.last_remote_window, 0);

        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"hello")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);

        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "hello",
        );
        t.poll_once_assert_pending().await;

        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].header.ack_nr.0, 0);
        assert_eq!(sent[0].payload(), b"hello");
    }

    #[tokio::test]
    async fn test_sends_up_to_remote_window_only_single_msg() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), true);
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 1); // syn ack
        assert_eq!(t.vsock.last_remote_window, 0);

        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"hello")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);

        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 4,
                ..Default::default()
            },
            "hello",
        );
        t.poll_once_assert_pending().await;

        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].header.ack_nr.0, 0);
        assert_eq!(sent[0].payload(), b"hell");

        // Until window updates and/or we receive an ACK, we don't send anything
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);
    }

    #[tokio::test]
    async fn test_sends_up_to_remote_window_only_multi_msg() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), true);
        t.vsock.socket_opts.max_outgoing_payload_size = NonZeroUsize::new(2).unwrap();
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 1); // syn ack
        assert_eq!(t.vsock.last_remote_window, 0);

        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"hello world")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);

        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                // This is enough to send "hello" in 3 messages
                wnd_size: 5,
                ..Default::default()
            },
            "hello",
        );
        t.poll_once_assert_pending().await;

        let sent = t.take_sent();
        assert_eq!(sent.len(), 3, "{sent:#?}");
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].payload(), b"he");
        assert_eq!(sent[1].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[1].payload(), b"ll");
        assert_eq!(sent[2].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[2].payload(), b"o");

        t.poll_once_assert_pending().await;

        let sent = t.take_sent();
        assert_eq!(sent.len(), 0);
    }

    #[tokio::test]
    async fn test_basic_retransmission() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // Write some data
        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"hello")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;

        // First transmission should happen
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].payload(), b"hello");
        let original_seq_nr = sent[0].header.seq_nr;

        // Wait for retransmission timeout
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;

        // Should retransmit the same data
        let resent = t.take_sent();
        assert_eq!(resent.len(), 1, "Should have retransmitted");
        assert_eq!(resent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(resent[0].payload(), b"hello");
        assert_eq!(
            resent[0].header.seq_nr, original_seq_nr,
            "Retransmitted packet should have same sequence number"
        );

        // Until time goes on, nothing should happen.
        t.poll_once_assert_pending().await;
        let resent = t.take_sent();
        assert_eq!(resent.len(), 0, "Should not have retransmitted");

        // Wait again for 2nd retransmission.
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;
        let resent = t.take_sent();
        assert_eq!(resent.len(), 1, "Should have retransmitted");
        assert_eq!(resent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(resent[0].payload(), b"hello");
        assert_eq!(
            resent[0].header.seq_nr, original_seq_nr,
            "Retransmitted packet should have same sequence number"
        );
    }

    #[tokio::test]
    async fn test_fast_retransmit() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);
        t.vsock.socket_opts.max_outgoing_payload_size = NonZeroUsize::new(5).unwrap();

        // Set a large retransmission timeout so we know fast retransmit is triggering, not RTO
        t.vsock.rtte.force_timeout(Duration::from_secs(10));

        let mut ack = UtpHeader {
            htype: Type::ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 1024,
            ..Default::default()
        };

        // Allow sending by setting window size
        t.send_msg(ack, "");

        // Write enough data to generate multiple packets
        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"helloworld")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;

        // Should have sent the data
        let sent = t.take_sent();
        assert!(sent.len() >= 2, "Should have sent multiple packets");
        assert_eq!(sent[0].payload(), b"hello");
        assert_eq!(sent[1].payload(), b"world");

        let first_seq_nr = sent[0].header.seq_nr;

        // Simulate receiving duplicate ACKs (as if first packet was lost but later ones arrived)
        ack.ack_nr = first_seq_nr;

        // First ACK
        t.send_msg(ack, "");

        // First duplicate ACK
        t.send_msg(ack, "");

        // Second duplicate ACK
        t.send_msg(ack, "");

        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.local_rx_dup_acks, 2);
        assert_eq!(t.take_sent().len(), 0, "Should not retransmit yet");

        // Third duplicate ACK should trigger fast retransmit
        t.send_msg(ack, "");

        t.poll_once_assert_pending().await;
        trace!("s");

        // Should have retransmitted the first packet immediately
        let resent = t.take_sent();
        assert_eq!(resent.len(), 1, "Should have retransmitted second packet");
        assert_eq!(
            resent[0].header.seq_nr,
            first_seq_nr + 1,
            "Should have retransmitted first packet"
        );
        assert_eq!(
            resent[0].payload(),
            b"world",
            "Should have retransmitted correct data"
        );
    }

    #[tokio::test]
    async fn test_fin_shutdown_sequence_initiated_by_explicit_shutdown() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        let mut stream = t.stream.take().unwrap();

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Write some data and initiate shutdown
        stream.write_all(b"hello").await.unwrap();

        // Ensure it's processed and sent.
        t.poll_once_assert_pending().await;

        // Should have sent the data
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].payload(), b"hello");
        let data_seq_nr = sent[0].header.seq_nr;

        // Acknowledge the data
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: data_seq_nr,
                ..Default::default()
            },
            "",
        );
        // Deliver the ACK.
        t.poll_once_assert_pending().await;

        // Initiate shutdown. It must complete immediately as all the data should have been flushed.
        stream.shutdown().await.unwrap();

        // Ensure we get error on calling "write" again.
        assert!(stream.write(b"test").await.is_err());

        t.poll_once_assert_pending().await;
        // Should send FIN after data is acknowledged
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_FIN);
        let fin_seq_nr = sent[0].header.seq_nr;
        assert_eq!(
            fin_seq_nr.0,
            data_seq_nr.0 + 1,
            "FIN should use next sequence number after data"
        );

        // Remote acknowledges our FIN
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: fin_seq_nr,
                ..Default::default()
            },
            "",
        );

        // Remote sends its FIN
        t.send_msg(
            UtpHeader {
                htype: Type::ST_FIN,
                seq_nr: 1.into(),
                ack_nr: fin_seq_nr,
                ..Default::default()
            },
            "",
        );
        let result = t.poll_once().await;

        // We should acknowledge remote's FIN
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_STATE);
        assert_eq!(sent[0].header.ack_nr.0, 1);
        assert!(
            matches!(result, Poll::Ready(Ok(()))),
            "Connection should complete cleanly"
        );
    }

    #[tokio::test]
    async fn test_fin_sent_when_writer_dropped() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        let (_reader, mut writer) = t.stream.take().unwrap().split();

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Write some data and initiate shutdown
        writer.write_all(b"hello").await.unwrap();

        // Ensure it's processed and sent.
        t.poll_once_assert_pending().await;

        // Should have sent the data
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].payload(), b"hello");
        let data_seq_nr = sent[0].header.seq_nr;

        // Acknowledge the data
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: data_seq_nr,
                ..Default::default()
            },
            "",
        );
        // Deliver the ACK.
        t.poll_once_assert_pending().await;

        // Initiate FIN.
        drop(writer);

        t.poll_once_assert_pending().await;
        // Should send FIN after data is acknowledged
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_FIN);
        let fin_seq_nr = sent[0].header.seq_nr;
        assert_eq!(
            fin_seq_nr.0,
            data_seq_nr.0 + 1,
            "FIN should use next sequence number after data"
        );
    }

    #[tokio::test]
    async fn test_flush_works() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        let (_reader, writer) = t.stream.take().unwrap().split();
        let mut writer = tokio::io::BufWriter::new(writer);

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Write some data and initiate shutdown
        writer.write_all(b"hello").await.unwrap();

        // Ensure nothing gets sent.
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);

        // Ensure flush blocks at first
        let flush_result = poll_fn(|cx| {
            let res = Pin::new(&mut writer).poll_flush(cx);
            Poll::Ready(res)
        })
        .await;
        assert!(flush_result.is_pending());

        // Now we send the data.
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].payload(), b"hello");
        let data_seq_nr = sent[0].header.seq_nr;

        // Ensure flush is still blocked until the data is ACKed.
        let flush_result = poll_fn(|cx| {
            let res = Pin::new(&mut writer).poll_flush(cx);
            Poll::Ready(res)
        })
        .await;
        assert!(flush_result.is_pending());

        // Acknowledge the data
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: data_seq_nr,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // Ensure flush completes at first
        let flush_result = poll_fn(|cx| {
            let res = Pin::new(&mut writer).poll_flush(cx);
            Poll::Ready(res)
        })
        .await;
        match flush_result {
            Poll::Ready(result) => result.unwrap(),
            Poll::Pending => panic!("flush should have completed"),
        };
    }

    #[tokio::test]
    async fn test_out_of_order_delivery() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // First allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Send packets out of order. Sequence should be:
        // seq 1: "hello"
        // seq 2: "world"
        // seq 3: "test!"

        // Send seq 2 first
        t.send_msg(
            UtpHeader {
                htype: Type::ST_DATA,
                seq_nr: 2.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                ..Default::default()
            },
            "world",
        );
        t.poll_once_assert_pending().await;

        // Nothing should be readable yet as we're missing seq 1
        assert_eq!(&t.read_all_available().await.unwrap(), b"");

        // We should get an immediate ACK due to out-of-order delivery
        let acks = t.take_sent();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].header.get_type(), Type::ST_STATE);
        assert_eq!(acks[0].header.ack_nr, 0.into());

        // Send seq 3
        t.send_msg(
            UtpHeader {
                htype: Type::ST_DATA,
                seq_nr: 3.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                ..Default::default()
            },
            "test!",
        );
        t.poll_once_assert_pending().await;

        // Still nothing readable
        assert_eq!(&t.read_all_available().await.unwrap(), b"");

        // Another immediate ACK due to out-of-order
        let acks = t.take_sent();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].header.get_type(), Type::ST_STATE);
        assert_eq!(acks[0].header.ack_nr, 0.into());

        // Finally send seq 1
        t.send_msg(
            UtpHeader {
                htype: Type::ST_DATA,
                seq_nr: 1.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                ..Default::default()
            },
            "hello",
        );
        t.poll_once_assert_pending().await;

        // Now we should get all the data in correct order
        assert_eq!(&t.read_all_available().await.unwrap(), b"helloworldtest!");

        // And a final ACK for the in-order delivery
        let acks = t.take_sent();
        assert_eq!(acks.len(), 1);
        assert_eq!(acks[0].header.get_type(), Type::ST_STATE);
        assert_eq!(acks[0].header.ack_nr.0, 3); // Should acknowledge up to last packet
    }

    #[tokio::test]
    async fn test_nagle_algorithm() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // Enable Nagle (should be on by default, but let's be explicit)
        t.vsock.socket_opts.nagle = true;
        // Set a large max payload size to ensure we're testing Nagle, not segmentation
        t.vsock.socket_opts.max_outgoing_payload_size = NonZeroUsize::new(1024).unwrap();

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Write a small amount of data
        t.stream.as_mut().unwrap().write_all(b"a").await.unwrap();
        t.poll_once_assert_pending().await;

        // First small write should be sent immediately
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b"a");
        let first_seq_nr = sent[0].header.seq_nr;

        // Write another small chunk - should not be sent while first is unacked
        t.stream.as_mut().unwrap().write_all(b"b").await.unwrap();
        t.poll_once_assert_pending().await;
        assert_eq!(
            t.take_sent().len(),
            0,
            "Nagle should prevent sending small chunk while data is in flight"
        );

        // Write more data - still should not send
        t.stream.as_mut().unwrap().write_all(b"c").await.unwrap();
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);

        // As debugging, ensure we did not even segment the new packets yet.
        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
        assert_eq!(t.vsock.user_tx_segments.total_len_bytes(), 1);
        assert_eq!(
            t.vsock.user_tx_segments.first_seq_nr().unwrap(),
            first_seq_nr
        );

        trace!("Acknowledge first packet");
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: first_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
        assert_eq!(t.vsock.user_tx_segments.total_len_bytes(), 2);

        // After ACK, buffered data should be sent as one packet
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1, "Buffered data should be sent as one packet");
        assert_eq!(
            sent[0].payload(),
            b"bc",
            "Buffered data should be coalesced"
        );

        // Now disable Nagle
        t.vsock.socket_opts.nagle = false;

        // Small writes should be sent immediately
        t.stream.as_mut().unwrap().write_all(b"d").await.unwrap();
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b"d");

        // Next small write should also go immediately, even without ACK
        t.stream.as_mut().unwrap().write_all(b"e").await.unwrap();
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b"e");
    }

    #[tokio::test]
    async fn test_resource_cleanup_both_sides_dropped_normally() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        let (reader, mut writer) = t.stream.take().unwrap().split();

        // Write some data
        writer.write_all(b"hello").await.unwrap();
        t.poll_once_assert_pending().await;

        // Data should be sent
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        let seq_nr = sent[0].header.seq_nr;

        // Drop the writer - this should trigger sending FIN.
        drop(writer);
        t.poll_once_assert_pending().await;

        // Nothing should happen until it's ACKed.
        assert_eq!(t.take_sent().len(), 0);
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Should see the FIN
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_FIN);
        let fin_nr = sent[0].header.seq_nr;

        assert_eq!(
            t.vsock.state,
            VirtualSocketState::FinWait1 { our_fin: fin_nr }
        );

        // Drop the reader - this should cause the stream to complete without waiting for FIN ACK
        drop(reader);

        // Poll should complete immediately even though we never got ACKs
        let result = t.poll_once().await;
        assert!(
            matches!(result, Poll::Ready(Ok(()))),
            "Poll should complete after both halves are dropped"
        );
    }

    #[tokio::test]
    async fn test_resource_cleanup_both_sides_dropped_abruptly() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        drop(t.stream.take());
        let result = t.poll_once().await;
        assert!(
            matches!(result, Poll::Ready(Ok(()))),
            "Poll should complete after both halves are dropped"
        );
    }

    #[tokio::test]
    async fn test_resource_cleanup_with_pending_data() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        let (reader, mut writer) = t.stream.take().unwrap().split();

        // Write data but don't wait for it to be sent
        writer.write_all(b"hello").await.unwrap();

        // Drop both handles immediately
        drop(writer);
        drop(reader);

        // First poll should send the pending data
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b"hello");
        let data_seq_nr = sent[0].header.seq_nr;

        // Nothing else whould be sent before ACK.
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);

        // Send ACK
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Next poll should send FIN and die.
        let result = t.poll_once().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_FIN);
        assert_eq!(sent[0].header.seq_nr.0, data_seq_nr.0 + 1);
        assert!(matches!(result, Poll::Ready(Ok(()))));
    }

    #[tokio::test]
    async fn test_sender_flow_control() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // Set initial remote window very small
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 5, // Only allow 5 bytes
                ..Default::default()
            },
            "",
        );

        // Try to write more data than window allows
        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"hello world")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;

        // Should only send up to window size
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b"hello");
        let first_seq_nr = sent[0].header.seq_nr;

        // No more data should be sent until window updates
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0);

        // ACK first packet but keep window small
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: first_seq_nr,
                wnd_size: 4, // Reduce window further
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // Should send up to new window size
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b" wor"); // Only 4 bytes
        let second_seq_nr = sent[0].header.seq_nr;

        // Remote increases window and ACKs
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: second_seq_nr,
                wnd_size: 10, // Open window
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // Should send remaining data
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b"ld");

        // Verify total data sent matches original write
        assert_eq!(
            t.vsock.user_tx_segments.next_seq_nr().0 - first_seq_nr.0,
            3, // 3 packets total
            "Should have split data into correct number of packets"
        );
    }

    #[tokio::test]
    async fn test_zero_window_handling() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // Set initial window to allow some data
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 5,
                ..Default::default()
            },
            "",
        );

        // Write data
        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"hello world")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;

        // First chunk should be sent
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b"hello");
        let seq_nr = sent[0].header.seq_nr;

        // ACK data but advertise zero window
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: seq_nr,
                wnd_size: 0, // Zero window
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // Nothing should be sent
        assert_eq!(t.take_sent().len(), 0);

        // Remote sends window update
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: seq_nr,
                wnd_size: 1024, // Open window
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // Remaining data should now be sent
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].payload(), b" world");
    }

    #[tokio::test]
    async fn test_congestion_control_basics() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        let remote_wnd = 64 * 1024;

        // Allow sending by setting large window
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: remote_wnd, // Large window to not interfere with congestion control
                ..Default::default()
            },
            "",
        );
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;

        let initial_window = t.vsock.congestion_controller.window();
        assert!(
            initial_window < remote_wnd as usize,
            "{initial_window} >= {remote_wnd}, should be less"
        );
        trace!(initial_window, remote_wnd);

        // Write a lot of data to test windowing
        let big_data = vec![0u8; 64 * 1024];
        t.stream
            .as_mut()
            .unwrap()
            .write_all(&big_data)
            .await
            .unwrap();

        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;

        assert_eq!(
            initial_window,
            t.vsock.congestion_controller.window(),
            "window shouldn't have changed"
        );

        // Should only send up to initial window
        let sent = t.take_sent();
        let sent_bytes: usize = sent.iter().map(|m| m.payload().len()).sum::<usize>();
        assert!(
            sent_bytes <= initial_window,
            "Should respect initial window. sent_bytes={sent_bytes}, initial_window={initial_window}"
        );
        let first_batch_seq_nrs = sent.iter().map(|m| m.header.seq_nr).collect::<Vec<_>>();

        // ACK all packets - window should increase
        for seq_nr in &first_batch_seq_nrs {
            t.send_msg(
                UtpHeader {
                    htype: Type::ST_STATE,
                    seq_nr: 0.into(),
                    ack_nr: *seq_nr,
                    wnd_size: remote_wnd,
                    ..Default::default()
                },
                "",
            );
        }
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;

        let intermediate_window = t.vsock.congestion_controller.window();
        trace!(intermediate_window);

        assert!(
            intermediate_window > initial_window,
            "Window should grow after ACKs"
        );

        let sent = t.take_sent();
        assert!(
            !sent.is_empty(),
            "Should send more data due to increased window"
        );

        // Now simulate packet loss via duplicate ACKs
        let lost_seq_nr = *first_batch_seq_nrs.last().unwrap();
        for _ in 0..4 {
            t.send_msg(
                UtpHeader {
                    htype: Type::ST_STATE,
                    seq_nr: 0.into(),
                    ack_nr: lost_seq_nr,
                    wnd_size: remote_wnd,
                    ..Default::default()
                },
                "",
            );
        }
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;

        // Should trigger fast retransmit
        let resent = t.take_sent();
        assert!(
            !resent.is_empty(),
            "Should retransmit on triple duplicate ACK"
        );

        // Window should be reduced
        let window_after_loss = t.vsock.congestion_controller.window();
        trace!(window_after_loss);
        assert!(
            window_after_loss < intermediate_window,
            "Window should decrease after loss: intermediate_window={intermediate_window} window_after_loss={window_after_loss}"
        );

        // Simulate timeout by advancing time
        t.env.increment_now(Duration::from_secs(10));
        t.poll_once_assert_pending().await;

        // Should retransmit and reduce window further
        let resent = t.take_sent();
        assert!(!resent.is_empty(), "Should retransmit on timeout");

        let window_after_timeout = t.vsock.congestion_controller.window();
        assert!(
            window_after_timeout <= window_after_loss,
            "Window should decrease or stay same after timeout"
        );
    }

    #[tokio::test]
    async fn test_duplicate_ack_only_on_st_state() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);
        t.vsock.socket_opts.max_outgoing_payload_size = NonZeroUsize::new(5).unwrap();

        // Set a large retransmission timeout so we know fast retransmit is triggering, not RTO
        t.vsock.rtte.force_timeout(Duration::from_secs(10));

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Write enough data to generate multiple packets
        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"helloworld")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;

        // Should have sent the data
        let sent = t.take_sent();
        assert!(sent.len() == 2, "Should have sent 2 packets");
        assert_eq!(sent[0].payload(), b"hello");
        assert_eq!(sent[1].payload(), b"world");
        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 2);

        let first_seq_nr = sent[0].header.seq_nr;

        // Send three duplicate ACKs using different packet types
        let mut header = UtpHeader {
            htype: Type::ST_STATE,
            seq_nr: 0.into(),
            ack_nr: first_seq_nr,
            wnd_size: 1024,
            ..Default::default()
        };

        // First normal ST_STATE ACK
        t.send_msg(header, "");
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.local_rx_dup_acks, 0);
        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);

        // Change to ST_DATA with same ACK - shouldn't count as duplicate
        header.htype = Type::ST_DATA;
        header.seq_nr += 1;
        t.send_msg(header, "a");
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.local_rx_dup_acks, 0);
        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);

        // Another ST_DATA - shouldn't count
        header.seq_nr += 1;
        t.send_msg(header, "b");
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.local_rx_dup_acks, 0);
        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);

        // ST_FIN with same ACK - shouldn't count
        header.htype = Type::ST_FIN;
        header.seq_nr += 1;
        t.send_msg(header, "");

        t.poll_once_assert_pending().await;

        // Duplicate ACK count should be 0 since non-ST_STATE packets don't count
        assert_eq!(t.vsock.local_rx_dup_acks, 0);
        assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
        let sent = t.take_sent();
        assert_eq!(
            sent.len(),
            0,
            "Should not have sent anything, but sent this: {sent:?}"
        );

        // Now send three ST_STATE duplicates
        header.htype = Type::ST_STATE;
        t.send_msg(header, "");
        t.send_msg(header, "");
        t.send_msg(header, "");

        t.poll_once_assert_pending().await;

        // Now we should see duplicate ACKs counted and fast retransmit triggered
        assert_eq!(t.vsock.local_rx_dup_acks, 3);
        let resent = t.take_sent();
        assert_eq!(resent.len(), 1, "Should have triggered fast retransmit");
        assert_eq!(
            resent[0].header.seq_nr,
            first_seq_nr + 1,
            "Should have retransmitted correct packet"
        );
    }

    #[tokio::test]
    async fn test_finack_not_sent_until_all_data_consumed() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);
        t.vsock.socket_opts.max_outgoing_payload_size = NonZeroUsize::new(5).unwrap();

        // Set a large retransmission timeout so we know fast retransmit is triggering, not RTO
        t.vsock.rtte.force_timeout(Duration::from_secs(10));

        // Send an out of order message
        let mut header = UtpHeader {
            htype: Type::ST_DATA,
            seq_nr: 2.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 1024,
            ..Default::default()
        };

        header.seq_nr = 2.into();
        t.send_msg(header, "world");
        t.poll_once_assert_pending().await;
        assert!(!t.vsock.user_rx.assembler_empty());
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1, "immediate ACK should have been sent");
        assert_eq!(sent[0].header.ack_nr, 0.into());

        // remote sends FIN
        header.set_type(Type::ST_FIN);
        header.seq_nr = 3.into();
        t.send_msg(header, "");
        t.poll_once_assert_pending().await;
        assert!(!t.vsock.user_rx.assembler_empty());
        assert_eq!(
            t.take_sent(),
            vec![],
            "nothing gets sent for out of order FIN"
        );

        // send in-order. This should ACK the FIN
        header.set_type(Type::ST_DATA);
        header.seq_nr = 1.into();
        t.send_msg(header, "a");
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1, "we should sent FIN ACK");
        assert_eq!(sent[0].header.ack_nr, 3.into());
    }

    #[tokio::test]
    async fn test_flow_control() {
        setup_test_logging();

        // Configure socket with small buffers to test flow control
        let opts = SocketOpts {
            mtu: Some(5 + UTP_HEADER_SIZE + MIN_UDP_HEADER + IPV4_HEADER),
            rx_bufsize: Some(25), // Small receive buffer (~5 packets of size 5)
            max_rx_out_of_order_packets: Some(5), // Small assembly queue
            ..Default::default()
        };

        let mut t = make_test_vsock(opts, true);
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 1); // syn ack

        // Test assembly queue limit first
        // Send packets out of order to fill assembly queue
        t.send_data(3, t.vsock.last_sent_seq_nr, "third"); // Out of order
        t.send_data(4, t.vsock.last_sent_seq_nr, "fourth"); // Out of order
        t.send_data(6, t.vsock.last_sent_seq_nr, "sixth"); // Should be dropped - assembly queue full

        t.poll_once_assert_pending().await;

        // Only two packets should be in assembly queue
        assert_eq!(t.vsock.user_rx.assembler_packets(), 2);

        // Send in-order packet to trigger processing
        t.send_data(1, t.vsock.last_sent_seq_nr, "first");
        t.poll_once_assert_pending().await;

        // Read available data
        let read = t.read_all_available().await.unwrap();
        assert_eq!(&read, b"first"); // Only first packet should be read

        // Now test user rx channel limit
        // Send several packets that exceed the rx buffer size
        for i in 6..15 {
            t.send_data(i, t.vsock.last_sent_seq_nr, "data!");
            t.poll_once_assert_pending().await;
        }

        // Read available data - should only get packets that fit in rx buffer
        let read = t.read_all_available().await.unwrap();
        assert!(read.len() < 50); // Should be limited by rx_bufsize_approx

        // Verify window size advertised matches available buffer space
        let sent = t.take_sent();
        assert!(!sent.is_empty());
        let window = sent.last().unwrap().header.wnd_size;
        assert!(window < 1024); // Window should be reduced

        // Send more data - should be dropped due to full rx buffer
        t.send_data(15, t.vsock.last_sent_seq_nr, "dropped");
        t.poll_once_assert_pending().await;

        // Verify data was dropped
        let read = String::from_utf8(t.read_all_available().await.unwrap()).unwrap();
        assert!(!read.contains("dropped"));
    }

    #[tokio::test]
    async fn test_data_integrity_manual_packets() {
        setup_test_logging();

        const DATA_SIZE: usize = 1024 * 1024;
        const CHUNK_SIZE: usize = 1024;

        let mut t = make_test_vsock(
            SocketOpts {
                rx_bufsize: Some(DATA_SIZE),
                ..Default::default()
            },
            false,
        );

        let mut test_data = Vec::with_capacity(DATA_SIZE);

        for char in std::iter::repeat(b'a'..=b'z').flatten().take(DATA_SIZE) {
            test_data.push(char);
        }

        // Send data in chunks
        let chunks = test_data.chunks(CHUNK_SIZE);
        let mut header = UtpHeader {
            htype: Type::ST_DATA,
            seq_nr: 0.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 64 * 1024,
            ..Default::default()
        };
        for chunk in chunks {
            header.seq_nr += 1;
            trace!(?header.seq_nr, "sending");
            t.send_msg(header, std::str::from_utf8(chunk).unwrap());
        }

        // Process all messages
        t.process_all_available_incoming().await;
        assert!(t.vsock.user_rx.assembler_empty());

        // Read all data
        let received_data = t.read_all_available().await.unwrap();
        assert_eq!(t.vsock.user_rx.len_test(), 0);

        // Verify data integrity
        assert_eq!(
            received_data.len(),
            DATA_SIZE,
            "Received data size mismatch: got {} bytes, expected {}",
            received_data.len(),
            DATA_SIZE
        );
        assert_eq!(received_data, test_data, "Data corruption detected");
    }

    #[tokio::test]
    async fn test_retransmission_behavior() {
        setup_test_logging();
        let mut t = make_test_vsock(
            SocketOpts {
                ..Default::default()
            },
            false,
        );

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Write some test data
        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"hello world")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;

        // Initial send
        let initial_sent = t.take_sent();
        assert_eq!(initial_sent.len(), 1, "Should send data initially");
        assert_eq!(initial_sent[0].payload(), b"hello world");
        let initial_seq_nr = initial_sent[0].header.seq_nr;

        // No retransmission should occur before timeout
        t.env.increment_now(Duration::from_millis(100));
        t.poll_once_assert_pending().await;
        assert_eq!(
            t.take_sent().len(),
            0,
            "Should not retransmit before timeout"
        );

        // After timeout, packet should be retransmitted
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;
        let first_retransmit = t.take_sent();
        assert_eq!(first_retransmit.len(), 1, "Should retransmit after timeout");
        assert_eq!(
            first_retransmit[0].header.seq_nr, initial_seq_nr,
            "Retransmitted packet should have same sequence number"
        );
        assert_eq!(
            first_retransmit[0].payload(),
            b"hello world",
            "Retransmitted packet should have same payload"
        );

        // Second timeout should trigger another retransmission with doubled timeout
        t.env.increment_now(Duration::from_secs(2));
        t.poll_once_assert_pending().await;
        let second_retransmit = t.take_sent();
        assert_eq!(
            second_retransmit.len(),
            1,
            "Should retransmit after second timeout"
        );
        assert_eq!(
            second_retransmit[0].header.seq_nr, initial_seq_nr,
            "Second retransmit should have same sequence number"
        );

        // Now ACK the packet - should stop retransmissions
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: initial_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // No more retransmissions should occur
        t.env.increment_now(Duration::from_secs(4));
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 0, "Should not retransmit after ACK");

        // Write new data - should use next sequence number
        t.stream.as_mut().unwrap().write_all(b"test").await.unwrap();
        t.poll_once_assert_pending().await;
        let new_sent = t.take_sent();
        assert_eq!(new_sent.len(), 1, "Should send new data");
        assert_eq!(
            new_sent[0].header.seq_nr.0,
            initial_seq_nr.0 + 1,
            "New packet should use next sequence number"
        );
    }

    #[tokio::test]
    async fn test_selective_ack_retransmission() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);
        t.vsock.socket_opts.max_outgoing_payload_size = NonZeroUsize::new(5).unwrap();

        const FORCED_RETRANSMISSION_TIME: Duration = Duration::from_secs(1);
        t.vsock.rtte.force_timeout(FORCED_RETRANSMISSION_TIME);

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        // Write enough data to generate multiple packets
        t.stream
            .as_mut()
            .unwrap()
            .write_all(b"helloworld")
            .await
            .unwrap();
        t.poll_once_assert_pending().await;

        // Should have sent two packets
        let initial_sent = t.take_sent();
        assert_eq!(initial_sent.len(), 2, "Should have sent 2 packets");
        assert_eq!(initial_sent[0].payload(), b"hello");
        assert_eq!(initial_sent[1].payload(), b"world");
        let first_seq_nr = initial_sent[0].header.seq_nr;

        // Send selective ACK indicating second packet was received but first wasn't
        let header = UtpHeader {
            htype: Type::ST_STATE,
            seq_nr: 0.into(),
            ack_nr: first_seq_nr - 1, // ACK previous packet
            wnd_size: 1024,
            extensions: crate::raw::Extensions {
                selective_ack: SelectiveAck::new_test([0]),
                ..Default::default()
            },
            ..Default::default()
        };
        t.send_msg(header, "");

        // After receiving selective ACK and waiting for retransmit
        t.env.increment_now(FORCED_RETRANSMISSION_TIME);
        t.poll_once_assert_pending().await;

        // Should only retransmit the first packet
        let retransmitted = t.take_sent();
        assert_eq!(
            retransmitted.len(),
            1,
            "Should only retransmit first packet"
        );
        assert_eq!(
            retransmitted[0].header.seq_nr, first_seq_nr,
            "Should retransmit first packet"
        );
        assert_eq!(
            retransmitted[0].payload(),
            b"hello",
            "Should retransmit correct data"
        );

        // Send normal ACK for first packet
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: first_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // No more retransmissions should occur
        t.env.increment_now(Duration::from_secs(1));
        t.poll_once_assert_pending().await;
        assert_eq!(
            t.take_sent().len(),
            0,
            "Should not retransmit after both packets acknowledged"
        );
    }

    #[tokio::test]
    async fn test_st_reset_error_propagation() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        let (mut reader, _writer) = t.stream.take().unwrap().split();

        // Send a RESET packet
        t.send_msg(
            UtpHeader {
                htype: Type::ST_RESET,
                seq_nr: 1.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                ..Default::default()
            },
            "",
        );

        // Process the RESET packet
        let _ = t.poll_once().await;

        // Try to read - should get an error containing "ST_RESET"
        let read_result = reader.read(&mut [0u8; 1024]).await;
        assert!(
            read_result.is_err(),
            "Read should fail after receiving RESET"
        );
        let err = read_result.unwrap_err();
        assert!(
            err.to_string().contains("ST_RESET"),
            "Error should mention ST_RESET: {err}"
        );
    }

    #[tokio::test]
    async fn test_fin_sent_when_both_halves_dropped() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        // Allow sending by setting window size
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: t.vsock.last_sent_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );

        let (reader, mut writer) = t.stream.take().unwrap().split();

        // Write some data
        writer.write_all(b"hello").await.unwrap();
        t.poll_once_assert_pending().await;

        // Data should be sent
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].payload(), b"hello");
        let data_seq_nr = sent[0].header.seq_nr;

        // Acknowledge the data
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: data_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // Drop both halves - this should trigger sending FIN
        drop(reader);
        drop(writer);

        // Next poll should send FIN
        let result = t.poll_once().await;
        assert!(
            matches!(result, Poll::Ready(Ok(()))),
            "Socket should complete immediately after both halves dropped"
        );

        let sent = t.take_sent();
        assert_eq!(sent.len(), 1, "Should send FIN after both halves dropped");
        assert_eq!(sent[0].header.get_type(), Type::ST_FIN);
        assert_eq!(
            sent[0].header.seq_nr.0,
            data_seq_nr.0 + 1,
            "FIN should use next sequence number"
        );
    }

    #[tokio::test]
    async fn test_fin_sent_when_reader_dead_first() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default(), false);

        let (reader, mut writer) = t.stream.take().unwrap().split();

        // Drop the reader first
        drop(reader);

        // Write some data - should still work
        writer.write_all(b"hello").await.unwrap();
        t.poll_once_assert_pending().await;

        // Data should be sent
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.get_type(), Type::ST_DATA);
        assert_eq!(sent[0].payload(), b"hello");
        let data_seq_nr = sent[0].header.seq_nr;

        // Acknowledge the data
        t.send_msg(
            UtpHeader {
                htype: Type::ST_STATE,
                seq_nr: 0.into(),
                ack_nr: data_seq_nr,
                wnd_size: 1024,
                ..Default::default()
            },
            "",
        );
        t.poll_once_assert_pending().await;

        // Remote sends some data - it should accumulate in OOQ.
        t.send_data(1, t.vsock.last_sent_seq_nr, "ignored data");
        t.poll_once_assert_pending().await;

        // Now drop the writer
        drop(writer);

        // Next poll should send FIN and complete
        let result = t.poll_once().await;
        assert!(
            matches!(result, Poll::Ready(Ok(()))),
            "Socket should complete immediately after both halves dropped"
        );

        let sent = t.take_sent();
        assert_eq!(sent.len(), 1, "Should send FIN after writer dropped");
        assert_eq!(sent[0].header.get_type(), Type::ST_FIN);
        assert_eq!(
            sent[0].header.seq_nr.0,
            data_seq_nr.0 + 1,
            "FIN should use next sequence number"
        );
    }

    #[tokio::test]
    async fn test_window_update_sent_when_window_less_than_mss() {
        setup_test_logging();

        // Configure socket with small but non-zero receive buffer
        let mss = 5;
        let opts = SocketOpts {
            rx_bufsize: Some(mss * 2),
            mtu: Some(mss + UTP_HEADER_SIZE + MIN_UDP_HEADER + IPV4_HEADER),
            ..Default::default()
        };

        let mut t = make_test_vsock(opts, false);
        assert_eq!(t.vsock.socket_opts.max_incoming_payload_size.get(), mss);

        // Fill buffer to just under MSS to get a small window
        t.send_data(1, t.vsock.last_sent_seq_nr, "aaaaa");
        t.poll_once_assert_pending().await;
        // We shouldn't have registered the flush waker yet.
        assert!(!t.vsock.user_rx.is_flush_waker_registered());
        assert_eq!(t.take_sent(), vec![]);

        // Now the window should be less than MSS.
        t.send_data(2, t.vsock.last_sent_seq_nr, "b");
        t.poll_once_assert_pending().await;
        assert!(t.vsock.user_rx.is_flush_waker_registered());
        // Verify window is now less than MSS
        let sent = t.take_sent();
        assert!(!sent.is_empty(), "Should have sent ACKs");
        let last_window = sent.last().unwrap().header.wnd_size;
        assert_eq!(last_window, 4);

        // Read some data to free up more than MSS worth of buffer space
        let mut buf = vec![0u8; mss];
        t.stream
            .as_mut()
            .unwrap()
            .read_exact(&mut buf)
            .await
            .unwrap();
        t.poll_once_assert_pending().await;

        // Should send window update ACK
        let sent = t.take_sent();
        assert_eq!(sent.len(), 1, "Should send window update ACK");
        assert_eq!(sent[0].header.get_type(), Type::ST_STATE);
        assert_eq!(sent[0].header.wnd_size, 9);
    }

    #[tokio::test]
    async fn test_window_update_ack_after_read_with_waking() {
        setup_test_logging();

        // Configure socket with very small receive buffer to test flow control
        let mss = 5;
        let opts = SocketOpts {
            rx_bufsize: Some(mss * 2),
            mtu: Some(mss + UTP_HEADER_SIZE + MIN_UDP_HEADER + IPV4_HEADER),
            ..Default::default()
        };

        let mut t = make_test_vsock(opts, true);
        t.poll_once_assert_pending().await;
        assert_eq!(t.take_sent().len(), 1); // syn ack

        let ack_nr = t.vsock.last_sent_seq_nr;
        let connection_id = t.vsock.conn_id_send + 1;
        let make_msg = |seq_nr, payload| {
            make_msg(
                UtpHeader {
                    htype: Type::ST_DATA,
                    connection_id,
                    wnd_size: 1024 * 1024,
                    seq_nr,
                    ack_nr,
                    ..Default::default()
                },
                payload,
            )
        };

        tokio::spawn(t.vsock);

        // Fill up the receive buffer
        t.tx.send(make_msg(1.into(), "aaaaa")).unwrap();
        t.tx.send(make_msg(2.into(), "bbbbb")).unwrap();

        tokio::task::yield_now().await;

        // Ensure we send back 0 window
        let sent = t.transport.take_sent_utpmessages();
        assert!(!sent.is_empty(), "Should have sent ACKs");
        let last_window = sent.last().unwrap().header.wnd_size;
        assert_eq!(last_window, 0, "Window should be zero when buffer full");

        // Now read some data to free up buffer space
        let mut buf = vec![0u8; 5];
        t.stream
            .as_mut()
            .unwrap()
            .read_exact(&mut buf)
            .await
            .unwrap();

        // Reading should wake up the vsock. On yield, it should get polled.
        tokio::task::yield_now().await;

        // Should send window update ACK
        let sent = t.transport.take_sent_utpmessages();
        assert_eq!(sent.len(), 1, "Should send window update ACK");
        assert_eq!(sent[0].header.get_type(), Type::ST_STATE);
        assert!(
            sent[0].header.wnd_size > 0,
            "Window size should be non-zero after reading"
        );
    }
}
