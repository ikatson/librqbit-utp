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
use tracing::{debug, error_span, trace, warn};

use crate::{
    congestion::CongestionController,
    constants::{ACK_DELAY, CHALLENGE_ACK_RATELIMIT, IMMEDIATE_ACK_EVERY, UTP_HEADER_SIZE},
    message::UtpMessage,
    raw::{Type, UtpHeader},
    rtte::RttEstimator,
    seq_nr::SeqNr,
    socket::{ControlRequest, ValidatedSocketOpts},
    stream::UtpStream,
    stream_rx::{AssemblerAddRemoveResult, UserRx},
    stream_tx::{FragmentedTx, UserTx, UtpStreamWriteHalf},
    traits::{Transport, UtpEnvironment},
    utils::{
        log_before_and_after_if_changed, spawn_print_error, update_optional_waker,
        DropGuardSendBeforeDeath,
    },
    UtpSocket,
};

// This contains more states than Rust could model with its enums, but I'm keeping
// the names for 1:1 TCP mapping.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum VirtualSocketState {
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
            VirtualSocketState::Established | VirtualSocketState::CloseWait { .. } => false,
            VirtualSocketState::Finished
            | VirtualSocketState::FinWait1 { .. }
            | VirtualSocketState::FinWait2
            | VirtualSocketState::Closing { .. }
            | VirtualSocketState::LastAck { .. } => true,
        }
    }

    fn remote_fin(&self) -> Option<SeqNr> {
        match *self {
            VirtualSocketState::Established
            | VirtualSocketState::Finished
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

    fn on_incoming_packet(&mut self, remote_fin_seq_nr: Option<SeqNr>, remote_ack_nr: SeqNr) {
        // To decrease combinations, first process remote_ack_nr, then process "is_fin"

        // Maybe ACK our fin
        match *self {
            VirtualSocketState::FinWait1 { our_fin } => {
                if remote_ack_nr == our_fin {
                    *self = VirtualSocketState::FinWait2;
                }
            }
            VirtualSocketState::Closing { our_fin, .. } => {
                // For simplicity, we don't need to wait until the last side receives our FIN.
                // This would be smth like TimeWait, but it's not worth it.
                if remote_ack_nr == our_fin {
                    *self = VirtualSocketState::Finished;
                    return;
                }
            }
            VirtualSocketState::LastAck { our_fin, .. } => {
                if our_fin == remote_ack_nr {
                    *self = VirtualSocketState::Finished;
                    return;
                }
            }
            _ => {}
        }

        let remote_fin = match remote_fin_seq_nr {
            Some(n) => n,
            None => return,
        };

        // Process remote FIN.
        match *self {
            VirtualSocketState::Established => {
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
    }
}

#[derive(Debug, PartialEq, Eq)]
pub(crate) enum UserRxMessage {
    Payload(UtpMessage),
    Error(String),
    Eof,
}

impl UserRxMessage {
    #[cfg(test)]
    pub fn len_bytes_test(&self) -> usize {
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

    // Next sequence number to use when fragmenting user input.
    next_seq_nr: SeqNr,

    // The last seq_nr we told the other end about.
    last_sent_seq_nr: SeqNr,

    // Last remote sequence number that we fully processed.
    last_consumed_remote_seq_nr: SeqNr,

    // Last ACK that we sent out. This is different from "ack_nr" because we don't ACK
    // every packet. This must be <= ack_nr.
    last_sent_ack_nr: SeqNr,

    // Incoming queue. The main UDP socket writes here, and we need to consume these
    // as fast as possible.
    //
    // TODO: make bounded
    rx: UnboundedReceiver<UtpMessage>,

    // Unacked fragments. Ready to send or retransmit.
    tx: FragmentedTx,

    user_rx: UserRx,

    // Last received ACK for fast retransmit
    local_rx_last_ack: Option<SeqNr>,
    local_rx_dup_acks: u8,

    // The user ppayload that we haven't yet fragmented into uTP messages.
    // This is what "UtpStream::poll_write" writes to.
    user_tx: Arc<UserTx>,
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
    async fn send_packet(&self, packet: UtpHeader) -> anyhow::Result<()> {
        let mut buf = [0u8; UTP_HEADER_SIZE];
        packet
            .serialize(&mut buf)
            .context("bug: can't serialize header")?;
        self.socket
            .upgrade()
            .context("socket dead")?
            .transport
            .send_to(&buf, self.remote)
            .await
            .context("error sending")?;
        Ok(())
    }

    async fn run_forever(self, first_packet: Option<UtpHeader>) -> anyhow::Result<()> {
        if let Some(packet) = first_packet {
            self.send_packet(packet).await?;
        }

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

    /// Prepare a previously created header for resend.
    fn update_header(&self, header: &UtpHeader) -> UtpHeader {
        let mut header = *header;

        header.timestamp_microseconds = self.timestamp_microseconds();
        header.timestamp_difference_microseconds = header
            .timestamp_microseconds
            .wrapping_sub(self.last_remote_timestamp);
        header.ack_nr = self.last_consumed_remote_seq_nr;
        header.wnd_size = self.rx_window();
        header
    }

    fn rx_window(&self) -> u32 {
        self.user_rx.window() as u32
    }

    // Returns true if UDP socket is full
    fn send_tx_queue(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<()> {
        if self.tx.is_empty() {
            return Ok(());
        }

        // No reason to send anything, we'll get polled next time.
        if self.this_poll.transport_pending {
            return Ok(());
        }

        let mut last_sent = None;
        self.congestion_controller.pre_transmit(self.this_poll.now);
        let mut recv_wnd = self.effective_remote_receive_window();

        // Send only the stuff we haven't sent yet, up to sender's window.
        for item in self.tx.iter() {
            let already_sent = item.header().seq_nr - self.last_sent_seq_nr <= 0;
            if already_sent {
                recv_wnd = recv_wnd.saturating_sub(item.payload_size());
                continue;
            }

            if recv_wnd < item.payload_size() {
                debug!("remote recv window exhausted, not sending anything");
                break;
            }

            let header = self.update_header(item.header());

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

            recv_wnd = recv_wnd.saturating_sub(item.payload_size());

            last_sent = Some(header);
        }

        if recv_wnd > 0 {
            trace!(recv_wnd, "remaining recv_wnd after sending");
        }

        if let Some(header) = last_sent {
            self.on_packet_sent(&header);
            self.timers
                .kind
                .set_for_retransmit(self.this_poll.now, self.rtte.retransmission_timeout());
        } else {
            trace!(recv_wnd, "did not send anything");
        }

        Ok(())
    }

    fn maybe_send_delayed_ack(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<bool> {
        if self.delayed_ack_expired() && self.ack_to_transmit() {
            trace!("delayed ack expired, sending ACK");
            self.send_ack(cx, socket)
        } else {
            Ok(false)
        }
    }

    fn maybe_prepare_for_retransmission(&mut self) {
        if let Some(retransmit_delta) = self.timers.kind.should_retransmit(self.this_poll.now) {
            let rewind_to = self
                .tx
                .first_seq_nr()
                .or_else(|| self.state.our_fin_if_unacked())
                .map(|rw| rw - 1);
            let rewind_to = match rewind_to {
                Some(v) => v,
                None => return,
            };

            // If a retransmit timer expired, we should resend data starting at the last ACK.
            trace!("retransmitting at t+{:?}", retransmit_delta);

            // Rewind "last sequence number sent", as if we never
            // had sent them. This will cause all data in the queue
            // to be sent again.
            self.last_sent_seq_nr = rewind_to;

            // Clear the `should_retransmit` state. If we can't retransmit right
            // now for whatever reason (like zero window), this avoids an
            // infinite polling loop where `poll_at` returns `Now` but `dispatch`
            // can't actually do anything.
            self.timers.kind.set_for_idle();

            // Inform RTTE, so that it can avoid bogus measurements.
            self.rtte.on_retransmit();

            // Inform the congestion controller that we're retransmitting.
            self.congestion_controller.on_retransmit(self.this_poll.now);
        }
    }

    fn on_packet_sent(&mut self, header: &UtpHeader) {
        // Each packet sent can act as ACK so update related state.
        self.last_sent_seq_nr = header.seq_nr;
        self.last_sent_ack_nr = header.ack_nr;
        self.timers.reset_delayed_ack_timer();
        self.rtte.on_send(self.this_poll.now, header.seq_nr);
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

    fn send_challenge_ack(
        &mut self,
        cx: &mut std::task::Context<'_>,
        socket: &UtpSocket<T, Env>,
    ) -> anyhow::Result<bool> {
        if self.this_poll.now < self.timers.challenge_ack_timer {
            return Ok(false);
        }

        // Rate-limit to 1 per second max.
        if self.send_ack(cx, socket)? {
            trace!("sending challenge ACK");
            self.timers.challenge_ack_timer = self.this_poll.now + CHALLENGE_ACK_RATELIMIT;
            Ok(true)
        } else {
            Ok(false)
        }
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
            self.timers
                .kind
                .set_for_retransmit(self.this_poll.now, self.rtte.retransmission_timeout());
            self.last_sent_seq_nr = seq_nr;
        }
        Ok(())
    }

    fn effective_remote_receive_window(&self) -> usize {
        (self.last_remote_window as usize).min(self.congestion_controller.window())
    }

    fn fragment_tx_queue(&mut self, cx: &mut std::task::Context<'_>) -> anyhow::Result<()> {
        let mut g = self.user_tx.locked.lock();

        if g.buffer().is_empty() {
            update_optional_waker(&mut g.buffer_has_data, cx);

            if g.is_closed() {
                let changed = log_before_and_after_if_changed(
                    "state",
                    &mut self.state,
                    |s| *s,
                    |s| s.transition_to_fin_sent(self.next_seq_nr),
                );
                if changed {
                    trace!("writer closed");
                }
            }

            return Ok(());
        }

        let tx_offset = self
            .tx
            .iter()
            .last()
            .map(|item| item.payload_offset() + item.payload_size())
            .unwrap_or(0);

        if g.buffer().len() < tx_offset {
            bail!(
                "bug in buffer computations: user_tx_buflen={} tx_offset={}",
                g.buffer().len(),
                tx_offset
            );
        }

        let mut remaining = g.buffer().len() - tx_offset;
        self.congestion_controller.pre_transmit(self.this_poll.now);
        let mut remote_window_remaining = self
            .effective_remote_receive_window()
            .saturating_sub(self.tx.total_len_bytes());

        while !self.tx.is_full() && remaining > 0 && remote_window_remaining > 0 {
            let max_payload_size = self
                .socket_opts
                .max_outgoing_payload_size
                .get()
                .min(remote_window_remaining);
            let payload_size = max_payload_size.min(remaining);

            // Run Nagle algorithm to prevent sending too many small segments.
            {
                let can_send_full_payload = payload_size == max_payload_size;
                let data_in_flight = !self.tx.is_empty();

                if self.socket_opts.nagle && !can_send_full_payload && data_in_flight {
                    trace!(payload_size, max_payload_size, "nagle: buffering more data");
                    break;
                }
            }

            let mut header = self.outgoing_header();
            header.set_type(Type::ST_DATA);
            header.seq_nr = self.next_seq_nr;

            if !self.tx.enqueue(header, payload_size) {
                bail!("bug, tx full, but we checked for it above")
            }
            remaining -= payload_size;
            remote_window_remaining -= payload_size;
            trace!(bytes = payload_size, "fragmented");
            self.next_seq_nr += 1;
        }

        Ok(())
    }

    fn try_send_last_message_to_user_rx(&mut self, msg: UserRxMessage) {
        self.user_rx.enqueue_last_message(msg);
    }

    fn just_before_death(&mut self, error: Option<&anyhow::Error>) {
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
                    self.state.transition_to_fin_sent(self.next_seq_nr);
                    self.maybe_send_fin(cx, socket)?;
                    self.state = VirtualSocketState::Finished;
                    return Ok(());
                }
            };
            self.process_incoming_message(cx, socket, msg)?;
        }
        trace!(rxlen = self.rx.len());
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
        socket: &UtpSocket<T, Env>,
        msg: UtpMessage,
    ) -> anyhow::Result<()> {
        trace!("processing message");

        log_before_and_after_if_changed(
            "state",
            self,
            |s| s.state,
            |s| {
                s.state.on_incoming_packet(
                    if matches!(msg.header.get_type(), Type::ST_FIN) {
                        Some(msg.header.seq_nr)
                    } else {
                        None
                    },
                    msg.header.ack_nr,
                )
            },
        );

        // Remove everything from tx_buffer that was acked by this message.
        let (removed_headers, removed_bytes) = self.tx.remove_up_to_ack(msg.header.ack_nr);
        if removed_headers > 0 {
            let mut g = self.user_tx.locked.lock();
            let was_full = g.is_full();
            g.truncate_front(removed_bytes);
            if was_full {
                if let Some(w) = g.buffer_no_longer_full.take() {
                    w.wake();
                }
            }

            if g.buffer().is_empty() {
                if let Some(w) = g.buffer_flushed.take() {
                    w.wake();
                }
            }

            trace!(removed_headers, removed_bytes, "removed ACKed tx messages");
        }

        self.last_remote_timestamp = msg.header.timestamp_microseconds;
        self.last_remote_timestamp_instant = self.this_poll.now;

        let is_window_update = self.last_remote_window != msg.header.wnd_size;
        self.last_remote_window = msg.header.wnd_size;
        self.congestion_controller
            .set_remote_window(msg.header.wnd_size as usize);

        self.rtte.on_ack(self.this_poll.now, msg.header.ack_nr);
        if removed_bytes > 0 {
            self.congestion_controller
                .on_ack(self.this_poll.now, removed_bytes, &self.rtte);
        }

        let ack_all = msg.header.ack_nr == self.last_sent_ack_nr;
        if !self.timers.kind.is_retransmit() || ack_all {
            self.timers.kind.set_for_idle();
        }

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
                    self.send_challenge_ack(cx, socket)?;
                    return Ok(());
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
                    AssemblerAddRemoveResult::ConsumedSequenceNumbers(count) => {
                        if count > 0 {
                            trace!(count, "acked messages, we have them stored in RX buffers");
                        } else {
                            trace!("out of order");
                        }

                        self.last_consumed_remote_seq_nr += count as u16;

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

                // Handle delayed acks
                if self.ack_to_transmit() {
                    self.timers.ack_delay_timer = match self.timers.ack_delay_timer {
                        AckDelayTimer::Idle => {
                            trace!("starting delayed ack timer");
                            AckDelayTimer::Waiting(self.this_poll.now + ACK_DELAY)
                        }
                        AckDelayTimer::Waiting(_) if self.immediate_ack_to_transmit() => {
                            trace!("delayed ack timer already started, forcing expiry");
                            AckDelayTimer::Immediate
                        }
                        timer @ AckDelayTimer::Waiting(_) => {
                            trace!("waiting until delayed ack timer expires");
                            timer
                        }
                        AckDelayTimer::Immediate => {
                            trace!("delayed ack timer already force-expired");
                            AckDelayTimer::Immediate
                        }
                    };
                }

                // Per RFC 5681, we should send an immediate ACK when either:
                //  1) an out-of-order segment is received, or
                //  2) a segment arrives that fills in all or part of a gap in sequence space.
                if !self.user_rx.assembler_empty()
                    || !assembler_was_empty
                    || self.immediate_ack_to_transmit()
                {
                    trace!(
                        assembler_not_empty = !self.user_rx.assembler_empty(),
                        assembler_was_empty,
                        immediate_ack_to_transmit = self.immediate_ack_to_transmit(),
                        "sending immediate ACK"
                    );
                    self.send_ack(cx, socket)?;
                }
                Ok(())
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
                            self.timers.kind.set_for_fast_retransmit();
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
                Ok(())
            }
            Type::ST_RESET => bail!("ST_RESET received"),
            Type::ST_FIN => {
                // This will (re)send FINACK
                if msg.header.seq_nr - self.last_consumed_remote_seq_nr == 1 {
                    self.last_consumed_remote_seq_nr += 1;
                    self.try_send_last_message_to_user_rx(UserRxMessage::Eof);
                }
                Ok(())
            }
            Type::ST_SYN => {
                warn!("ignoring unexpected ST_SYN packet: {:?}", msg.header);
                Ok(())
            }
        }
    }

    /// TODO: implement this better
    ///
    /// Return whether to send ACK immediately due to the amount of unacknowledged data.
    ///
    /// RFC 9293 states "An ACK SHOULD be generated for at least every second full-sized segment or
    /// 2*RMSS bytes of new data (where RMSS is the MSS specified by the TCP endpoint receiving the
    /// segments to be acknowledged, or the default value if not specified) (SHLD-19)."
    ///
    /// Note that the RFC above only says "at least 2*RMSS bytes", which is not a hard requirement.
    /// In practice, we follow the Linux kernel's empirical value of sending an ACK for every RMSS
    /// byte of new data. For details, see
    /// <https://elixir.bootlin.com/linux/v6.11.4/source/net/ipv4/tcp_input.c#L5747>.
    fn immediate_ack_to_transmit(&self) -> bool {
        self.last_consumed_remote_seq_nr - self.last_sent_ack_nr >= IMMEDIATE_ACK_EVERY
    }

    fn ack_to_transmit(&self) -> bool {
        self.last_consumed_remote_seq_nr > self.last_sent_ack_nr
    }

    fn delayed_ack_expired(&self) -> bool {
        match self.timers.ack_delay_timer {
            AckDelayTimer::Idle => true,
            AckDelayTimer::Waiting(t) => t <= self.this_poll.now,
            AckDelayTimer::Immediate => true,
        }
    }

    // When do we need to send smth timer-based next time.
    fn next_poll_send_to_at(&self) -> PollAt {
        let want_ack = self.ack_to_transmit();

        let delayed_ack_poll_at = match (want_ack, self.timers.ack_delay_timer) {
            (false, _) => PollAt::Ingress,
            (true, AckDelayTimer::Idle) => PollAt::Now,
            (true, AckDelayTimer::Waiting(t)) => PollAt::Time(t),
            (true, AckDelayTimer::Immediate) => PollAt::Now,
        };

        // We wait for the earliest of our timers to fire.
        self.timers.kind.poll_at().min(delayed_ack_poll_at)
    }

    fn user_rx_is_closed(&self) -> bool {
        self.user_rx.is_closed()
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

    is_outgoing: bool,

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

            is_outgoing: true,

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

            is_outgoing: false,
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
    first_packet: Option<UtpHeader>,
}

impl<T: Transport, E: UtpEnvironment> UtpStreamStarter<T, E> {
    pub fn start(self) -> UtpStream {
        let Self {
            stream,
            vsock,
            first_packet,
        } = self;

        let span = match vsock.parent_span.clone() {
            Some(s) => error_span!(parent: s, "utp_stream", conn_id_send = ?vsock.conn_id_send),
            None => error_span!(parent: None, "utp_stream", conn_id_send = ?vsock.conn_id_send),
        };

        spawn_print_error(span, vsock.run_forever(first_packet));
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
            is_outgoing,
            parent_span,
        } = args;

        let (user_rx, read_half) = UserRx::build(
            socket.opts().max_user_rx_buffered_bytes,
            socket.opts().max_rx_out_of_order_packets,
        );

        let user_tx = UserTx::new(socket.opts().virtual_socket_tx_bytes);
        let write_half = UtpStreamWriteHalf::new(user_tx.clone());

        let env = socket.env.copy();
        let now = env.now();

        let socket_opts = socket.opts();

        let vsock = VirtualSocket {
            state: VirtualSocketState::Established,
            env,
            socket: Arc::downgrade(socket),
            socket_created: socket.created,
            socket_opts: *socket_opts,
            remote,
            conn_id_send,
            timers: Timers {
                kind: TimerKind::new(),
                sleep: Box::pin(tokio::time::sleep(Duration::from_secs(0))),
                challenge_ack_timer: now - CHALLENGE_ACK_RATELIMIT,
                ack_delay_timer: AckDelayTimer::Idle,
            },
            last_remote_timestamp,
            last_remote_timestamp_instant: now,
            last_remote_window: remote_window,
            next_seq_nr,
            last_sent_seq_nr,
            last_consumed_remote_seq_nr,
            last_sent_ack_nr,
            rx,
            tx: FragmentedTx::new(),
            local_rx_last_ack: None,
            local_rx_dup_acks: 0,
            user_tx,
            rtte: {
                let mut rtt = RttEstimator::default();
                if let (Some(sent), Some(recv)) = (syn_sent_ts, ack_received_ts) {
                    rtt.on_send(sent, next_seq_nr);
                    rtt.on_ack(recv, next_seq_nr);
                }
                rtt
            },
            this_poll: ThisPoll {
                now,
                tmp_buf: vec![0u8; socket.opts().max_incoming_packet_size.get()],
                transport_pending: false,
            },
            congestion_controller: socket_opts.congestion.create(now),
            parent_span,
            drop_guard: DropGuardSendBeforeDeath::new(
                ControlRequest::Shutdown((remote, conn_id_recv)),
                &socket.control_requests,
            ),
            user_rx,
        };

        let first_packet = if !is_outgoing {
            let mut hdr = vsock.outgoing_header();
            // The initial "ACK" should be of the NEXT sequence, not the previous one.
            hdr.seq_nr += 1;
            Some(hdr)
        } else {
            None
        };

        let stream = UtpStream::new(read_half, write_half);
        UtpStreamStarter {
            stream,
            vsock,
            first_packet,
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
    Immediate,
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum TimerKind {
    Idle,
    Retransmit {
        expires_at: Instant,
        delay: Duration,
    },
    FastRetransmit,
}

struct Timers {
    sleep: Pin<Box<Sleep>>,

    kind: TimerKind,

    ack_delay_timer: AckDelayTimer,

    /// Used for rate-limiting: No more challenge ACKs will be sent until this instant.
    challenge_ack_timer: Instant,
}

impl TimerKind {
    fn new() -> TimerKind {
        TimerKind::Idle
    }

    fn should_retransmit(&self, timestamp: Instant) -> Option<Duration> {
        match *self {
            TimerKind::Retransmit { expires_at, delay } if timestamp >= expires_at => {
                Some(timestamp - expires_at + delay)
            }
            TimerKind::FastRetransmit => Some(Duration::from_millis(0)),
            _ => None,
        }
    }

    fn poll_at(&self) -> PollAt {
        match *self {
            TimerKind::Idle => PollAt::Ingress,
            TimerKind::Retransmit { expires_at, .. } => PollAt::Time(expires_at),
            TimerKind::FastRetransmit => PollAt::Now,
        }
    }

    fn set_for_idle(&mut self) {
        *self = TimerKind::Idle
    }

    fn set_for_retransmit(&mut self, timestamp: Instant, delay: Duration) {
        match *self {
            TimerKind::Idle { .. } | TimerKind::FastRetransmit { .. } => {
                *self = TimerKind::Retransmit {
                    expires_at: timestamp + delay,
                    delay,
                }
            }
            TimerKind::Retransmit { expires_at, delay } if timestamp >= expires_at => {
                *self = TimerKind::Retransmit {
                    expires_at: timestamp + delay,
                    delay: delay * 2,
                }
            }
            TimerKind::Retransmit { .. } => (),
        }
    }

    fn set_for_fast_retransmit(&mut self) {
        *self = TimerKind::FastRetransmit
    }

    fn is_retransmit(&self) -> bool {
        matches!(
            *self,
            TimerKind::Retransmit { .. } | TimerKind::FastRetransmit
        )
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
            AckDelayTimer::Immediate => {
                trace!("stop delayed ack timer (was force-expired)")
            }
        }
        self.ack_delay_timer = AckDelayTimer::Idle;
    }
}

// The main dispatch loop for the virtual socket is here.
impl<T: Transport, Env: UtpEnvironment> std::future::Future for VirtualSocket<T, Env> {
    type Output = anyhow::Result<()>;

    fn poll(self: std::pin::Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();

        macro_rules! bail_if_err {
            ($e:expr) => {
                match $e {
                    Ok(val) => val,
                    Err(e) => {
                        this.just_before_death(Some(&e));
                        return Poll::Ready(Err(e));
                    }
                }
            };
        }

        macro_rules! bail_if_cannot_send {
            ($e:expr) => {{
                let val = bail_if_err!($e);
                if this.this_poll.transport_pending {
                    return Poll::Pending;
                }
                val
            }};
        }

        // Doing this once here not to upgrade too often below.
        let socket = bail_if_err!(this.socket.upgrade().context("device dead"));
        let socket = &*socket;

        // Track if UDP socket is full this poll, and don't send to it if so.
        this.this_poll.transport_pending = false;
        this.this_poll.now = this.env.now();

        loop {
            // Flow control: flush as many out of order messages to user RX as possible.
            bail_if_err!(this.user_rx.flush(cx));

            // Read incoming stream.
            bail_if_err!(this.process_all_incoming_messages(cx, socket));

            bail_if_cannot_send!(this.maybe_send_delayed_ack(cx, socket));

            this.maybe_prepare_for_retransmission();

            // Fragment data sent by user
            bail_if_cannot_send!(this.fragment_tx_queue(cx));

            // (Re)send tx queue.
            bail_if_cannot_send!(this.send_tx_queue(cx, socket));

            bail_if_cannot_send!(this.maybe_send_fin(cx, socket));

            if this.state.is_done() {
                this.just_before_death(None);
                return Poll::Ready(Ok(()));
            }

            if this.user_rx_is_closed() && this.state.is_local_fin_or_later() {
                trace!(current_state=?this.state, "both halves are dead, no reason to continue");
                this.just_before_death(None);
                return Poll::Ready(Ok(()));
            }

            match this.next_poll_send_to_at() {
                PollAt::Now => {
                    trace!("need to repoll");
                    continue;
                }
                PollAt::Time(instant) => {
                    let duration = instant - this.this_poll.now;
                    trace!(sleep = ?duration, "arming timer");
                    if this.timers.arm_in(cx, duration) {
                        return Poll::Pending;
                    } else {
                        trace!(deadline = ?instant - this.this_poll.now, "failed arming timer, continuing loop");
                        continue;
                    }
                }
                PollAt::Ingress => return Poll::Pending,
            }
        }
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
        raw::{Type, UtpHeader},
        stream_dispatch::VirtualSocketState,
        test_util::{
            env::MockUtpEnvironment, setup_test_logging, transport::RememberingTransport, ADDR_1,
            ADDR_2,
        },
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

        fn send_data(&mut self, seq_nr: u16, payload: &str) {
            let mut header = self.vsock.outgoing_header();
            header.set_type(Type::ST_DATA);
            header.seq_nr = seq_nr.into();
            self.send_msg(header, payload);
        }

        fn take_sent(&self) -> Vec<UtpMessage> {
            self.transport.take_sent_utpmessages()
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

        async fn read_all_available(&mut self) -> std::io::Result<Vec<u8>> {
            let mut buf = vec![0u8; 1024 * 1024];
            // unbounded_channel() needs time to progress. It doesn't return everything it
            // has on each poll so we need to
            let timeout = tokio::time::sleep(Duration::from_millis(100));
            let mut offset = 0;

            tokio::pin!(timeout);
            let stream = self.stream.as_mut().unwrap();
            loop {
                tokio::select! {
                    _ = &mut timeout => {
                        break;
                    },
                    res = stream.read(&mut buf[offset..]) => {
                        let len = res?;
                        if len == 0 {
                            break;
                        }
                        offset += len;
                    }
                }
            }

            buf.truncate(offset);
            Ok(buf)
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

    fn make_test_vsock(opts: SocketOpts) -> TestVsock {
        let transport = RememberingTransport::new(ADDR_1);
        let env = MockUtpEnvironment::new();
        let socket = UtpSocket::new_with_opts(transport.clone(), env.clone(), opts).unwrap();
        let (tx, rx) = unbounded_channel();

        let remote_syn = UtpHeader {
            htype: Type::ST_SYN,
            ..Default::default()
        };

        let args = StreamArgs::new_incoming(100.into(), &remote_syn);

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

        let mut t = make_test_vsock(Default::default());
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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());
        t.vsock.socket_opts.max_outgoing_payload_size = NonZeroUsize::new(2).unwrap();

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
        let mut t = make_test_vsock(Default::default());

        // First allow sending by setting window size
        t.vsock.last_remote_window = 1024;

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
        let mut t = make_test_vsock(Default::default());
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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

        // Enable Nagle (should be on by default, but let's be explicit)
        t.vsock.socket_opts.nagle = true;
        // Set a large max payload size to ensure we're testing Nagle, not fragmentation
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

        // As debugging, ensure we did not even fragment the new packets yet.
        assert_eq!(t.vsock.tx.total_len_packets(), 1);
        assert_eq!(t.vsock.tx.total_len_bytes(), 1);
        assert_eq!(t.vsock.tx.first_seq_nr().unwrap(), first_seq_nr);

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

        assert_eq!(t.vsock.tx.total_len_packets(), 1);
        assert_eq!(t.vsock.tx.total_len_bytes(), 2);

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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

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
            t.vsock.next_seq_nr.0 - first_seq_nr.0,
            3, // 3 packets total
            "Should have split data into correct number of packets"
        );
    }

    #[tokio::test]
    async fn test_zero_window_handling() {
        setup_test_logging();
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());

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
        let mut t = make_test_vsock(Default::default());
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
        assert_eq!(t.vsock.tx.total_len_packets(), 2);

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
        assert_eq!(t.vsock.tx.total_len_packets(), 1);

        // Change to ST_DATA with same ACK - shouldn't count as duplicate
        header.htype = Type::ST_DATA;
        header.seq_nr += 1;
        t.send_msg(header, "a");
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.local_rx_dup_acks, 0);
        assert_eq!(t.vsock.tx.total_len_packets(), 1);

        // Another ST_DATA - shouldn't count
        header.seq_nr += 1;
        t.send_msg(header, "b");
        t.poll_once_assert_pending().await;
        assert_eq!(t.vsock.local_rx_dup_acks, 0);
        assert_eq!(t.vsock.tx.total_len_packets(), 1);

        // ST_FIN with same ACK - shouldn't count
        header.htype = Type::ST_FIN;
        header.seq_nr += 1;
        t.send_msg(header, "");

        t.poll_once_assert_pending().await;

        // Duplicate ACK count should be 0 since non-ST_STATE packets don't count
        assert_eq!(t.vsock.local_rx_dup_acks, 0);
        assert_eq!(t.vsock.tx.total_len_packets(), 1);
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
        let mut t = make_test_vsock(Default::default());
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
        assert_eq!(t.take_sent().len(), 0, "nothing gets sent for out of order");

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

        let mut t = make_test_vsock(opts);

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

        // Test assembly queue limit first
        // Send packets out of order to fill assembly queue
        t.send_data(3, "third"); // Out of order
        t.send_data(4, "fourth"); // Out of order
        t.send_data(6, "sixth"); // Should be dropped - assembly queue full

        t.poll_once_assert_pending().await;

        // Only two packets should be in assembly queue
        assert_eq!(t.vsock.user_rx.assembler_packets(), 2);

        // Send in-order packet to trigger processing
        t.send_data(1, "first");
        t.poll_once_assert_pending().await;

        // Read available data
        let read = t.read_all_available().await.unwrap();
        assert_eq!(&read, b"first"); // Only first packet should be read

        // Now test user rx channel limit
        // Send several packets that exceed the rx buffer size
        for i in 6..15 {
            t.send_data(i, "data!");
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
        t.send_data(15, "dropped");
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

        let mut t = make_test_vsock(SocketOpts {
            rx_bufsize: Some(DATA_SIZE),
            ..Default::default()
        });

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
}
