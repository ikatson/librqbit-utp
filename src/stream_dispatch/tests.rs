use std::{future::poll_fn, num::NonZeroUsize, pin::Pin, sync::Arc, task::Poll, time::Duration};

use futures::FutureExt;
use tokio::{
    io::{AsyncReadExt, AsyncWrite, AsyncWriteExt},
    sync::mpsc::{unbounded_channel, UnboundedSender},
};
use tracing::trace;

use crate::{
    constants::{IPV4_HEADER, MIN_UDP_HEADER, UTP_HEADER_SIZE},
    message::UtpMessage,
    raw::{selective_ack::SelectiveAck, Type::*, UtpHeader},
    seq_nr::SeqNr,
    stream_dispatch::{StreamArgs, UtpStreamStarter, VirtualSocket, VirtualSocketState},
    test_util::{
        env::MockUtpEnvironment, setup_test_logging, transport::RememberingTransport, ADDR_1,
        ADDR_2,
    },
    traits::UtpEnvironment,
    SocketOpts, UtpSocket, UtpStream,
};

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
            htype: ST_DATA,
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

    #[track_caller]
    fn assert_sent_empty(&self) {
        assert_eq!(self.take_sent(), Vec::<UtpMessage>::new())
    }

    #[track_caller]
    fn assert_sent_empty_msg(&self, msg: &'static str) {
        assert_eq!(self.take_sent(), Vec::<UtpMessage>::new(), "{}", msg)
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
                panic!("poll returned Ready, but expected Pending");
            }
        };
    }

    async fn poll_once_assert_ready(&mut self) -> anyhow::Result<()> {
        let res = self.poll_once().await;
        match res {
            Poll::Pending => {
                panic!("expected poll to return Ready, but got Pending")
            }
            Poll::Ready(res) => res,
        }
    }
}

fn make_test_vsock_args(opts: SocketOpts, args: StreamArgs, env: MockUtpEnvironment) -> TestVsock {
    let transport = RememberingTransport::new(ADDR_1);
    let socket = UtpSocket::new_with_opts(transport.clone(), env.clone(), opts).unwrap();
    let (tx, rx) = unbounded_channel();

    let UtpStreamStarter { stream, vsock, .. } = UtpStreamStarter::new(&socket, ADDR_2, rx, args);

    TestVsock {
        transport,
        env,
        _socket: socket,
        vsock,
        stream: Some(stream),
        tx,
    }
}

fn make_test_vsock(opts: SocketOpts, is_incoming: bool) -> TestVsock {
    let env = MockUtpEnvironment::new();
    let args = if is_incoming {
        let remote_syn = UtpHeader {
            htype: ST_SYN,
            ..Default::default()
        };
        StreamArgs::new_incoming(100.into(), &remote_syn)
    } else {
        let remote_ack = UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        };
        StreamArgs::new_outgoing(&remote_ack, env.now(), env.now())
    };
    make_test_vsock_args(opts, args, env)
}

#[tokio::test]
async fn test_delayed_ack_sent_once() {
    setup_test_logging();

    let mut t = make_test_vsock(Default::default(), false);
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    t.send_msg(
        UtpHeader {
            htype: ST_DATA,
            seq_nr: 1.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            ..Default::default()
        },
        "hello",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(&t.read_all_available().await.unwrap(), b"hello");
    t.assert_sent_empty();

    // Pretend it's 1 second later.
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    assert_eq!(&t.read_all_available().await.unwrap(), b"");
    // Assert an ACK was sent.
    let sent = t.take_sent();
    assert_eq!(sent, vec![cmphead! {ST_STATE, ack_nr=1}]);

    // Assert nothing else is sent later.
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}

#[tokio::test]
async fn test_doesnt_send_until_window_updated() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), true);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, ack_nr = 0)],
        "intial SYN-ACK should be sent"
    );
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
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 1024,
            ..Default::default()
        },
        "hello",
    );
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, ack_nr = 0, payload = "hello")]
    );
}

#[tokio::test]
async fn test_sends_up_to_remote_window_only_single_msg() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), true);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, ack_nr = 0)],
        "intial SYN-ACK should be sent"
    );
    assert_eq!(t.vsock.last_remote_window, 0);

    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 4,
            ..Default::default()
        },
        "hello",
    );
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, ack_nr = 0, payload = "hell")]
    );

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
    t.assert_sent_empty();

    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            // This is enough to send "hello" in 3 messages
            wnd_size: 5,
            ..Default::default()
        },
        "hello",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 100, payload = "he"),
            cmphead!(ST_DATA, seq_nr = 101, payload = "ll"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "o")
        ]
    );

    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
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

    let seq_nr: SeqNr = 101.into();

    // First transmission should happen
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = seq_nr, payload = "hello"),]
    );

    // Wait for retransmission timeout
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;

    // Should retransmit the same data
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = seq_nr, payload = "hello")]
    );

    // Until time goes on, nothing should happen.
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Wait again for 2nd retransmission.
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = seq_nr, payload = "hello")]
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
        htype: ST_STATE,
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

    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "world")
        ]
    );

    // Simulate receiving duplicate ACKs (as if first packet was lost but later ones arrived)
    ack.ack_nr = 101.into();

    // First ACK
    t.send_msg(ack, "");

    // First duplicate ACK
    t.send_msg(ack, "");

    // Second duplicate ACK
    t.send_msg(ack, "");

    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.local_rx_dup_acks, 2);
    t.assert_sent_empty_msg("Should not retransmit yet");

    // Third duplicate ACK should trigger fast retransmit
    t.send_msg(ack, "");

    t.poll_once_assert_pending().await;
    trace!("s");

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 102, payload = "world")],
        "Should have retransmitted second packet"
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
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "hello")],
    );

    // Acknowledge the data
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 102)],
        "FIN should use next sequence number after data"
    );

    // Remote acknowledges our FIN
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 102.into(),
            ..Default::default()
        },
        "",
    );

    // Remote sends its FIN
    t.send_msg(
        UtpHeader {
            htype: ST_FIN,
            seq_nr: 1.into(),
            ack_nr: 102.into(),
            ..Default::default()
        },
        "",
    );
    let result = t.poll_once().await;

    // We should acknowledge remote's FIN
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, ack_nr = 1, seq_nr = 102)],
        "FIN should use next sequence number after data"
    );
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
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )],
    );

    // Acknowledge the data
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            ..Default::default()
        },
        "",
    );
    // Deliver the ACK.
    t.poll_once_assert_pending().await;

    // Initiate FIN.
    drop(writer);

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 102, ack_nr = 0)],
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
            htype: ST_STATE,
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
    t.assert_sent_empty();

    // Ensure flush blocks at first
    let flush_result = poll_fn(|cx| {
        let res = Pin::new(&mut writer).poll_flush(cx);
        Poll::Ready(res)
    })
    .await;
    assert!(flush_result.is_pending());

    // Now we send the data.
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "hello")],
    );

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
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
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
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 100.into(),
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
            htype: ST_DATA,
            seq_nr: 2.into(),
            ack_nr: 100.into(),
            ..Default::default()
        },
        "world",
    );
    t.poll_once_assert_pending().await;

    // Nothing should be readable yet as we're missing seq 1
    assert_eq!(&t.read_all_available().await.unwrap(), b"");

    // We should send an immediate ACK due to out-of-order delivery
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 100, ack_nr = 0)]
    );

    // Send seq 3
    t.send_msg(
        UtpHeader {
            htype: ST_DATA,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 100, ack_nr = 0)]
    );

    // Finally send seq 1
    t.send_msg(
        UtpHeader {
            htype: ST_DATA,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 100, ack_nr = 3)]
    );
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
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, ack_nr = 0, payload = "a")]
    );

    // Write another small chunk - should not be sent while first is unacked
    t.stream.as_mut().unwrap().write_all(b"b").await.unwrap();
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Nagle should prevent sending small chunk while data is in flight");

    // Write more data - still should not send
    t.stream.as_mut().unwrap().write_all(b"c").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 0);

    // As debugging, ensure we did not even segment the new packets yet.
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(t.vsock.user_tx_segments.total_len_bytes(), 1);
    assert_eq!(t.vsock.user_tx_segments.first_seq_nr().unwrap(), 101.into());

    trace!("Acknowledge first packet");
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(t.vsock.user_tx_segments.total_len_bytes(), 2);

    // After ACK, buffered data should be sent as one packet
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 102, ack_nr = 0, payload = "bc")],
        "Buffered data should be coalesced"
    );

    // Now disable Nagle
    t.vsock.socket_opts.nagle = false;

    // Small writes should be sent immediately
    t.stream.as_mut().unwrap().write_all(b"d").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 103, ack_nr = 0, payload = "d")],
    );

    // Next small write should also go immediately, even without ACK
    t.stream.as_mut().unwrap().write_all(b"e").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 104, ack_nr = 0, payload = "e")],
    );
}

#[tokio::test]
async fn test_resource_cleanup_both_sides_dropped_normally() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )],
    );

    // Drop the writer - this should NOT trigger sending FIN until data is ACKed.
    drop(writer);
    t.poll_once_assert_pending().await;

    // Nothing should happen until it's ACKed.
    t.assert_sent_empty();
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 102, ack_nr = 0)],
        "Should send FIN"
    );

    assert_eq!(
        t.vsock.state,
        VirtualSocketState::FinWait1 {
            our_fin: 102.into()
        }
    );

    // Drop the reader - this should cause the stream to complete in 1 second.
    drop(reader);
    t.poll_once_assert_pending().await;
    t.env.increment_now(Duration::from_secs(1));
    let result = t.poll_once().await;
    assert!(
        !matches!(result, Poll::Pending),
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
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    drop(t.stream.take());
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)]
    );

    t.env.increment_now(Duration::from_secs(1));
    let result = t.poll_once().await;
    t.assert_sent_empty(); // todo: maybe there should be retransmission here?

    assert!(
        !matches!(result, Poll::Pending),
        "Poll should complete in 1 second after both halves are dropped"
    );
}

#[tokio::test]
async fn test_resource_cleanup_with_pending_data() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // Nothing else should be sent before ACK.
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Send ACK
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.last_sent_seq_nr,
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Next poll should send FIN and die in 1 second.
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 102, ack_nr = 0)]
    );

    t.env.increment_now(Duration::from_secs(1));
    let res = t.poll_once().await;
    assert!(!matches!(res, Poll::Pending));
}

#[tokio::test]
async fn test_sender_flow_control() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    // Set initial remote window very small
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // No more data should be sent until window updates
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // ACK first packet but keep window small
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 4, // Reduce window further
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Should send up to new window size
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            ack_nr = 0,
            payload = " wor"
        )]
    );

    // Remote increases window and ACKs
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 102.into(),
            wnd_size: 10, // Open window
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Should send remaining data
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 103, ack_nr = 0, payload = "ld")]
    );

    // Verify total data sent matches original write
    assert_eq!(
        t.vsock.user_tx_segments.next_seq_nr(),
        104.into(),
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
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // ACK data but advertise zero window
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 0, // Zero window
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Nothing should be sent
    t.assert_sent_empty();

    // Remote sends window update
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024, // Open window
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Remaining data should now be sent
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            ack_nr = 0,
            payload = " world"
        )]
    );
}

#[tokio::test]
async fn test_congestion_control_basics() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    let remote_wnd = 64 * 1024;

    // Allow sending by setting large window
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
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
                htype: ST_STATE,
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
                htype: ST_STATE,
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
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 100.into(),
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
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, ack_nr = 0, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 102, ack_nr = 0, payload = "world")
        ]
    );
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 2);

    // Send three duplicate ACKs using different packet types
    let mut header = UtpHeader {
        htype: ST_STATE,
        seq_nr: 0.into(),
        ack_nr: 101.into(),
        wnd_size: 1024,
        ..Default::default()
    };

    // First normal ST_STATE ACK
    t.send_msg(header, "");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.local_rx_dup_acks, 0);
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);

    // Change to ST_DATA with same ACK - shouldn't count as duplicate
    header.htype = ST_DATA;
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
    header.htype = ST_FIN;
    header.seq_nr += 1;
    t.send_msg(header, "");

    t.poll_once_assert_pending().await;

    // Duplicate ACK count should be 0 since non-ST_STATE packets don't count
    assert_eq!(t.vsock.local_rx_dup_acks, 0);
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 102, ack_nr = 3)],
        "Should have ACKed the FIN"
    );

    // Now send three ST_STATE duplicates
    header.htype = ST_STATE;
    t.send_msg(header, "");
    t.send_msg(header, "");
    t.send_msg(header, "");

    t.poll_once_assert_pending().await;

    // Now we should see duplicate ACKs counted and fast retransmit triggered
    assert_eq!(t.vsock.local_rx_dup_acks, 3);
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            // TODO: which? ack_nr = 3,
            payload = "world"
        )],
        "Should have triggered fast retransmit"
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
        htype: ST_DATA,
        seq_nr: 2.into(),
        ack_nr: t.vsock.last_sent_seq_nr,
        wnd_size: 1024,
        ..Default::default()
    };

    header.seq_nr = 2.into();
    t.send_msg(header, "world");
    t.poll_once_assert_pending().await;
    assert!(!t.vsock.user_rx.assembler_empty());
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 100, ack_nr = 0)],
        "immediate ACK should have been sent"
    );

    // remote sends FIN
    header.set_type(ST_FIN);
    header.seq_nr = 3.into();
    t.send_msg(header, "");
    t.poll_once_assert_pending().await;
    assert!(!t.vsock.user_rx.assembler_empty());
    t.assert_sent_empty_msg("nothing gets sent for out of order FIN");

    // send in-order. This should ACK the DATA
    header.set_type(ST_DATA);
    header.seq_nr = 1.into();
    t.send_msg(header, "a");

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 100, ack_nr = 2)],
        "we should sent DATA ack as it was out of order"
    );

    // send in-order. This should ACK the DATA
    header.set_type(ST_FIN);
    header.seq_nr = 3.into();
    t.send_msg(header, "");

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 100, ack_nr = 3)],
        "we should ACK the FIN"
    );
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
        htype: ST_DATA,
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
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello world"
        )],
        "should send data initially"
    );

    // No retransmission should occur before timeout
    t.env.increment_now(Duration::from_millis(100));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not retransmit before timeout");

    // After timeout, packet should be retransmitted
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello world"
        )],
        "should retransmit"
    );

    // Second timeout should trigger another retransmission with doubled timeout
    t.env.increment_now(Duration::from_secs(2));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello world"
        )],
        "should retransmit after second timeout"
    );

    // Now ACK the packet - should stop retransmissions
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // No more retransmissions should occur
    t.env.increment_now(Duration::from_secs(4));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not retransmit after ACK");

    // Write new data - should use next sequence number
    t.stream.as_mut().unwrap().write_all(b"test").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            ack_nr = 0,
            payload = "test"
        )]
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
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, ack_nr = 0, payload = "hello"),
            cmphead!(ST_DATA, seq_nr = 102, ack_nr = 0, payload = "world")
        ]
    );

    // Send selective ACK indicating second packet was received but first wasn't
    let header = UtpHeader {
        htype: ST_STATE,
        seq_nr: 0.into(),
        ack_nr: 100.into(), // ACK previous packet
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // Send normal ACK for first packet
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // No more retransmissions should occur
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not retransmit after both packets acknowledged");
}

#[tokio::test]
async fn test_st_reset_error_propagation() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    let (mut reader, _writer) = t.stream.take().unwrap().split();

    // Send a RESET packet
    t.send_msg(
        UtpHeader {
            htype: ST_RESET,
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
            htype: ST_STATE,
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // Acknowledge the data
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
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
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 102, ack_nr = 0)]
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )]
    );

    // Acknowledge the data
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Remote sends some data - it should accumulate in OOQ, but be ACKed.
    t.send_data(1, t.vsock.last_sent_seq_nr, "ignored data");
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Now drop the writer
    drop(writer);

    // Next poll should send FIN
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 102, ack_nr = 1)],
        "Should send FIN after writer dropped"
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
    assert_eq!(t.take_sent(), Vec::<UtpMessage>::new());

    // Now the window should be less than MSS.
    t.send_data(2, t.vsock.last_sent_seq_nr, "b");
    t.poll_once_assert_pending().await;
    assert!(t.vsock.user_rx.is_flush_waker_registered());

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_STATE,
            seq_nr = 100,
            ack_nr = 2,
            wnd_size = 4u32
        )],
        "Should have sent an ACK with new window less than MSS"
    );

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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_STATE,
            seq_nr = 100,
            ack_nr = 2,
            wnd_size = 9u32
        )],
        "Should have sent an ACK with updated window"
    );
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
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 100, ack_nr = 0)],
        "should have sent syn-ack"
    );

    let ack_nr = 99.into();
    let connection_id = t.vsock.conn_id_send + 1;
    let make_msg = |seq_nr, payload| {
        make_msg(
            UtpHeader {
                htype: ST_DATA,
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
    assert_eq!(
        sent,
        vec![cmphead!(ST_STATE, seq_nr = 99, ack_nr = 2, wnd_size = 0u32)],
        "should have sent zero-window ACK"
    );

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
    assert_eq!(
        sent,
        vec![cmphead!(ST_STATE, seq_nr = 99, ack_nr = 2, wnd_size = 5u32)],
        "should have sent zero-window ACK"
    );
}

#[tokio::test]
async fn test_inactivity_timeout() {
    setup_test_logging();

    // Set up socket with a short inactivity timeout
    let opts = SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let mut t = make_test_vsock(opts, false);

    // Send initial data to start the inactivity timer
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"hello")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;

    // Initial data should be sent
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )],
    );

    // Wait just under timeout - connection should still be alive, but we
    // should get a retransmission.
    t.env.increment_now(Duration::from_millis(900));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hello"
        )],
    );

    // Remote sends ACK - should reset inactivity timer
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Wait just under timeout again - connection should still be alive
    t.env.increment_now(Duration::from_millis(900));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not send anything before timeout");

    // Wait past timeout - connection should not error out as TX is empty.
    t.env.increment_now(Duration::from_millis(200));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not send anything before timeout");

    // Write more data
    t.stream
        .as_mut()
        .unwrap()
        .write_all(b"world")
        .await
        .unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 102,
            ack_nr = 0,
            payload = "world"
        )],
    );

    // Wait past timeout - connection should error out as nothing received.
    t.env.increment_now(Duration::from_secs(1));
    let result = t.poll_once().await;
    // Should get inactivity timeout error
    match result {
        Poll::Ready(Err(e)) => {
            assert!(
                e.to_string().contains("inactive"),
                "Error should mention inactivity: {e}"
            );
        }
        other => panic!("Expected inactivity error, got: {other:?}"),
    }

    // Try to read - should get error
    let mut buf = [0u8; 1024];
    let read_result = t.stream.as_mut().unwrap().read(&mut buf).await;
    assert!(
        read_result.is_err(),
        "Read should fail after inactivity timeout"
    );
}

#[tokio::test]
async fn test_inactivity_timeout_initial_synack() {
    // Set up socket with a short inactivity timeout
    let opts = SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let mut t = make_test_vsock(opts, true);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 100, ack_nr = 0)],
        "Should send syn-ack"
    );

    // Wait past timeout.
    t.env.increment_now(Duration::from_secs(1));
    let result = t.poll_once().await;
    // Should get inactivity timeout error
    match result {
        Poll::Ready(Err(e)) => {
            assert!(
                e.to_string().contains("inactive"),
                "Error should mention inactivity: {e}"
            );
        }
        other => panic!("Expected inactivity error, got: {other:?}"),
    }

    // Try to read - should get error
    let mut buf = [0u8; 1024];
    let read_result = t.stream.as_mut().unwrap().read(&mut buf).await;
    assert!(
        read_result.is_err(),
        "Read should fail after inactivity timeout"
    );
}

#[tokio::test]
async fn test_inactivity_timeout_our_fin_acked() {
    let opts = SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let mut t = make_test_vsock(opts, false);

    // At first nothing should happen past timeout
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("nothing should happen");

    let (_read, write) = t.stream.take().unwrap().split();
    drop(write);

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
    );
    assert_eq!(
        t.vsock.state,
        VirtualSocketState::FinWait1 {
            our_fin: 101.into()
        }
    );

    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            ack_nr: 101.into(),
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    assert_eq!(t.vsock.state, VirtualSocketState::FinWait2);

    // Nothing should happen as our FIN was acked.
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}

#[tokio::test]
async fn test_inactivity_timeout_our_fin_unacked() {
    let opts = SocketOpts {
        remote_inactivity_timeout: Some(Duration::from_secs(1)),
        ..Default::default()
    };
    let mut t = make_test_vsock(opts, false);

    // At first nothing should happen past timeout
    t.env.increment_now(Duration::from_secs(1));
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("nothing should happen");

    let (mut read, write) = t.stream.take().unwrap().split();
    drop(write);

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
    );
    assert_eq!(
        t.vsock.state,
        VirtualSocketState::FinWait1 {
            our_fin: 101.into()
        }
    );

    // Connection should die as our FIN was not ACKed for too long.
    t.env.increment_now(Duration::from_secs(1));
    let result = t.poll_once().await;
    // Should get inactivity timeout error
    match result {
        Poll::Ready(Err(e)) => {
            assert!(
                e.to_string().contains("inactive"),
                "Error should mention inactivity: {e}"
            );
        }
        other => panic!("Expected inactivity error, got: {other:?}"),
    }

    // Try to read - should get error
    let mut buf = [0u8; 1024];
    let read_result = read.read(&mut buf).await;
    assert!(
        read_result.is_err(),
        "Read should fail after inactivity timeout"
    );
}

#[tokio::test]
async fn test_wait_for_remote_fin_both_halves_dropped_quick_reply() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    let (reader, writer) = t.stream.take().unwrap().split();

    // Drop both halves - this should trigger sending FIN
    drop(reader);
    drop(writer);

    // Next poll should send FIN
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
        "Should send FIN after both halves dropped"
    );

    // Remote acknowledges our FIN
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Wait a bit - connection should stay alive
    t.env.increment_now(Duration::from_millis(500));
    t.poll_once_assert_pending().await;

    // Remote sends FIN
    t.send_msg(
        UtpHeader {
            htype: ST_FIN,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Should send ACK for remote's FIN and complete
    let result = t.poll_once().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 1)],
        "Should ACK remote FIN"
    );
    assert!(
        matches!(result, Poll::Ready(Ok(()))),
        "Connection should complete after handling remote FIN"
    );
}

#[tokio::test]
async fn test_wait_for_remote_fin_both_halves_dropped_slow_reply() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    let (reader, writer) = t.stream.take().unwrap().split();

    // Drop both halves - this should trigger sending FIN
    drop(reader);
    drop(writer);

    // Next poll should send FIN and wait for reply.
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
        "Should send FIN after both halves dropped"
    );

    // Wait a long time, we should die
    t.env.increment_now(Duration::from_millis(3000));
    let result = t.poll_once().await;
    match result {
        Poll::Ready(Err(e)) => {
            assert!(
                e.to_string().contains("inactive"),
                "Error should mention inactivity: {e}"
            );
        }
        other => panic!("Expected inactivity error, got: {other:?}"),
    }
}

#[tokio::test]
async fn test_fin_retransmission() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    // Set a specific retransmission timeout for predictable testing
    let retransmit_timeout = Duration::from_millis(100);
    t.vsock.rtte.force_timeout(retransmit_timeout);

    // Drop writer immediately to trigger FIN
    let (_reader, writer) = t.stream.take().unwrap().split();
    drop(writer);

    // Should send initial FIN
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
        "Should send initial FIN"
    );

    // Wait for first retransmission timeout
    t.env.increment_now(retransmit_timeout);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
        "Should retransmit FIN after timeout"
    );

    // Wait for second retransmission timeout
    t.env.increment_now(retransmit_timeout);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 0)],
        "Should retransmit FIN after second timeout"
    );

    // Now ACK the FIN
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // Wait another timeout period - should not retransmit anymore
    t.env.increment_now(retransmit_timeout);
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("Should not retransmit FIN after it was ACKed");

    // Connection should stay alive waiting for remote FIN
    assert_eq!(t.vsock.state, VirtualSocketState::FinWait2);
}

#[tokio::test]
async fn test_read_gets_eof_on_fin_real_world_packets() {
    setup_test_logging();

    let env = MockUtpEnvironment::new();
    let mut t = make_test_vsock_args(
        SocketOpts::default(),
        StreamArgs::new_outgoing(
            &UtpHeader {
                htype: ST_STATE,
                connection_id: 1.into(),
                seq_nr: 43658.into(),
                ack_nr: 39078.into(),
                wnd_size: 1024,
                ..Default::default()
            },
            env.now(),
            env.now(),
        ),
        env,
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    assert_eq!(t.vsock.state, VirtualSocketState::Established);

    let (mut read, mut write) = t.stream.take().unwrap().split();

    write.write_all(b"hello").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 39079,
            ack_nr = 43657,
            payload = "hello"
        )]
    );

    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 43658.into(),
            ack_nr: 39079.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.send_msg(
        UtpHeader {
            htype: ST_FIN,
            seq_nr: 43658.into(),
            ack_nr: 39079.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 39079, ack_nr = 43658)],
        "we should ACK the FIN"
    );
    assert_eq!(
        t.vsock.state,
        VirtualSocketState::CloseWait {
            remote_fin: 43658.into()
        }
    );

    assert_eq!(
        read.read(&mut [0u8; 1024]).await.unwrap(),
        0,
        "we should get EOF"
    );

    // 6 seconds is how long it passed in the real world.
    t.env.increment_now(Duration::from_secs(6));
    drop(write);

    // Ensure we send FIN on writer drop
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 39080, ack_nr = 43658)],
        "we should send FIN"
    );
    assert_eq!(
        t.vsock.state,
        VirtualSocketState::LastAck {
            our_fin: 39080.into(),
            remote_fin: 43658.into()
        }
    );

    // Remote ACKs our FIN
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 43658.into(),
            ack_nr: 39080.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_ready().await.unwrap();
    t.assert_sent_empty();
    assert_eq!(t.vsock.state, VirtualSocketState::Closed);
}

#[tokio::test]
async fn test_real_world_packets_fin_sequence_0() {
    setup_test_logging();

    let env = MockUtpEnvironment::new();
    let mut t = make_test_vsock_args(
        SocketOpts::default(),
        StreamArgs::new_outgoing(
            &UtpHeader {
                htype: ST_STATE,
                connection_id: 1.into(),
                seq_nr: 52040.into(),
                ack_nr: 32747.into(),
                wnd_size: 1024,
                ..Default::default()
            },
            env.now(),
            env.now(),
        ),
        env,
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    assert_eq!(t.vsock.state, VirtualSocketState::Established);

    let (_read, mut write) = t.stream.take().unwrap().split();

    write.write_all(b"hello").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 32748,
            ack_nr = 52039,
            payload = "hello"
        )]
    );

    let mut rhdr = UtpHeader {
        htype: ST_DATA,
        seq_nr: 52040.into(),
        ack_nr: 32748.into(),
        wnd_size: 1024,
        ..Default::default()
    };
    t.send_msg(rhdr, "a");

    drop(write);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 32749, ack_nr = 52040)],
    );
    rhdr.seq_nr = 52041.into();
    t.send_msg(rhdr, "b");
    rhdr.seq_nr = 52042.into();
    t.send_msg(rhdr, "c");
    rhdr.seq_nr = 52043.into();
    t.send_msg(rhdr, "d");
    rhdr.seq_nr = 52044.into();
    rhdr.htype = ST_FIN;
    t.send_msg(rhdr, "");
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 32749, ack_nr = 52044)],
    );

    // Remote ACKs our FIN, BUT sends a higher seq_nr
    rhdr.htype = ST_STATE;
    rhdr.seq_nr = 52045.into();
    rhdr.ack_nr = 32749.into();
    t.send_msg(rhdr, "");

    // We should still process it and close ourselves.
    t.poll_once_assert_ready()
        .await
        .expect("expected poll to return Ready");
}

#[tokio::test]
async fn test_repeated_eof_read() {
    setup_test_logging();

    let mut t = make_test_vsock(Default::default(), false);
    let (mut r, _w) = t.stream.take().unwrap().split();
    t.send_msg(
        UtpHeader {
            htype: ST_FIN,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_pending().await;
    assert_eq!(r.read(&mut [0u8; 1]).await.unwrap(), 0);
    assert_eq!(r.read(&mut [0u8; 1]).await.unwrap(), 0);
}

#[tokio::test]
async fn test_repeated_eof_read_2() {
    setup_test_logging();

    let mut t = make_test_vsock(Default::default(), false);
    let (mut r, _w) = t.stream.take().unwrap().split();
    t.send_msg(
        UtpHeader {
            htype: ST_DATA,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            ..Default::default()
        },
        "hello",
    );
    t.send_msg(
        UtpHeader {
            htype: ST_FIN,
            seq_nr: 2.into(),
            ack_nr: 100.into(),
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_pending().await;
    let mut buf = [0u8; 1024];
    assert_eq!(r.read(&mut buf).await.unwrap(), 5);
    assert_eq!(&buf[..5], b"hello");
    assert_eq!(r.read(&mut buf).await.unwrap(), 0);
    assert_eq!(r.read(&mut buf).await.unwrap(), 0);
}
