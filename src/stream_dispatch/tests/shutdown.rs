use std::{task::Poll, time::Duration};

use anyhow::Context;
use futures::FutureExt;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    time::timeout,
};
use tracing::trace;

use crate::{
    SocketOpts,
    raw::{Type::*, UtpHeader},
    stream_dispatch::{
        StreamArgs, VirtualSocketState,
        tests::{calc_mtu_for_mss, make_msg, make_test_vsock, make_test_vsock_args},
    },
    test_util::{env::MockUtpEnvironment, setup_test_logging},
    traits::UtpEnvironment,
};

#[tokio::test]
async fn test_no_writes_allowed_after_explicit_shutdown() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    let mut stream = t.stream.take().unwrap();

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: t.vsock.seq_nr,
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
    t.assert_sent_empty();

    // Initiate shutdown. It must complete on next vsock poll immediately as all the data should have been flushed.
    let (r, mut w) = stream.split();
    let shutdown = w.shutdown();
    tokio::pin!(shutdown);

    // Poll shutdown once
    std::future::poll_fn(|cx| match shutdown.poll_unpin(cx) {
        Poll::Pending => Poll::Ready(()),
        Poll::Ready(v) => panic!("expected shutdown to return pending, but got {v:?}"),
    })
    .await;

    // Ensure we get error on calling "write" again.
    assert!(w.write(b"test").await.is_err());

    drop(r);

    // Ensure we send FIN
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 102, ack_nr = 0)]
    );

    // 1 second later we should die and shutdown should complete.
    t.env.increment_now(Duration::from_secs(1));
    assert!(
        t.poll_once_assert_ready()
            .await
            .unwrap_err()
            .to_string()
            .contains("inactive")
    );
    timeout(Duration::from_secs(1), w.shutdown())
        .await
        .context("timeout waiting for shutdown")
        .unwrap()
        .context("error in shutdown")
        .unwrap();
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
            ack_nr: t.vsock.seq_nr,
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

    drop(reader);
    drop(writer);
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

    t.send_msg(
        UtpHeader {
            htype: ST_FIN,
            seq_nr: 1.into(),
            ack_nr: 102.into(),
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_ready().await.unwrap();
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
            ack_nr: t.vsock.seq_nr,
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
    let rto = Duration::from_millis(100);
    t.vsock.rtte.force_timeout(rto);

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 100.into(),
            wnd_size: 3, // small window so that we don't send everything right away
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
        vec![cmphead!(ST_DATA, seq_nr = 101, ack_nr = 0, payload = "hel"),]
    );

    // Nothing else should be sent before ACK.
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Data should be retransmitted
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, ack_nr = 0, payload = "hel"),]
    );

    // Send ACK for DATA
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
        vec![
            cmphead!(ST_DATA, seq_nr = 102, ack_nr = 0, payload = "lo"),
            cmphead!(ST_FIN, seq_nr = 103, ack_nr = 0)
        ]
    );

    // Send ACK for DATA
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 102.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    tracing::trace!("waiting for FIN retransmission");
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;
    // Should retransmit FIN
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 103, ack_nr = 0)],
        "should retransmit FIN"
    );
    assert_eq!(
        t.vsock.state,
        VirtualSocketState::FinWait1 {
            our_fin: 103.into()
        }
    );

    // We should die in ~1 second now
    t.env.increment_now(Duration::from_secs(1));

    let err = t.poll_once_assert_ready().await.unwrap_err();
    assert!(
        err.to_string().contains("inactive"),
        "Error should mention inactivity: {err}"
    );
}

#[tokio::test]
async fn test_finack_not_sent_until_all_data_consumed() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            ..Default::default()
        },
        false,
    );

    // Set a large retransmission timeout so we know fast retransmit is triggering, not RTO
    t.vsock.rtte.force_timeout(Duration::from_secs(10));

    // Send an out of order message
    let mut header = UtpHeader {
        htype: ST_DATA,
        seq_nr: 2.into(),
        ack_nr: t.vsock.seq_nr,
        wnd_size: 1024,
        ..Default::default()
    };

    t.send_msg(header, "world");
    t.poll_once_assert_pending().await;
    assert!(!t.vsock.user_rx.assembler_empty());
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 0)],
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
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 2)],
        "we should sent DATA ack as it was out of order"
    );

    // send in-order. This should ACK the DATA
    header.set_type(ST_FIN);
    header.seq_nr = 3.into();
    t.send_msg(header, "");

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 101, ack_nr = 3)],
        "we should ACK the FIN"
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
            ack_nr: t.vsock.seq_nr,
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
    t.send_data(1, t.vsock.seq_nr, "ignored data");
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
        vec![cmphead!(ST_STATE, seq_nr = 102, ack_nr = 1)],
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

    // Drop reader and writer immediately to trigger FIN
    t.stream.take().unwrap().split();

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
async fn test_write_fails_after_remote_fin_received() {
    setup_test_logging();
    let mut t = make_test_vsock(Default::default(), false);

    let (mut reader, mut writer) = t.stream.take().unwrap().split();

    // First write some data and get it ACKed
    writer.write_all(b"hello").await.unwrap();
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

    // Remote ACKs our data
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

    // Remote sends some data followed by FIN
    t.send_msg(
        UtpHeader {
            htype: ST_DATA,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "world",
    );
    t.send_msg(
        UtpHeader {
            htype: ST_FIN,
            seq_nr: 2.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;

    // We should ACK both the data and FIN as one message, and send FIN in return.
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 102, ack_nr = 2)],
        "Should ACK both data and FIN and send FIN"
    );

    // Try to write more data - should fail
    let write_result = writer.write(b"more data").await;
    assert!(
        write_result.is_err(),
        "Write should fail after receiving FIN"
    );
    let err = write_result.unwrap_err();
    assert!(
        err.to_string().contains("closed"),
        "Error should indicate connection is closed: {err}"
    );

    trace!("reading data");

    // Read should get the last data
    let mut buf = [0u8; 1024];
    let n = reader.read(&mut buf).await.unwrap();
    assert_eq!(&buf[..n], b"world");

    // Next read should return EOF (0 bytes)
    let n = reader.read(&mut buf).await.unwrap();
    assert_eq!(n, 0, "Read should return EOF after FIN");

    // We should wait for remote FIN
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Remote ACKs our FIN
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 2.into(),
            ack_nr: 102.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_ready()
        .await
        .expect("connection should complete with Ok(())");
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
        vec![cmphead!(ST_FIN, seq_nr = 39080, ack_nr = 43658)],
        "we should ACK the FIN"
    );
    assert_eq!(
        t.vsock.state,
        VirtualSocketState::LastAck {
            our_fin: 39080.into(),
            remote_fin: 43658.into()
        }
    );

    assert_eq!(
        read.read(&mut [0u8; 1024]).await.unwrap(),
        0,
        "we should get EOF"
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
async fn test_window_update_ack_after_read_with_waking() {
    setup_test_logging();

    // Configure socket with very small receive buffer to test flow control
    let mss = 5;
    let opts = SocketOpts {
        vsock_rx_bufsize_bytes: Some(mss * 2),
        link_mtu: Some(calc_mtu_for_mss(5)),
        ..Default::default()
    };

    let mut t = make_test_vsock(opts, true);
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 0)],
        "should have sent syn-ack"
    );

    let connection_id = t.vsock.conn_id_send + 1;
    let make_msg = |seq_nr, payload| {
        make_msg(
            UtpHeader {
                htype: ST_DATA,
                connection_id,
                wnd_size: 1024 * 1024,
                seq_nr,
                ack_nr: 100.into(),
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
        vec![cmphead!(
            ST_STATE,
            seq_nr = 101,
            ack_nr = 2,
            wnd_size = 0u32
        )],
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
        vec![cmphead!(
            ST_STATE,
            seq_nr = 101,
            ack_nr = 2,
            wnd_size = 5u32
        )],
        "should have sent zero-window ACK"
    );
}
