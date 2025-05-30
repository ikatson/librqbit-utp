use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    raw::{Type::*, UtpHeader},
    stream_dispatch::{
        tests::{calc_mtu_for_mss, make_test_vsock, make_test_vsock_args},
        StreamArgs,
    },
    test_util::{env::MockUtpEnvironment, setup_test_logging},
    traits::UtpEnvironment,
    SocketOpts,
};

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
            ack_nr: t.vsock.seq_nr,
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
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 0)],
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
            ack_nr: 100.into(),
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
            ack_nr: 100.into(),
            wnd_size: 4,
            ..Default::default()
        },
        "hello",
    );
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 101,
            ack_nr = 0,
            payload = "hell"
        )]
    );

    // Until window updates and/or we receive an ACK, we don't send anything
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 0);
}

#[tokio::test]
async fn test_sends_up_to_remote_window_only_multi_msg() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(2)),
            ..Default::default()
        },
        true,
    );
    t.poll_once_assert_pending().await;
    assert_eq!(t.take_sent().len(), 1); // syn ack
    assert_eq!(t.vsock.last_remote_window, 0);

    // Hack: force congestion controller to have a larger window than 2 * MSS (4).
    t.vsock.congestion_controller.set_remote_window(5);
    t.vsock.congestion_controller.on_recovered(10000, 10000);
    assert_eq!(t.vsock.congestion_controller.window(), 5);

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
            seq_nr: 1.into(),
            ack_nr: 100.into(),
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
            cmphead!(ST_DATA, seq_nr = 101, payload = "he"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "ll"),
            cmphead!(ST_DATA, seq_nr = 103, payload = "o")
        ]
    );

    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
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
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 0)]
    );

    // Send seq 3
    t.send_msg(
        UtpHeader {
            htype: ST_DATA,
            seq_nr: 3.into(),
            ack_nr: t.vsock.seq_nr,
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
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 0)]
    );

    // Finally send seq 1
    t.send_msg(
        UtpHeader {
            htype: ST_DATA,
            seq_nr: 1.into(),
            ack_nr: t.vsock.seq_nr,
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
        vec![cmphead!(ST_STATE, seq_nr = 101, ack_nr = 3)]
    );
}

#[tokio::test]
async fn test_data_integrity_manual_packets() {
    setup_test_logging();

    const DATA_SIZE: usize = 1024 * 1024;
    const CHUNK_SIZE: usize = 1024;

    let mut t = make_test_vsock(
        SocketOpts {
            vsock_rx_bufsize_bytes: Some(DATA_SIZE),
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
        ack_nr: t.vsock.seq_nr,
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
async fn test_sequence_numbers_incoming() {
    // The sequence numbers for incoming connections don't make any sense unfortunately in the protocol,
    // or its reference libutp implementation.
    //
    // Before we sent any data, we keep sending ST_STATE packets with seq_nr=NEXT,
    // but the remote ACKs NEXT-1.
    // Like WTF.
    //
    // After that all the other packets have the actual last sent data sequence number in there.

    setup_test_logging();

    let mut t = make_test_vsock_args(
        Default::default(),
        StreamArgs::new_incoming(
            31420.into(),
            &UtpHeader {
                htype: ST_SYN,
                seq_nr: 15089.into(),
                ack_nr: 0.into(),
                ..Default::default()
            },
        ),
        Default::default(),
    );
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 31420, ack_nr = 15089)]
    );
    // allow sending by setting the remote window
    t.send_data(15090, 31419, "hello");
    t.vsock.force_immediate_ack();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 31420, ack_nr = 15090)]
    );

    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(b"hello").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 31420,
            ack_nr = 15090,
            payload = "hello"
        )]
    );

    t.vsock.socket_opts.nagle = false;
    w.write_all(b"world").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 31421,
            ack_nr = 15090,
            payload = "world"
        )]
    );
}

#[tokio::test]
async fn test_sequence_numbers_outgoing() {
    // The same test as test_sequence_numbers_incoming but in reverse.
    setup_test_logging();
    let env = MockUtpEnvironment::default();
    let mut t = make_test_vsock_args(
        Default::default(),
        StreamArgs::new_outgoing(
            &UtpHeader {
                htype: ST_STATE,
                seq_nr: 31420.into(),
                ack_nr: 15089.into(),
                wnd_size: 1024 * 1024,
                ..Default::default()
            },
            env.now(),
            env.now(),
        ),
        Default::default(),
    );
    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(b"hello").await.unwrap();

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(
            ST_DATA,
            seq_nr = 15090,
            ack_nr = 31419,
            payload = "hello"
        )]
    );
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 31420.into(),
            ack_nr: 15090.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
    t.send_data(31420, 15090, "hello");
    t.send_data(31421, 15090, "world");

    t.vsock.force_immediate_ack();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_STATE, seq_nr = 15091, ack_nr = 31421)]
    )
}
