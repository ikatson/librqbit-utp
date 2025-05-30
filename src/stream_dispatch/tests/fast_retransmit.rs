use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::{calc_mtu_for_mss, make_test_vsock},
    test_util::setup_test_logging,
    SocketOpts,
};

#[tokio::test]
async fn test_fast_retransmit() {
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

    let mut ack = UtpHeader {
        htype: ST_STATE,
        seq_nr: 0.into(),
        ack_nr: 99.into(),
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
async fn test_duplicate_ack_counted_only_on_st_state() {
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

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 0.into(),
            ack_nr: 99.into(),
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
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);

    // Change to ST_DATA with same ACK - shouldn't count as duplicate
    header.htype = ST_DATA;
    header.seq_nr += 1;
    t.send_msg(header, "a");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);

    // Another ST_DATA - shouldn't count
    header.seq_nr += 1;
    t.send_msg(header, "b");
    t.poll_once_assert_pending().await;
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);

    // ST_FIN with same ACK - shouldn't count
    header.htype = ST_FIN;
    header.seq_nr += 1;
    t.send_msg(header, "");

    t.poll_once_assert_pending().await;

    // Duplicate ACK count should be 0 since non-ST_STATE packets don't count
    assert_eq!(t.vsock.user_tx_segments.total_len_packets(), 1);
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_FIN, seq_nr = 103, ack_nr = 3)],
        "Should have ACKed the FIN and sent back another FIN"
    );

    // Now send three ST_STATE duplicates
    header.htype = ST_STATE;
    t.send_msg(header, "");
    t.send_msg(header, "");
    t.send_msg(header, "");

    t.poll_once_assert_pending().await;

    // Now we should see duplicate ACKs counted and fast retransmit triggered
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 102, ack_nr = 3, payload = "world"),
            // Re-send the FIN.
            cmphead!(ST_FIN, seq_nr = 103, ack_nr = 3)
        ],
        "Should have triggered fast retransmit"
    );
}
