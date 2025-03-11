use std::time::Duration;

use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    raw::Type::*,
    raw::UtpHeader,
    stream_dispatch::tests::{calc_mtu_for_mss, make_test_vsock},
    test_util::setup_test_logging,
    SocketOpts,
};

#[tokio::test]
async fn test_rto_single_packet_retransmission_mss_sized() {
    setup_test_logging();
    // Use MSS of 1 to create the smallest possible packet
    const MSS: usize = 1;
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(MSS)),
            ..Default::default()
        },
        false,
    );

    // Setup a fixed RTO duration for predictable testing
    let rto = Duration::from_millis(100);
    t.vsock.rtte.force_timeout(rto);

    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: t.vsock.seq_nr,
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Write multiple bytes, but they should be sent in 1-byte packets due to MSS=1
    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(b"abcdefg").await.unwrap();

    // Initial poll should send the first packet with one byte
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = "a"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "b")
        ],
        "Initial send should be 2 MSS"
    );

    // Ensure nothing gets sent.
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    // Let's trigger a timeout for the first packet
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;

    trace!(
        congestion_controller = ?t.vsock.congestion_controller,
        "congestion before RTO",
    );

    // RTO should retransmit ONLY the first unacked packet (according to RFC 5681)
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "a")],
        "After RTO, only the first unacked packet should be retransmitted"
    );

    // After RTO, additional poll should NOT send any new packets until an ACK is received
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("No additional packets should be sent after RTO until ACK received");

    trace!(
        congestion_controller = ?t.vsock.congestion_controller,
        "congestion after RTO",
    );

    // After ACK, we should resume sending from where we left off
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 101.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Poll should now continue sending packets that were queued
    t.poll_once_assert_pending().await;
    trace!(
        congestion_controller = ?t.vsock.congestion_controller,
        "congestion after resume",
    );
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 102, payload = "b"),
            cmphead!(ST_DATA, seq_nr = 103, payload = "c")
        ],
        "After ACK for retransmitted packet, should continue sending"
    );

    // ACK the second packet
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 102.into(),
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    // Should send more packets
    t.poll_once_assert_pending().await;
    trace!(
        congestion_controller = ?t.vsock.congestion_controller,
        "congestion after second ACK",
    );

    // Packets 103 and 104 should be in the network. MAYBE more, that depends on congestion calculations.
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 104, payload = "d"),],
        "After second ACK, should send next packet"
    );

    // Trigger another timeout for the third packet
    t.env.increment_now(rto);
    t.poll_once_assert_pending().await;

    // Should retransmit only the first unacked packet
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 103, payload = "c")],
        "After second RTO, only the first unacked packet should be retransmitted"
    );

    // Additional poll should not send any more packets
    t.poll_once_assert_pending().await;
    t.assert_sent_empty_msg("After second RTO, no additional packets should be sent until ACK");
}

// This would test an edge case where on RTO we send only one packet even
// if more packets fit into the window.
#[tokio::test]
async fn test_rto_single_packet_retransmission_smaller_than_mss() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(calc_mtu_for_mss(5)),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    // Allow sending by setting window size
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: t.vsock.seq_nr,
            wnd_size: 1024,
            ..Default::default()
        },
        "",
    );

    let (_r, mut w) = t.stream.take().unwrap().split();
    w.write_all(b"a").await.unwrap();

    // Writing in small pieces
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "a")]
    );

    w.write_all(b"bcde").await.unwrap();
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 102, payload = "bcde")]
    );

    // Trigger a timeout. Should retransmit only ONE packet and do nothing on second poll.
    t.env.increment_now(Duration::from_secs(10));
    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "a")]
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();
}
