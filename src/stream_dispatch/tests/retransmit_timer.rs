// https://datatracker.ietf.org/doc/html/rfc6298#section-5

use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    constants::{IPV4_HEADER, UDP_HEADER, UTP_HEADER},
    raw::{Type::*, UtpHeader},
    stream_dispatch::{
        tests::{calc_mtu_for_mss, make_test_vsock},
        RetransmitTimer,
    },
    test_util::{cmphead::CmpUtpHeader, setup_test_logging},
    SocketOpts,
};

// (5.1) Every time a packet containing data is sent (including a
// retransmission), if the timer is not running, start it running
// so that it will expire after RTO seconds (for the current value
// of RTO).
#[tokio::test]
async fn retransmit_timer_started() {
    let mut t = make_test_vsock(Default::default(), false);
    let (_r, mut w) = t.stream.take().unwrap().split();

    assert!(matches!(t.vsock.timers.retransmit, RetransmitTimer::Idle));

    w.write_all(b"test").await.unwrap();

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "test")]
    );

    assert!(matches!(
        t.vsock.timers.retransmit,
        RetransmitTimer::Retransmit { .. }
    ));
}

// (5.2) When all outstanding data has been acknowledged, turn off the
// retransmission timer.
//
// (5.3) When an ACK is received that acknowledges new data, restart the
// retransmission timer so that it will expire after RTO seconds
// (for the current value of RTO).//
#[tokio::test]
async fn retransmit_timer_idle_when_all_data_acked() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            link_mtu: Some(calc_mtu_for_mss(2)),
            ..Default::default()
        },
        false,
    );
    let (_r, mut w) = t.stream.take().unwrap().split();

    // Send 2 packets
    w.write_all(b"test").await.unwrap();

    t.poll_once_assert_pending().await;
    assert_eq!(
        t.take_sent(),
        vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = "te"),
            cmphead!(ST_DATA, seq_nr = 102, payload = "st")
        ]
    );

    let timer_0 = t.vsock.timers.retransmit;

    assert!(matches!(timer_0, RetransmitTimer::Retransmit { .. }));

    t.env.increment_now(Duration::from_millis(100));
    // Only 1 packet ACKed. Retransmit timer should reset.
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    let timer_1 = t.vsock.timers.retransmit;
    match (timer_0, timer_1) {
        (
            RetransmitTimer::Retransmit { expires_at: e0, .. },
            RetransmitTimer::Retransmit { expires_at: e1, .. },
        ) if e1 > e0 => {}
        _ => {
            panic!("expected retransmit timer to update: timer_0={timer_0:?}, timer_1={timer_1:?}")
        }
    }

    // ACK the first packet again. Retransmit timer should not change.
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 101.into(),
            ..Default::default()
        },
        "",
    );
    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    assert_eq!(t.vsock.timers.retransmit, timer_1);

    // ACK the second packet. Retransmit timer should reset.
    t.env.increment_now(Duration::from_millis(100));
    t.send_msg(
        UtpHeader {
            htype: ST_STATE,
            wnd_size: 1024 * 1024,
            seq_nr: 0.into(),
            ack_nr: 102.into(),
            ..Default::default()
        },
        "",
    );

    t.poll_once_assert_pending().await;
    t.assert_sent_empty();

    assert_eq!(t.vsock.timers.retransmit, RetransmitTimer::Idle);
}

#[tokio::test]
async fn retransmit_timer_not_restarted_on_newly_sent_packets() {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            link_mtu: Some(calc_mtu_for_mss(2)),
            ..Default::default()
        },
        false,
    );
    let (_r, mut w) = t.stream.take().unwrap().split();

    // Send 2 packets separately.
    w.write_all(b"te").await.unwrap();
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 101, payload = "te"),]
    );

    let timer_0 = t.vsock.timers.retransmit;
    assert!(matches!(timer_0, RetransmitTimer::Retransmit { .. }));

    t.env.increment_now(Duration::from_millis(100));
    w.write_all(b"st").await.unwrap();
    t.poll_once_assert_pending().await;

    assert_eq!(
        t.take_sent(),
        vec![cmphead!(ST_DATA, seq_nr = 102, payload = "st"),]
    );

    let timer_1 = t.vsock.timers.retransmit;
    assert!(matches!(timer_0, RetransmitTimer::Retransmit { .. }));
    assert_eq!(timer_0, timer_1);
}
