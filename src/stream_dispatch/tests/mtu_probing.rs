use std::time::Duration;

use tokio::io::AsyncWriteExt;

use crate::{
    constants::{IPV4_HEADER, UDP_HEADER, UTP_HEADER},
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
    test_util::{cmphead::CmpUtpHeader, setup_test_logging},
    SocketOpts,
};

#[tokio::test]
async fn test_mtu_probing() {
    setup_test_logging();

    // Let's pretend the actual MTU is 1280. By probing through binary search, ensure it gets found quickly.
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(1500),
            disable_nagle: true,
            ..Default::default()
        },
        false,
    );
    let (_r, mut w) = t.stream.take().unwrap().split();

    fn make_payload(len: usize) -> String {
        String::from_utf8(vec![b'a'; len]).unwrap()
    }

    const fn calc_payload_size(mtu: u16) -> usize {
        (mtu - IPV4_HEADER - UTP_HEADER - UDP_HEADER) as usize
    }

    fn p(mtu: u16) -> String {
        make_payload(calc_payload_size(mtu))
    }

    // Force congestion controller to have a very high window so that it doesn't interfere.
    t.vsock
        .congestion_controller
        .on_recovered(1024 * 1024, 100 * 1024 * 1024);

    #[derive(Debug)]
    enum TestCommand {
        ExpectSend(Vec<CmpUtpHeader>),
        Ack(u16),
        WaitForRto,
    }

    use TestCommand::*;

    const RWND: u32 = calc_payload_size(1280) as u32 * 6 - 1;

    // The data below assumes we send 3 packets with minimum segment size before sending an MTU probe.
    t.vsock
        .segment_sizes
        .set_probe_expiry_cooldown_max_packets(3);

    let commands = [
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 101, payload = p(576)),
            cmphead!(ST_DATA, seq_nr = 102, payload = p(1038)), // (1500 + 576) / 2: first probe, successful
        ]),
        Ack(102),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 103, payload = p(1038)),
            cmphead!(ST_DATA, seq_nr = 104, payload = p(1038)),
            cmphead!(ST_DATA, seq_nr = 105, payload = p(1038)),
            cmphead!(ST_DATA, seq_nr = 106, payload = p(1269)), // (1500 + 1038) / 2: second probe, successful
        ]),
        Ack(106),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 107, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 108, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 109, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 110, payload = p(1384)), // (1500 + 1269) / 2: third probe, unsuccessful
        ]),
        Ack(109),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 110, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 111, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 112, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 113, payload = p(1326)), // 4th probe, unsuccessful
        ]),
        Ack(112),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 113, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 114, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 115, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 116, payload = p(1297)), // 5th probe, unsuccessful
        ]),
        Ack(115),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 116, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 117, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 118, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 119, payload = p(1283)), // 6th probe, unsuccessful
        ]),
        Ack(118),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 119, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 120, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 121, payload = p(1269)),
            cmphead!(ST_DATA, seq_nr = 122, payload = p(1276)), // 7th probe, successful
        ]),
        Ack(122),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 123, payload = p(1276)),
            cmphead!(ST_DATA, seq_nr = 124, payload = p(1276)),
            cmphead!(ST_DATA, seq_nr = 125, payload = p(1276)),
            cmphead!(ST_DATA, seq_nr = 126, payload = p(1279)), // 8th probe, successful
        ]),
        Ack(126),
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 127, payload = p(1279)),
            cmphead!(ST_DATA, seq_nr = 128, payload = p(1279)),
            cmphead!(ST_DATA, seq_nr = 129, payload = p(1279)),
            cmphead!(ST_DATA, seq_nr = 130, payload = p(1281)), // 9th probe, unsuccessful
        ]),
        Ack(129),
        WaitForRto,
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 130, payload = p(1279)),
            cmphead!(ST_DATA, seq_nr = 131, payload = p(1279)),
            cmphead!(ST_DATA, seq_nr = 132, payload = p(1279)),
            cmphead!(ST_DATA, seq_nr = 133, payload = p(1280)), // 10th probe, successful
        ]),
        Ack(133),
        // At this point final MTU was found, and we don't need to be limited by probes, but rather by other factors.
        // In this test's case we'll get limited by remote window.
        ExpectSend(vec![
            cmphead!(ST_DATA, seq_nr = 134, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 135, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 136, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 137, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 138, payload = p(1280)),
            cmphead!(ST_DATA, seq_nr = 139, payload = p(1279)), // limited by rwnd, no nagle
        ]),
    ];

    w.write_all(make_payload(200000).as_bytes()).await.unwrap();

    for command in commands {
        match command {
            ExpectSend(sent) => {
                t.poll_once_assert_pending().await;
                assert_eq!(t.take_sent(), sent)
            }
            Ack(seq_nr) => {
                t.env.increment_now(Duration::from_secs(1));
                t.send_msg(
                    UtpHeader {
                        htype: ST_STATE,
                        seq_nr: 1.into(),
                        ack_nr: seq_nr.into(),
                        wnd_size: RWND,
                        ..Default::default()
                    },
                    "",
                );
            }
            WaitForRto => {
                t.poll_once_assert_pending().await;
                t.assert_sent_empty();
                t.env.increment_now(t.vsock.rtte.retransmission_timeout());
            }
        }
    }
}
