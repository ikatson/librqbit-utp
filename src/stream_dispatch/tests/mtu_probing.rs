use tokio::io::AsyncWriteExt;
use tracing::trace;

use crate::{
    constants::{IPV4_HEADER, UDP_HEADER, UTP_HEADER},
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
    test_util::setup_test_logging,
    SocketOpts,
};

#[tokio::test]
async fn test_mtu_probing() {
    setup_test_logging();

    const FAKE_MTU_LIMIT: usize = 1280;

    // Let's pretend the actual MTU is 1280. By probing through binary search, ensure it gets found quickly.
    let mut t = make_test_vsock(
        SocketOpts {
            link_mtu: Some(1500),
            ..Default::default()
        },
        false,
    );
    let (_r, mut w) = t.stream.take().unwrap().split();

    fn make_payload(len: usize) -> String {
        String::from_utf8(vec![b'a'; len]).unwrap()
    }

    // Binary search mtu lengths
    const EXPECTED_MTU_LENGTHS: &[u16] = &[
        1038, // (1500 + 576) / 2: first probe, successful
        1269, // (1500 + 1038) / 2: second probe, successful
        1384, // (1500 + 1269) / 2: third probe, unsuccessful
        1326, // (1384 + 1269) / 2: 4th probe, unsuccessful
        1297, // (1326 + 1269) / 2: 5th probe, unsuccessful
        1283, // (1297 + 1269) / 2: 6th probe, unsuccessful
        1276, // (1283 + 1269) / 2: 7th probe, successful
        1279, // (1283 + 1276) / 2: 8th probe, successful
        1281, // (1283 + 1279) / 2: 9th probe, unsuccessful
        1280, // (1281 + 1279) / 2: 10th probe, successful
        1280, // it shouldn't try increasing again as it's different only by 1
    ];

    w.write_all(make_payload(20000).as_bytes()).await.unwrap();
    for mtu in EXPECTED_MTU_LENGTHS.iter().copied() {
        let expected_payload_len = (mtu - IPV4_HEADER - UTP_HEADER - UDP_HEADER) as usize;
        trace!(mtu, expected_payload_len);
        t.poll_once_assert_pending().await;
        let sent = t.take_sent();
        assert_eq!(
            *sent.last().unwrap(),
            cmphead!(
                ST_DATA,
                payload = make_payload((mtu - IPV4_HEADER - UTP_HEADER - UDP_HEADER) as usize)
            )
        );
        if mtu as usize > FAKE_MTU_LIMIT {
            t.env.increment_now(t.vsock.rtte.retransmission_timeout());
        } else {
            t.send_msg(
                UtpHeader {
                    htype: ST_STATE,
                    seq_nr: 1.into(),
                    ack_nr: sent[0].header.seq_nr,
                    wnd_size: 1024 * 1024,
                    ..Default::default()
                },
                "",
            );
        }
    }
}
