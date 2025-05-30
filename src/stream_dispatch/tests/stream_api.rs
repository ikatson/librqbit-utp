// Tests for stream API.

use std::{future::poll_fn, pin::Pin, task::Poll};

use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::{
    raw::{Type::*, UtpHeader},
    stream_dispatch::tests::make_test_vsock,
    test_util::setup_test_logging,
};

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
            ack_nr: t.vsock.seq_nr,
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
            ack_nr: t.vsock.seq_nr,
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
