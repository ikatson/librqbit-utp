mod basics;
mod congestion;
mod delayed_ack;
mod fast_retransmit;
mod flow_control;
mod inactivity_timeout;
mod mtu_probing;
mod nagle;
mod retransmit_timer;
mod rtte;
mod shutdown;
mod stream_api;

use std::{sync::Arc, task::Poll, time::Duration};

use futures::FutureExt;
use tokio::sync::mpsc::{unbounded_channel, UnboundedSender};

use crate::{
    constants::{UDP_HEADER, UTP_HEADER},
    message::UtpMessage,
    raw::{Type::*, UtpHeader},
    seq_nr::SeqNr,
    stream_dispatch::{StreamArgs, UtpStreamStarter, VirtualSocket},
    test_util::{env::MockUtpEnvironment, transport::RememberingTransport, ADDR_1, ADDR_2},
    traits::UtpEnvironment,
    SocketOpts, UtpSocket, UtpStream,
};

fn make_msg(header: UtpHeader, payload: &str) -> UtpMessage {
    UtpMessage::new_test(header, payload.as_bytes())
}

const fn calc_mtu_for_mss(mss: usize) -> usize {
    const IP_HEADER: usize = 20;
    mss + UTP_HEADER as usize + UDP_HEADER as usize + IP_HEADER
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

// Remote's seq_nr == 1
// Our seq_nr == 101
fn make_test_vsock(opts: SocketOpts, is_incoming: bool) -> TestVsock {
    let env = MockUtpEnvironment::new();
    let args = if is_incoming {
        let remote_syn = UtpHeader {
            htype: ST_SYN,
            ..Default::default()
        };
        StreamArgs::new_incoming(101.into(), &remote_syn)
    } else {
        let remote_ack = UtpHeader {
            htype: ST_STATE,
            seq_nr: 1.into(),
            ack_nr: 100.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        };
        let now = env.now();
        env.increment_now(Duration::from_secs(1));
        StreamArgs::new_outgoing(&remote_ack, now, env.now())
    };
    make_test_vsock_args(opts, args, env)
}
