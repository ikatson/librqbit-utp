use std::{pin::Pin, task::Poll};

use tokio::{io::AsyncRead, sync::mpsc::Receiver};
use tracing::trace;

use crate::{message::UtpMessage, stream_dispatch::UserRxMessage};

pub struct UtpStreamReadHalf {
    rx: Receiver<UserRxMessage>,
    current: Option<UtpMessage>,
    offset: usize,
}

impl UtpStreamReadHalf {
    pub fn new(rx: Receiver<UserRxMessage>) -> Self {
        Self {
            rx,
            current: None,
            offset: 0,
        }
    }
}

impl AsyncRead for UtpStreamReadHalf {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let this = self.get_mut();

        let mut written = false;

        while buf.remaining() > 0 {
            // If there was a previous message we haven't read till the end, do it.
            if let Some(current) = this.current.as_ref() {
                let payload = &current.payload()[this.offset..];
                if payload.is_empty() {
                    return Poll::Ready(Err(std::io::Error::other(
                        "bug in UtpStreamReadHalf: payload is empty",
                    )));
                }

                let len = buf.remaining().min(payload.len());

                trace!(
                    seq_nr = ?current.header.seq_nr,
                    offset = this.offset,
                    len,
                    payload_len = current.payload().len(),
                    "reading from UtpMessage to user"
                );

                buf.put_slice(&payload[..len]);
                written = true;
                this.offset += len;
                if this.offset == current.payload().len() {
                    this.offset = 0;
                    this.current = None;
                }
            }

            match this.rx.poll_recv(cx) {
                Poll::Ready(Some(UserRxMessage::UtpMessage(msg))) => this.current = Some(msg),
                Poll::Ready(Some(UserRxMessage::Error(msg))) => {
                    return Poll::Ready(Err(std::io::Error::other(msg)))
                }
                Poll::Ready(Some(UserRxMessage::Eof)) => return Poll::Ready(Ok(())),

                Poll::Ready(None) => return Poll::Ready(Err(std::io::Error::other("socket died"))),
                Poll::Pending => break,
            };
        }

        if written {
            return Poll::Ready(Ok(()));
        }

        Poll::Pending
    }
}
