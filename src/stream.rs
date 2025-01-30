use std::{pin::Pin, task::Poll};

use tokio::io::{AsyncRead, AsyncWrite};

use crate::{stream_rx::UtpStreamReadHalf, stream_tx::UtpStreamWriteHalf};

pub type UtpStreamUdp = UtpStream;

pub struct UtpStream {
    reader: UtpStreamReadHalf,
    writer: UtpStreamWriteHalf,
}

impl UtpStream {
    pub(crate) fn new(reader: UtpStreamReadHalf, writer: UtpStreamWriteHalf) -> Self {
        Self { reader, writer }
    }

    pub fn split(
        self,
    ) -> (
        impl AsyncRead + Send + Sync + 'static,
        impl AsyncWrite + Send + Sync + 'static,
    ) {
        (self.reader, self.writer)
    }
}

impl AsyncRead for UtpStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for UtpStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, std::io::Error>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }

    fn poll_flush(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().writer).poll_flush(cx)
    }

    fn poll_shutdown(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Result<(), std::io::Error>> {
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}
