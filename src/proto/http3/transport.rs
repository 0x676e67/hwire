use std::task::{ready, Context, Poll};

use bytes::Buf;
use http3::quic::WriteBuf;

use crate::rt::quic;

#[derive(Clone)]
pub(crate) struct Transport<T>(pub(crate) T);

pub(crate) struct Stream<T, B> {
    inner: T,
    pending: Option<WriteBuf<B>>,
}

fn closed() -> http3::quic::ConnectionErrorIncoming {
    http3::quic::ConnectionErrorIncoming::ApplicationClose {
        error_code: http3::error::Code::H3_NO_ERROR.value(),
    }
}

fn contract_error(reason: &str) -> http3::quic::StreamErrorIncoming {
    http3::quic::StreamErrorIncoming::ConnectionErrorIncoming {
        connection_error: http3::quic::ConnectionErrorIncoming::InternalError(reason.into()),
    }
}

// ===== impl Transport =====

impl<B: Buf, Q: quic::Connection<B>> http3::quic::Connection<B> for Transport<Q> {
    type RecvStream = Stream<Q::RecvStream, B>;

    type OpenStreams = Transport<Q::OpenStreams>;

    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::RecvStream, http3::quic::ConnectionErrorIncoming>> {
        self.0
            .poll_accept_recv(cx)
            .map(|res| res.and_then(|stream| stream.map(Stream::new).ok_or_else(closed)))
    }

    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, http3::quic::ConnectionErrorIncoming>> {
        self.0
            .poll_accept_bidi(cx)
            .map(|res| res.and_then(|stream| stream.map(Stream::new).ok_or_else(closed)))
    }

    fn opener(&self) -> Self::OpenStreams {
        Transport(self.0.opener())
    }
}

impl<B: Buf, Q: quic::OpenStreams<B>> http3::quic::OpenStreams<B> for Transport<Q> {
    type SendStream = Stream<Q::SendStream, B>;

    type BidiStream = Stream<Q::BidiStream, B>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, http3::quic::StreamErrorIncoming>> {
        self.0.poll_open_bidi(cx).map_ok(Stream::new)
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, http3::quic::StreamErrorIncoming>> {
        self.0.poll_open_send(cx).map_ok(Stream::new)
    }

    fn close(&mut self, code: http3::error::Code, reason: &[u8]) {
        self.0.close(code.value(), reason);
    }
}

// ===== impl Stream =====

impl<T, B> Stream<T, B> {
    fn new(inner: T) -> Self {
        Self {
            inner,
            pending: None,
        }
    }
}

impl<T: quic::SendStream<B>, B: Buf> http3::quic::SendStream<B> for Stream<T, B> {
    fn poll_ready(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), http3::quic::StreamErrorIncoming>> {
        if let Some(buf) = self.pending.as_mut() {
            let mut budget = 0;
            while buf.has_remaining() {
                let before = buf.remaining();
                let written = ready!(self.inner.poll_send(cx, buf))?;
                if written == 0 || before.checked_sub(buf.remaining()) != Some(written) {
                    return Poll::Ready(Err(contract_error(
                        "QUIC send did not advance its buffer consistently",
                    )));
                }
                budget += 1;
                if budget == 32 && buf.has_remaining() {
                    cx.waker().wake_by_ref();
                    return Poll::Pending;
                }
            }
        }
        self.pending = None;
        Poll::Ready(Ok(()))
    }

    fn send_data<D: Into<WriteBuf<B>>>(
        &mut self,
        data: D,
    ) -> Result<(), http3::quic::StreamErrorIncoming> {
        if self.pending.is_some() {
            return Err(contract_error("send_data called without QUIC readiness"));
        }
        self.pending = Some(data.into());
        Ok(())
    }

    fn poll_finish(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), http3::quic::StreamErrorIncoming>> {
        ready!(http3::quic::SendStream::poll_ready(self, cx))?;
        self.inner.poll_finish(cx)
    }

    fn poll_stopped(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<u64>, http3::quic::StreamErrorIncoming>> {
        self.inner.poll_stopped(cx)
    }

    fn reset(&mut self, code: u64) {
        self.pending = None;
        self.inner.reset(code);
    }

    fn send_id(&self) -> http3::quic::StreamId {
        self.inner.send_id()
    }
}

impl<T: quic::SendStream<B>, B: Buf> http3::quic::SendStreamUnframed<B> for Stream<T, B> {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut D,
    ) -> Poll<Result<usize, http3::quic::StreamErrorIncoming>> {
        ready!(http3::quic::SendStream::poll_ready(self, cx))?;
        self.inner.poll_send(cx, buf)
    }
}

impl<T: quic::RecvStream, B: Buf> http3::quic::RecvStream for Stream<T, B> {
    type Buf = T::Buf;

    fn poll_data(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::Buf>, http3::quic::StreamErrorIncoming>> {
        self.inner.poll_data(cx)
    }

    fn stop_sending(&mut self, code: u64) {
        self.inner.stop_sending(code);
    }

    fn recv_id(&self) -> http3::quic::StreamId {
        self.inner.recv_id()
    }
}

impl<T: quic::BidiStream<B>, B: Buf> http3::quic::BidiStream<B> for Stream<T, B> {
    type SendStream = Stream<T::SendStream, B>;

    type RecvStream = Stream<T::RecvStream, B>;

    fn split(self) -> (Self::SendStream, Self::RecvStream) {
        let (send, recv) = self.inner.split();
        (
            Stream {
                inner: send,
                pending: self.pending,
            },
            Stream::new(recv),
        )
    }
}
