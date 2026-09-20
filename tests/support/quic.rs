//! Test transport over an externally established native QUIC connection.
//!
//! Streams and openers come from `http3-quic`; this module only bridges its
//! `http3::quic` implementation to the `wreq_proto::rt::quic` contract.
#![allow(dead_code)]

use std::task::{Context, Poll};

use ::quic as backend;
use bytes::Buf;
use http3::{error::Code, quic as h3};
use wreq_proto::rt::quic::{self as rt, ConnectionError, StreamError, StreamId};
#[cfg(feature = "http3-datagram")]
#[path = "quic/datagram.rs"]
mod datagram;

/// Owns the incoming streams of one established QUIC connection.
/// Use one adapter per HTTP/3 connection and keep other stream readers inactive.
pub struct Connection {
    inner: http3_quic::Connection,
    #[cfg(feature = "http3-datagram")]
    connection: backend::Connection,
    #[cfg(feature = "http3-datagram")]
    datagrams_taken: bool,
}

/// Bridges an `http3::quic` opener or stream to the `rt::quic` traits.
#[derive(Clone)]
pub struct Bridge<T>(T);

// ===== impl Connection =====

impl Connection {
    /// Adapts a connection whose QUIC and TLS handshakes have completed.
    pub fn new(connection: backend::Connection) -> Self {
        Self {
            #[cfg(feature = "http3-datagram")]
            connection: connection.clone(),
            #[cfg(feature = "http3-datagram")]
            datagrams_taken: false,
            inner: http3_quic::Connection::new(connection),
        }
    }
}

impl<B: Buf> rt::Connection<B> for Connection {
    type RecvStream = Bridge<http3_quic::RecvStream>;

    type OpenStreams = Bridge<http3_quic::OpenStreams>;

    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::RecvStream>, ConnectionError>> {
        h3::Connection::<B>::poll_accept_recv(&mut self.inner, cx)
            .map_ok(|stream| Some(Bridge(stream)))
    }

    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::BidiStream>, ConnectionError>> {
        h3::Connection::<B>::poll_accept_bidi(&mut self.inner, cx)
            .map_ok(|stream| Some(Bridge(stream)))
    }

    fn opener(&self) -> Self::OpenStreams {
        Bridge(h3::Connection::<B>::opener(&self.inner))
    }
}

impl<B: Buf> rt::OpenStreams<B> for Connection {
    type SendStream = Bridge<http3_quic::SendStream<B>>;

    type BidiStream = Bridge<http3_quic::BidiStream<B>>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamError>> {
        h3::OpenStreams::<B>::poll_open_bidi(&mut self.inner, cx).map_ok(Bridge)
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamError>> {
        h3::OpenStreams::<B>::poll_open_send(&mut self.inner, cx).map_ok(Bridge)
    }

    fn close(&mut self, code: u64, reason: &[u8]) {
        h3::OpenStreams::<B>::close(&mut self.inner, Code::from(code), reason);
    }
}

// ===== impl Bridge =====

impl<B: Buf, T: h3::OpenStreams<B>> rt::OpenStreams<B> for Bridge<T>
where
    T::SendStream: h3::SendStreamUnframed<B>,
    T::BidiStream: h3::BidiStream<B> + h3::SendStreamUnframed<B>,
    <T::BidiStream as h3::BidiStream<B>>::SendStream: h3::SendStreamUnframed<B>,
{
    type SendStream = Bridge<T::SendStream>;

    type BidiStream = Bridge<T::BidiStream>;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, StreamError>> {
        self.0.poll_open_bidi(cx).map_ok(Bridge)
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, StreamError>> {
        self.0.poll_open_send(cx).map_ok(Bridge)
    }

    fn close(&mut self, code: u64, reason: &[u8]) {
        self.0.close(Code::from(code), reason);
    }
}

impl<B: Buf, T: h3::SendStreamUnframed<B>> rt::SendStream<B> for Bridge<T> {
    fn poll_send<D: Buf>(
        &mut self,
        cx: &mut Context<'_>,
        buf: &mut D,
    ) -> Poll<Result<usize, StreamError>> {
        self.0.poll_send(cx, buf)
    }

    fn poll_stopped(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<u64>, StreamError>> {
        self.0.poll_stopped(cx)
    }

    fn poll_finish(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), StreamError>> {
        self.0.poll_finish(cx)
    }

    fn reset(&mut self, code: u64) {
        self.0.reset(code);
    }

    fn send_id(&self) -> StreamId {
        self.0.send_id()
    }
}

impl<T: h3::RecvStream> rt::RecvStream for Bridge<T> {
    type Buf = T::Buf;

    fn poll_data(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Self::Buf>, StreamError>> {
        self.0.poll_data(cx)
    }

    fn stop_sending(&mut self, code: u64) {
        self.0.stop_sending(code);
    }

    fn recv_id(&self) -> StreamId {
        self.0.recv_id()
    }
}

impl<B: Buf, T> rt::BidiStream<B> for Bridge<T>
where
    T: h3::BidiStream<B> + h3::SendStreamUnframed<B>,
    T::SendStream: h3::SendStreamUnframed<B>,
{
    type SendStream = Bridge<T::SendStream>;

    type RecvStream = Bridge<T::RecvStream>;

    fn split(self) -> (Self::SendStream, Self::RecvStream) {
        let (send, recv) = self.0.split();
        (Bridge(send), Bridge(recv))
    }
}
