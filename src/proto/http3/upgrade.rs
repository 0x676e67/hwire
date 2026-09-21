//! CONNECT tunnels over HTTP/3 request streams.

#[cfg(feature = "http3-datagram")]
use std::sync::Arc;
use std::{
    future::{poll_fn, Future},
    io,
    pin::{pin, Pin},
    task::{ready, Context, Poll},
};

use bytes::{Buf, Bytes};
use futures_util::future::{try_join, BoxFuture};
use http::Response;
use http3::quic;
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{mpsc, oneshot},
};
use tokio_util::sync::{CancellationToken, PollSender};

use super::{
    client::{cooperate, invalid_datagram, RecvGuard, SendGuard, CHUNK},
    shared::Active,
};
use crate::{
    body::{chan, Incoming},
    rt::Executor,
    upgrade::{pending, Upgraded},
    Error, Result,
};
#[cfg(feature = "http3-datagram")]
use crate::{
    conn::http3::datagram::Pending,
    proto::http3::datagram::{Registration, RequestState},
};

/// Write commands from the tunnel to its pump task; acknowledgments carry
/// the transport result back.
enum Write {
    Data(Bytes),
    Flush(oneshot::Sender<Result<()>>),
    Finish(oneshot::Sender<Result<()>>),
}

/// Tunnel I/O handed to the application through `Upgraded`. Reads drain the
/// pump task's channel; writes are forwarded to it one chunk at a time.
struct Io {
    rx: chan::Receiver,
    data: Bytes,
    sender: PollSender<Write>,
    pending: Option<(bool, oneshot::Receiver<Result<()>>)>,
    shutdown: bool,
    read_closed: bool,
    #[cfg(feature = "http3-datagram")]
    datagrams: Option<Arc<RequestState>>,
}

/// Datagram semantics and the registration owned by a CONNECT tunnel task.
#[cfg(feature = "http3-datagram")]
pub(super) enum TunnelDatagrams {
    Disabled,
    Ordinary(Registration),
    Datagram(Registration),
}

/// Turns a successful CONNECT into an upgraded tunnel. Both stream directions
/// move to an executor task that reads eagerly, so peer resets surface even
/// while the tunnel is idle.
pub(super) fn tunnel<S, R, E>(
    send: SendGuard<S>,
    recv: RecvGuard<R>,
    mut headers: Response<()>,
    active: Active,
    #[cfg(feature = "http3-datagram")] datagrams: TunnelDatagrams,
    exec: &E,
) -> Response<Incoming>
where
    S: quic::SendStream<Bytes> + Send + 'static,
    R: quic::RecvStream + Send + 'static,
    E: Executor<BoxFuture<'static, ()>>,
{
    #[cfg(feature = "http3-datagram")]
    let (registration, datagrams) = match datagrams {
        TunnelDatagrams::Disabled => (None, None),
        TunnelDatagrams::Ordinary(registration) => (Some(registration), None),
        TunnelDatagrams::Datagram(registration) => {
            let state = registration.0.clone();
            (Some(registration), Some(state))
        }
    };
    #[cfg(feature = "http3-datagram")]
    let invalid = registration
        .as_ref()
        .map(|registration| registration.0.invalid.clone());
    #[cfg(not(feature = "http3-datagram"))]
    let invalid: Option<CancellationToken> = None;
    let (body, rx) = chan::channel(false);
    let (tx, writes) = mpsc::channel(1);
    let io = Io {
        rx,
        data: Bytes::new(),
        sender: PollSender::new(tx),
        pending: None,
        shutdown: false,
        read_closed: false,
        #[cfg(feature = "http3-datagram")]
        datagrams: datagrams.clone(),
    };
    let (pending, on_upgrade) = pending();
    let io = Upgraded::new(io, Bytes::new());
    #[cfg(feature = "http3-datagram")]
    let io = if let Some(datagrams) = datagrams {
        headers.extensions_mut().insert(Pending::new(io, datagrams));
        None
    } else {
        Some(io)
    };
    #[cfg(not(feature = "http3-datagram"))]
    let io = Some(io);
    if io.is_some() {
        headers.extensions_mut().insert(on_upgrade);
    }
    *headers.version_mut() = http::Version::HTTP_3;
    exec.execute(Box::pin(run(
        send,
        recv,
        body,
        writes,
        active,
        invalid,
        #[cfg(feature = "http3-datagram")]
        registration,
    )));
    if let Some(io) = io {
        pending.fulfill(io);
    }
    headers.map(|()| Incoming::empty())
}

/// Pumps both directions until the tunnel closes or fails, then reports the
/// failure to the reader.
async fn run<S, R>(
    send: SendGuard<S>,
    recv: RecvGuard<R>,
    body: chan::Sender,
    writes: mpsc::Receiver<Write>,
    active: Active,
    invalid: Option<CancellationToken>,
    #[cfg(feature = "http3-datagram")] registration: Option<Registration>,
) where
    S: quic::SendStream<Bytes>,
    R: quic::RecvStream,
{
    let mut body = Some(body);
    let result = {
        let mut invalid_watch = pin!(invalid_datagram(invalid));
        let mut transfer = pin!(try_join(upload(send, writes), download(recv, &mut body)));
        poll_fn(|cx| {
            if let Poll::Ready(error) = invalid_watch.as_mut().poll(cx) {
                return Poll::Ready(Err(error));
            }
            transfer.as_mut().poll(cx)
        })
        .await
    };
    if let (Err(error), Some(mut body)) = (result, body) {
        body.send_error(active.shared().error_or(error));
    }
    #[cfg(feature = "http3-datagram")]
    drop(registration);
    drop(active);
}

/// Writes tunnel data until the application shuts down or the peer stops.
async fn upload<S: quic::SendStream<Bytes>>(
    mut send: SendGuard<S>,
    mut rx: mpsc::Receiver<Write>,
) -> Result<()> {
    let mut budget = 0;
    let mut stopped = false;
    while let Some(write) = poll_fn(|cx| {
        if let Poll::Ready(result) = send.stream.poll_stopped(cx) {
            stopped = true;
            return Poll::Ready(result.map(|_| None).map_err(Error::new_h3));
        }
        rx.poll_recv(cx).map(Ok)
    })
    .await?
    {
        match write {
            Write::Data(data) => send.stream.send_data(data).await.map_err(Error::new_h3)?,
            Write::Flush(ack) => {
                let _ = ack.send(Ok(()));
            }
            Write::Finish(ack) => {
                #[cfg(feature = "http3-datagram")]
                if let Some(datagrams) = &send.datagrams {
                    datagrams.close_send();
                }
                send.stream.finish().await.map_err(Error::new_h3)?;
                send.finished = true;
                let _ = ack.send(Ok(()));
                // Acknowledge the local shutdown immediately, but keep drain
                // waiting until QUIC confirms FIN or the peer stops receiving.
                return poll_fn(|cx| send.stream.poll_stopped(cx))
                    .await
                    .map(|_| ())
                    .map_err(Error::new_h3);
            }
        }
        cooperate(&mut budget).await;
    }
    if stopped {
        #[cfg(feature = "http3-datagram")]
        if let Some(datagrams) = &send.datagrams {
            datagrams.close_send();
        }
        Ok(())
    } else {
        Err(Error::new_canceled())
    }
}

/// Forwards received data to the tunnel reader and closes it at FIN.
async fn download<R: quic::RecvStream>(
    mut recv: RecvGuard<R>,
    body: &mut Option<chan::Sender>,
) -> Result<()> {
    let Some(sender) = body.as_mut() else {
        return Err(Error::new_canceled());
    };
    let mut budget = 0;
    while let Some(mut data) = poll_fn(|cx| {
        if sender.poll_closed(cx).is_ready() {
            return Poll::Ready(Err(Error::new_canceled()));
        }
        recv.stream.poll_recv_data(cx).map_err(Error::new_h3)
    })
    .await?
    {
        while data.has_remaining() {
            poll_fn(|cx| sender.poll_ready(cx)).await?;
            let size = data.remaining().min(CHUNK);
            sender
                .send_data(data.copy_to_bytes(size))
                .map_err(|_| Error::new_canceled())?;
            cooperate(&mut budget).await;
        }
        cooperate(&mut budget).await;
    }
    poll_fn(|cx| {
        if sender.poll_closed(cx).is_ready() {
            return Poll::Ready(Err(Error::new_canceled()));
        }
        recv.stream.poll_recv_trailers(cx).map_err(Error::new_h3)
    })
    .await?;
    recv.finished = true;
    #[cfg(feature = "http3-datagram")]
    if let Some(datagrams) = &recv.datagrams {
        datagrams.close_recv();
    }
    // Dropping the sender delivers EOF to the tunnel reader.
    body.take();
    Ok(())
}

/// The error a closed tunnel reports to the application.
fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "HTTP/3 tunnel closed")
}

// ===== impl Io =====

impl Io {
    /// Waits for the pump task to acknowledge the pending flush or finish.
    fn poll_ack(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if let Some((finish, ack)) = self.pending.as_mut() {
            let result = ready!(Pin::new(ack).poll(cx));
            let finish = *finish;
            self.pending = None;
            result.map_err(|_| closed())?.map_err(io::Error::other)?;
            self.shutdown |= finish;
        }
        Poll::Ready(Ok(()))
    }

    /// Sends a flush or finish to the pump task and waits for its acknowledgment;
    /// a finish requested behind a pending flush follows once that flush completes.
    fn poll_barrier(&mut self, cx: &mut Context<'_>, finish: bool) -> Poll<io::Result<()>> {
        if self.pending.is_some() {
            let pending_finish = self.pending.as_ref().is_some_and(|(finish, _)| *finish);
            ready!(self.poll_ack(cx))?;
            if !finish || pending_finish {
                return Poll::Ready(Ok(()));
            }
        }
        if self.shutdown {
            return Poll::Ready(Ok(()));
        }
        ready!(self.sender.poll_reserve(cx)).map_err(|_| closed())?;
        let (tx, rx) = oneshot::channel();
        self.sender
            .send_item(if finish {
                Write::Finish(tx)
            } else {
                Write::Flush(tx)
            })
            .map_err(|_| closed())?;
        self.pending = Some((finish, rx));
        self.poll_ack(cx)
    }
}

impl AsyncRead for Io {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        if buf.remaining() == 0 {
            return Poll::Ready(Ok(()));
        }
        loop {
            if !self.data.is_empty() {
                let size = buf.remaining().min(self.data.len());
                buf.put_slice(&self.data[..size]);
                self.data.advance(size);
                return Poll::Ready(Ok(()));
            }
            if self.read_closed {
                return Poll::Ready(Ok(()));
            }
            match ready!(self.rx.poll_next(cx)) {
                Some(Ok(data)) => self.data = data,
                Some(Err(error)) => {
                    self.read_closed = true;
                    return Poll::Ready(Err(io::Error::other(error)));
                }
                None => {
                    self.read_closed = true;
                    return Poll::Ready(Ok(()));
                }
            }
        }
    }
}

impl AsyncWrite for Io {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        ready!(self.poll_ack(cx))?;
        if self.shutdown {
            return Poll::Ready(Err(closed()));
        }
        if buf.is_empty() {
            return Poll::Ready(Ok(0));
        }
        ready!(self.sender.poll_reserve(cx)).map_err(|_| closed())?;
        let size = buf.len().min(CHUNK);
        self.sender
            .send_item(Write::Data(Bytes::copy_from_slice(&buf[..size])))
            .map_err(|_| closed())?;
        Poll::Ready(Ok(size))
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_barrier(cx, false)
    }

    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_barrier(cx, true)
    }
}

#[cfg(feature = "http3-datagram")]
impl Drop for Io {
    fn drop(&mut self) {
        if let Some(datagrams) = &self.datagrams {
            datagrams.close();
        }
    }
}
