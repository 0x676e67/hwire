//! Compio runtime integration for wreq.

pub mod future;
pub mod io;

use std::{
    fmt::Debug,
    future::Future,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    pin::Pin,
    task::{Context, Poll, ready},
    time::{Duration, Instant},
};
#[cfg(unix)]
use std::{path::Path, sync::Arc};

use compio::{
    io::{AsyncRead, AsyncWrite, util::Splittable},
    net::TcpSocket,
};
use futures_util::FutureExt;
use send_wrapper::SendWrapper;

use crate::{
    BoxError, Executor,
    compio::future::SendFuture,
    conn::{Connect, Connecting, ConnectionSocket},
    dns::{DnsResolver, Resolving},
    timer::{Sleep, Timer},
};

/// A [`compio`] connection wrapper
#[derive(Debug)]
pub struct CompioIO<T: Splittable> {
    io: io::CompioIO<T>,
    peer_addr: Option<SocketAddr>,
    local_addr: Option<SocketAddr>,
}

impl<S> tokio::io::AsyncRead for CompioIO<S>
where
    S: Splittable + 'static,
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    #[inline(always)]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        // Flush any buffered writes before reading. This is necessary
        // because code like hyper_util::rt::write_all (used by Tunnel
        // and SOCKS handshakes) and hyper's own body encoder may call
        // poll_write without poll_flush, leaving data buffered in
        // compio's AsyncWriteStream. Since HTTP/1.1 is half-duplex
        // (write then read), flushing here ensures the remote peer
        // receives our data before we wait for its response.
        // In HTTP/2 the stream is split, so this combined poll_read
        // is not called and concurrent reads/writes are unaffected.
        ready!(tokio::io::AsyncWrite::poll_flush(self.as_mut(), cx))?;
        Pin::new(&mut self.get_mut().io).poll_read(cx, buf)
    }
}

impl<S> tokio::io::AsyncWrite for CompioIO<S>
where
    S: Splittable + 'static,
    S::ReadHalf: AsyncRead + Unpin,
    S::WriteHalf: AsyncWrite + Unpin,
{
    #[inline(always)]
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().io).poll_write(cx, buf)
    }

    #[inline(always)]
    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_flush(cx)
    }

    #[inline(always)]
    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().io).poll_shutdown(cx)
    }
}

impl ConnectionSocket for CompioIO<compio::net::TcpStream> {
    #[inline(always)]
    fn local_addr(&self) -> Option<SocketAddr> {
        self.local_addr
    }

    #[inline(always)]
    fn peer_addr(&self) -> Option<SocketAddr> {
        self.peer_addr
    }

    #[inline(always)]
    fn set_nodelay(&self, _: bool) -> Result<(), BoxError> {
        Ok(())
    }
}

#[cfg(unix)]
impl ConnectionSocket for CompioIO<compio::net::UnixStream> {
    #[inline(always)]
    fn local_addr(&self) -> Option<SocketAddr> {
        None
    }

    #[inline(always)]
    fn peer_addr(&self) -> Option<SocketAddr> {
        None
    }

    #[inline(always)]
    fn set_nodelay(&self, _: bool) -> Result<(), BoxError> {
        Ok(())
    }
}

/// A runtime adapter backed by `compio`.
#[derive(Default, Clone, Debug)]
pub struct CompioRuntime {
    _priv: (),
}

impl CompioRuntime {
    /// Create a new [`CompioRuntime`].
    #[inline(always)]
    pub fn new() -> Self {
        Self { _priv: () }
    }
}

impl<Fut> Executor<Fut> for CompioRuntime
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    #[inline(always)]
    fn execute(&self, fut: Fut) {
        compio::runtime::spawn(fut).detach();
    }
}

impl DnsResolver for CompioRuntime {
    #[inline(always)]
    fn resolve(&self, name: Box<str>) -> Resolving {
        Box::pin(async move {
            compio::runtime::spawn_blocking(move || {
                (name.as_ref(), 0)
                    .to_socket_addrs()
                    .map(|iter| Box::new(iter) as _)
            })
            .await
            .map_err(std::io::Error::from)?
            .map_err(Into::into)
        })
    }
}

impl Connect for CompioRuntime {
    #[inline(always)]
    fn tcp_connect(&self, socket: TcpStream, addr: SocketAddr) -> Connecting {
        SendFuture::new(async move {
            TcpSocket::from_std_stream(socket)?
                .connect(addr)
                .await
                .map(|stream| CompioIO {
                    peer_addr: stream.peer_addr().ok(),
                    local_addr: stream.local_addr().ok(),
                    io: io::CompioIO::new(stream),
                })
                .map(|stream| Box::new(stream) as _)
                .map_err(Into::into)
        })
        .boxed()
    }

    #[cfg(unix)]
    #[inline(always)]
    fn unix_connect(&self, path: Arc<Path>) -> Connecting {
        SendFuture::new(async move {
            compio::net::UnixStream::connect(path)
                .await
                .map(|stream| CompioIO {
                    peer_addr: None,
                    local_addr: None,
                    io: io::CompioIO::new(stream),
                })
                .map(|stream| Box::new(stream) as _)
                .map_err(Into::into)
        })
        .boxed()
    }
}

/// A sleep future wrapping a compio timer via SendWrapper.
///
/// compio futures are `!Send` (thread-per-core), so we wrap in `SendWrapper`
/// to satisfy wreq's `Send` bounds.
struct CompioSleep(SendWrapper<Pin<Box<dyn Future<Output = ()>>>>);

impl CompioSleep {
    #[inline(always)]
    fn new<Fut>(fut: Fut) -> Self
    where
        Fut: Future<Output = ()> + 'static,
    {
        CompioSleep(SendWrapper::new(Box::pin(fut)))
    }
}

impl Timer for CompioRuntime {
    #[inline(always)]
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        Box::pin(CompioSleep::new(compio::time::sleep(duration)))
    }

    #[inline(always)]
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        Box::pin(CompioSleep::new(compio::time::sleep_until(deadline)))
    }

    #[inline(always)]
    fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
        sleep.as_mut().reset(new_deadline);
    }
}

impl Future for CompioSleep {
    type Output = ();

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.get_mut().0.as_mut().poll(cx)
    }
}

impl Sleep for CompioSleep {
    #[inline(always)]
    fn reset(self: Pin<&mut Self>, deadline: Instant) {
        self.get_mut().0 = SendWrapper::new(Box::pin(compio::time::sleep_until(deadline)));
    }
}

impl Debug for CompioSleep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CompioSleep").finish()
    }
}
