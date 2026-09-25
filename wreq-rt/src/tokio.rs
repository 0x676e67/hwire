//! Tokio runtime integration for wreq.

use std::{
    future::Future,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};
#[cfg(unix)]
use std::{path::Path, sync::Arc};

use futures_util::{FutureExt, TryFutureExt};
use pin_project_lite::pin_project;
use tokio::net::TcpSocket;

use crate::{
    BoxError, Executor,
    conn::{Connect, Connecting, ConnectionSocket},
    dns::{DnsResolver, Resolving},
    timer::{Sleep, Timer},
};

/// A runtime adapter backed by `tokio`.
#[derive(Default, Clone, Debug)]
pub struct TokioRuntime {
    _priv: (),
}

// Use TokioSleep to get tokio::time::Sleep to implement Unpin.
// see https://docs.rs/tokio/latest/tokio/time/struct.Sleep.html
pin_project! {
    #[derive(Debug)]
    struct TokioSleep {
        #[pin]
        inner: tokio::time::Sleep,
    }
}

// ===== impl TokioRuntime =====

impl<Fut> Executor<Fut> for TokioRuntime
where
    Fut: Future + Send + 'static,
    Fut::Output: Send + 'static,
{
    #[inline(always)]
    fn execute(&self, fut: Fut) {
        tokio::spawn(fut);
    }
}

impl DnsResolver for TokioRuntime {
    #[inline(always)]
    fn resolve(&self, name: Box<str>) -> Resolving {
        Box::pin(async move {
            tokio::task::spawn_blocking(move || {
                (name.as_ref(), 0)
                    .to_socket_addrs()
                    .map(|iter| Box::new(iter) as _)
            })
            .await?
            .map_err(Into::into)
        })
    }
}

impl Connect for TokioRuntime {
    fn tcp_connect(&self, socket: TcpStream, addr: SocketAddr) -> Connecting {
        TcpSocket::from_std_stream(socket)
            .connect(addr)
            .map_ok(|s| Box::new(s) as _)
            .map_err(Into::into)
            .boxed()
    }

    #[cfg(unix)]
    #[inline(always)]
    fn unix_connect(&self, path: Arc<Path>) -> Connecting {
        tokio::net::UnixStream::connect(path)
            .map_ok(|s| Box::new(s) as _)
            .map_err(Into::into)
            .boxed()
    }
}

impl ConnectionSocket for tokio::net::TcpStream {
    #[inline(always)]
    fn local_addr(&self) -> Option<SocketAddr> {
        tokio::net::TcpStream::local_addr(self).ok()
    }

    #[inline(always)]
    fn peer_addr(&self) -> Option<SocketAddr> {
        tokio::net::TcpStream::peer_addr(self).ok()
    }

    #[inline(always)]
    fn set_nodelay(&self, nodelay: bool) -> Result<(), BoxError> {
        tokio::net::TcpStream::set_nodelay(self, nodelay).map_err(Into::into)
    }
}

#[cfg(unix)]
impl ConnectionSocket for tokio::net::UnixStream {
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

impl TokioRuntime {
    /// Create a new [`TokioRuntime`].
    #[inline(always)]
    pub fn new() -> Self {
        Self { _priv: () }
    }
}

impl Timer for TokioRuntime {
    #[inline(always)]
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep(duration),
        })
    }

    #[inline(always)]
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep_until(deadline.into()),
        })
    }

    #[inline(always)]
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into()
    }

    #[inline(always)]
    fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
        sleep.as_mut().reset(new_deadline);
    }
}

// ===== impl TokioSleep =====

impl Future for TokioSleep {
    type Output = ();

    #[inline(always)]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

impl Sleep for TokioSleep {
    #[inline(always)]
    fn reset(self: Pin<&mut Self>, deadline: Instant) {
        self.project().inner.as_mut().reset(deadline.into());
    }
}
