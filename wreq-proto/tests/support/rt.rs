#![allow(dead_code)]

//! Tokio runtime integration for core tests.
use std::{
    future::Future,
    net::{SocketAddr, TcpStream},
    pin::Pin,
    task::{Context, Poll},
    time::{Duration, Instant},
};

use pin_project_lite::pin_project;
use wreq_rt::{
    conn::{Connect, Connecting},
    dns::{DnsResolver, Resolving},
    timer::{Sleep, Timer},
    Executor,
};

/// A test runtime adapter backed by `tokio`.
#[non_exhaustive]
#[derive(Default, Debug, Clone)]
pub struct TokioRuntime {}

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
    fn execute(&self, fut: Fut) {
        tokio::spawn(fut);
    }
}

impl DnsResolver for TokioRuntime {
    fn resolve(&self, _: Box<str>) -> Resolving {
        unimplemented!()
    }
}

impl Connect for TokioRuntime {
    fn tcp_connect(&self, _: TcpStream, _: SocketAddr) -> Connecting {
        unimplemented!()
    }

    #[cfg(unix)]
    fn unix_connect(&self, _: std::sync::Arc<std::path::Path>) -> Connecting {
        unimplemented!()
    }
}

impl TokioRuntime {
    /// Create a new [`TokioRuntime`].
    pub fn new() -> Self {
        Self {}
    }
}

impl Timer for TokioRuntime {
    #[inline]
    fn sleep(&self, duration: Duration) -> Pin<Box<dyn Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep(duration),
        })
    }

    #[inline]
    fn sleep_until(&self, deadline: Instant) -> Pin<Box<dyn Sleep>> {
        Box::pin(TokioSleep {
            inner: tokio::time::sleep_until(deadline.into()),
        })
    }

    #[inline]
    fn now(&self) -> Instant {
        tokio::time::Instant::now().into()
    }

    #[inline]
    fn reset(&self, sleep: &mut Pin<Box<dyn Sleep>>, new_deadline: Instant) {
        sleep.as_mut().reset(new_deadline);
    }
}

// ===== impl TokioSleep =====

impl Future for TokioSleep {
    type Output = ();

    #[inline]
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.project().inner.poll(cx)
    }
}

impl Sleep for TokioSleep {
    #[inline]
    fn reset(self: Pin<&mut Self>, deadline: Instant) {
        self.project().inner.as_mut().reset(deadline.into());
    }
}
