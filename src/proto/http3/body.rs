use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{ready, Context, Poll},
};

use bytes::{Buf, Bytes};
use http3::{error::Code, quic};
use http_body::Frame;
use tokio::sync::oneshot;

use super::client::{consume_length, Failure, RecvGuard};
use crate::{Error, Result};

/// Drives a split response stream without exposing the QUIC backend in Incoming.
pub(crate) trait RecvBody: Send + Sync {
    fn poll_frame(&self, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>>>>;

    fn cancel(&self);
}

/// Owns the response direction until EOF, failure, or body cancellation.
/// Completion releases the exchange's receive-side wait; upload remains independent.
struct Receiving<S: quic::RecvStream> {
    recv: Option<RecvGuard<S>>,
    remaining: Option<u64>,
    data_done: bool,
    canceled: bool,
    failure: Arc<Failure>,
    completed: Option<oneshot::Sender<Result<()>>>,
}

/// Keeps cancellation independent of whether the caller polls the response body.
/// Dropping the exchange closes its receive direction and releases QPACK state.
pub(super) struct Completion {
    recv: Arc<dyn RecvBody>,
    completed: oneshot::Receiver<Result<()>>,
}

pub(super) fn incoming<S>(
    recv: RecvGuard<S>,
    remaining: Option<u64>,
    failure: Arc<Failure>,
) -> (crate::body::Incoming, Completion)
where
    S: quic::RecvStream + Send + 'static,
{
    let (completed, completion) = oneshot::channel();
    let recv = Receiving {
        recv: Some(recv),
        remaining,
        data_done: false,
        canceled: false,
        failure,
        completed: Some(completed),
    };
    // Only Incoming reads DATA. The exchange also needs access to stop an idle
    // body on cancellation, without requiring the application to poll again.
    let recv: Arc<dyn RecvBody> = Arc::new(Mutex::new(recv));
    let incoming = crate::body::Incoming::h3(recv.clone());
    (
        incoming,
        Completion {
            recv,
            completed: completion,
        },
    )
}

// ===== impl Receiving =====

impl<S: quic::RecvStream> Receiving<S> {
    fn poll_frame(&mut self, cx: &mut Context<'_>) -> Poll<Result<Option<Frame<Bytes>>>> {
        let Some(recv) = self.recv.as_mut() else {
            return Poll::Ready(if self.canceled {
                Err(self.failure.get())
            } else {
                Ok(None)
            });
        };
        if let Some(error) = self.failure.poll_error(cx) {
            return Poll::Ready(Err(error));
        }
        if !self.data_done {
            for _ in 0..32 {
                match ready!(recv.stream.poll_recv_data(cx)).map_err(Error::new_h3)? {
                    Some(mut data) => {
                        let size = data.remaining();
                        consume_length(&mut self.remaining, size).map_err(|reason| {
                            recv.code = Code::H3_MESSAGE_ERROR;
                            Error::new_h3(reason)
                        })?;
                        if size != 0 {
                            return Poll::Ready(Ok(Some(Frame::data(data.copy_to_bytes(size)))));
                        }
                    }
                    None => {
                        self.data_done = true;
                        if self.remaining.is_some_and(|n| n != 0) {
                            recv.code = Code::H3_MESSAGE_ERROR;
                            return Poll::Ready(Err(Error::new_body(
                                "HTTP/3 body shorter than content-length",
                            )));
                        }
                        break;
                    }
                }
            }
            if !self.data_done {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
        }
        let trailers = ready!(recv.stream.poll_recv_trailers(cx)).map_err(Error::new_h3)?;
        Poll::Ready(Ok(trailers.map(Frame::trailers)))
    }

    fn finish(&mut self, result: Result<()>) {
        self.canceled = result.is_err();
        if result.is_ok() {
            if let Some(recv) = self.recv.as_mut() {
                recv.finished = true;
                #[cfg(feature = "http3-datagram")]
                if let Some(datagrams) = &recv.datagrams {
                    datagrams.close_recv();
                }
            }
        }
        self.recv.take();
        if let Some(completed) = self.completed.take() {
            let _ = completed.send(result);
        }
    }
}

impl<S: quic::RecvStream + Send> RecvBody for Mutex<Receiving<S>> {
    fn poll_frame(&self, cx: &mut Context<'_>) -> Poll<Option<Result<Frame<Bytes>>>> {
        let mut this = self.lock().unwrap_or_else(|error| error.into_inner());
        match ready!(this.poll_frame(cx)) {
            Ok(frame) => {
                if frame.as_ref().is_none_or(Frame::is_trailers) {
                    this.finish(Ok(()));
                }
                Poll::Ready(frame.map(Ok))
            }
            Err(error) => {
                this.failure.set(error);
                let error = this.failure.get();
                this.finish(Err(error));
                Poll::Ready(Some(Err(this.failure.get())))
            }
        }
    }

    fn cancel(&self) {
        let mut this = self.lock().unwrap_or_else(|error| error.into_inner());
        if this.completed.is_some() {
            let error = this.failure.get();
            this.finish(Err(error));
        }
    }
}

impl<S: quic::RecvStream> Drop for Receiving<S> {
    fn drop(&mut self) {
        if self.completed.is_some() {
            self.finish(Err(self.failure.get()));
        }
    }
}

// ===== impl Completion =====

impl Future for Completion {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.completed)
            .poll(cx)
            .map(|result| result.unwrap_or_else(|_| Err(Error::new_canceled())))
    }
}

impl Drop for Completion {
    fn drop(&mut self) {
        self.recv.cancel();
    }
}
