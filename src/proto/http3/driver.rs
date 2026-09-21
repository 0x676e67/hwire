//! Background task of an HTTP/3 connection: control streams, SETTINGS,
//! Datagram routing, and the drain that closes the QUIC connection once
//! every exchange is done.

use std::{
    borrow::Cow,
    future::Future,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use http3::{error::Code, ConnectionState};
use tokio::sync::oneshot;

#[cfg(feature = "http3-datagram")]
use super::datagram::Drive;
use super::{shared::Shared, transport::Transport};
use crate::{rt::quic, Error, Result};

/// The connection task the executor runs: drives control streams until the
/// connection closes, then reports the outcome to the handle. Dropping it
/// unfinished terminates the connection, so an executor that discards it
/// cannot leave requests waiting.
pub struct ConnTask<Q: quic::Connection<Bytes>> {
    #[cfg(feature = "http3-datagram")]
    datagrams: Option<Box<dyn Drive>>,
    driver: http3::client::Connection<Transport<Q>, Bytes>,
    /// Keeps the protocol layer open while request handles come and go.
    _sender: http3::client::SendRequest<Transport<Q::OpenStreams>, Bytes>,
    opener: Q::OpenStreams,
    shared: Arc<Shared>,
    done: Option<oneshot::Sender<Result<()>>>,
}

/// Nothing here is ever pinned: the protocol layer and the QUIC traits take
/// `&mut self`, so the task is `Unpin` whatever the backend is.
impl<Q: quic::Connection<Bytes>> Unpin for ConnTask<Q> {}

// ===== impl ConnTask =====

impl<Q: quic::Connection<Bytes>> ConnTask<Q> {
    /// Wraps the protocol driver and the Datagram driver; `done` reports the
    /// outcome to the handle.
    pub(crate) fn new(
        driver: http3::client::Connection<Transport<Q>, Bytes>,
        sender: http3::client::SendRequest<Transport<Q::OpenStreams>, Bytes>,
        opener: Q::OpenStreams,
        #[cfg(feature = "http3-datagram")] datagrams: Option<Box<dyn Drive>>,
        shared: Arc<Shared>,
        done: oneshot::Sender<Result<()>>,
    ) -> Self {
        Self {
            #[cfg(feature = "http3-datagram")]
            datagrams,
            driver,
            _sender: sender,
            opener,
            shared,
            done: Some(done),
        }
    }

    /// Reports the outcome; the handle may already be gone.
    fn finish(&mut self, result: Result<()>) -> Poll<()> {
        if let Some(done) = self.done.take() {
            let _ = done.send(result);
        }
        Poll::Ready(())
    }
}

impl<Q: quic::Connection<Bytes>> Future for ConnTask<Q> {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = self.get_mut();
        if this.done.is_none() {
            return Poll::Ready(());
        }
        this.shared.register(cx);
        if let Poll::Ready(error) = this.driver.poll_close(cx) {
            let normal = error.is_h3_no_error();
            this.shared.terminate(Error::new_h3(error));
            let result = if normal {
                Ok(())
            } else {
                Err(this.shared.error())
            };
            return this.finish(result);
        }
        if this.shared.peer_extended_connect.get().is_none() {
            // Borrowed settings come from the received SETTINGS frame; Owned
            // values are protocol defaults before peer negotiation completes.
            if let Cow::Borrowed(settings) = this.driver.settings() {
                #[cfg(feature = "http3-datagram")]
                if let Some(datagrams) = &this.shared.datagrams {
                    datagrams.negotiated(settings.enable_datagram());
                }
                let _ = this
                    .shared
                    .peer_extended_connect
                    .set(settings.enable_extended_connect());
                this.shared.settings_ready.cancel();
            }
        }
        #[cfg(feature = "http3-datagram")]
        if let Some(datagrams) = &mut this.datagrams {
            if let Poll::Ready(result) = datagrams.poll(cx) {
                this.datagrams = None;
                if let Err((code, error)) = result {
                    // Publish the cause before transport close wakes exchanges.
                    this.shared.terminate(error);
                    quic::OpenStreams::close(
                        &mut this.opener,
                        code.value(),
                        b"HTTP Datagram driver failed",
                    );
                    let error = this.shared.error();
                    return this.finish(Err(error));
                }
                if let Some(registry) = &this.shared.datagrams {
                    registry.close();
                }
            }
        }
        if ConnectionState::is_closing(&this.driver) {
            this.shared.shutdown();
        }
        // The waker is registered above, so a completion racing this check
        // still wakes the task.
        if this.shared.is_closed() && this.shared.is_idle() {
            quic::OpenStreams::close(&mut this.opener, Code::H3_NO_ERROR.value(), b"");
            this.shared.terminate(Error::new_closed());
            return this.finish(Ok(()));
        }
        Poll::Pending
    }
}

impl<Q: quic::Connection<Bytes>> Drop for ConnTask<Q> {
    fn drop(&mut self) {
        if self.done.is_some() {
            self.shared
                .terminate(Error::new_canceled().with("HTTP/3 connection task dropped"));
            quic::OpenStreams::close(
                &mut self.opener,
                Code::H3_NO_ERROR.value(),
                b"connection task dropped",
            );
            let error = self.shared.error();
            let _ = self.finish(Err(error));
        }
    }
}
