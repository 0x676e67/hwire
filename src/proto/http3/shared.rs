//! Connection state shared by the driver, request handles, bodies and uploads:
//! peer settings, local admission, the drain and the published connection error.

use std::{
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, OnceLock,
    },
    task::Context,
};

use futures_util::task::AtomicWaker;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[cfg(feature = "http3-datagram")]
use super::datagram::Registry;
use crate::{Error, Result};

/// Connection state shared by the driver, request handles, bodies and uploads.
pub(crate) struct Shared {
    #[cfg(feature = "http3-datagram")]
    pub(crate) datagrams: Option<Arc<Registry>>,
    pub(crate) peer_extended_connect: OnceLock<bool>,
    pub(crate) settings_ready: CancellationToken,
    pub(crate) draining: AtomicBool,
    /// Local admission; closed by a shutdown so waiting requests are returned.
    pub(crate) permits: Semaphore,
    /// Admitted exchanges: pending heads, unread bodies, uploads and tunnels.
    /// A permit the semaphore assigned to a waiter does not count until that
    /// request runs, so a shutdown can finish without it being polled.
    pub(crate) active: AtomicUsize,
    /// The driver's waker, for drain, shutdown and completion events.
    pub(crate) waker: AtomicWaker,
    pub(crate) error: OnceLock<Arc<Error>>,
}

/// Tracks request admission. Once admitted, the exchange holds its permit
/// until both directions finish, keeping graceful shutdown pending.
pub(crate) struct Active {
    shared: Arc<Shared>,
    permit: bool,
}

// ===== impl Shared =====

impl Shared {
    /// `limit` caps admitted requests.
    pub(crate) fn new(
        limit: usize,
        #[cfg(feature = "http3-datagram")] datagrams: Option<Arc<Registry>>,
    ) -> Arc<Self> {
        Arc::new(Self {
            #[cfg(feature = "http3-datagram")]
            datagrams,
            peer_extended_connect: OnceLock::new(),
            settings_ready: CancellationToken::new(),
            draining: AtomicBool::new(false),
            permits: Semaphore::new(limit.min(Semaphore::MAX_PERMITS)),
            active: AtomicUsize::new(0),
            waker: AtomicWaker::new(),
            error: OnceLock::new(),
        })
    }

    /// Drains and returns requests waiting for SETTINGS or admission. The driver
    /// calls this on every poll while closing, so only a change wakes it.
    pub(crate) fn shutdown(&self) {
        if !self.permits.is_closed() {
            self.permits.close();
        }
        // Close admission before waking the driver to check active exchanges.
        if !self.draining.swap(true, Ordering::AcqRel) {
            self.waker.wake();
        }
        // SETTINGS waiters have not entered the admission semaphore yet.
        self.settings_ready.cancel();
    }

    /// Publishes the connection error and fails everything still waiting on it.
    pub(crate) fn terminate(&self, error: Error) {
        self.error.get_or_init(|| Arc::new(error));
        self.shutdown();
        #[cfg(feature = "http3-datagram")]
        if let Some(datagrams) = &self.datagrams {
            datagrams.close();
        }
    }

    /// Whether new requests are no longer accepted.
    pub(crate) fn is_closed(&self) -> bool {
        self.draining.load(Ordering::Acquire)
    }

    /// Whether every admitted exchange has finished. Shutdown returns requests
    /// still waiting for admission instead of waiting for them to be polled.
    pub(crate) fn is_idle(&self) -> bool {
        self.active.load(Ordering::Acquire) == 0
    }

    /// The published connection error, or a cancellation before any was published.
    pub(crate) fn error(&self) -> Error {
        self.error
            .get()
            .map_or_else(Error::new_canceled, |e| Error::from_shared(e.clone()))
    }

    /// Prefers the published connection error over a later stream-level one.
    pub(crate) fn error_or(&self, error: Error) -> Error {
        self.error
            .get()
            .map_or(error, |e| Error::from_shared(e.clone()))
    }

    /// Registers the driver for drain, shutdown and completion wakeups.
    pub(crate) fn register(&self, cx: &Context<'_>) {
        self.waker.register(cx.waker());
    }
}

// ===== impl Active =====

impl Active {
    /// Prepares a request for admission without holding an active slot.
    pub(crate) fn reserve(shared: &Arc<Shared>) -> Self {
        Self {
            shared: shared.clone(),
            permit: false,
        }
    }

    /// Waits for local admission; fails once admission is closed.
    pub(crate) async fn admit(mut self) -> Result<Self> {
        match self.shared.permits.acquire().await {
            Ok(permit) => permit.forget(),
            Err(_) => return Err(self.shared.error().with("connection closed")),
        }
        self.permit = true;
        self.shared.active.fetch_add(1, Ordering::AcqRel);
        Ok(self)
    }

    /// The connection state this exchange belongs to.
    pub(crate) fn shared(&self) -> &Arc<Shared> {
        &self.shared
    }
}

impl Drop for Active {
    fn drop(&mut self) {
        if self.permit {
            self.shared.permits.add_permits(1);
            self.shared.active.fetch_sub(1, Ordering::AcqRel);
        }
        if self.shared.draining.load(Ordering::Acquire) {
            self.shared.waker.wake();
        }
    }
}
