use std::{
    collections::VecDeque,
    fmt,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_util::task::AtomicWaker;
use http::HeaderMap;

use crate::lock::LockResultExt;

/// Upper bound on queued body bytes for batched channels.
const MAX_QUEUED_BYTES: usize = 64 * 1024;

pub(crate) fn channel(wanter: bool) -> (Sender, Receiver) {
    channel_with(wanter, 1)
}

/// Accepts up to `capacity` chunks before applying backpressure, so a producer
/// holding several transport chunks hands them over in one receiver wakeup.
#[cfg(feature = "http3")]
pub(crate) fn batched(capacity: usize) -> (Sender, Receiver) {
    channel_with(false, capacity)
}

fn channel_with(wanter: bool, capacity: usize) -> (Sender, Receiver) {
    let shared = Arc::new(Shared {
        state: Mutex::new(State {
            item: None,
            rest: VecDeque::new(),
            queued: 0,
            capacity,
            pending_error: None,
            trailers: None,
            sender_open: true,
            receiver_open: true,
            want: !wanter,
        }),
        sender_waker: AtomicWaker::new(),
        receiver_waker: AtomicWaker::new(),
    });

    (
        Sender {
            shared: Arc::clone(&shared),
            trailers_sent: false,
        },
        Receiver {
            shared,
            terminated: false,
        },
    )
}

#[must_use = "Sender does nothing unless sent on"]
pub(crate) struct Sender {
    shared: Arc<Shared>,
    trailers_sent: bool,
}

pub(crate) struct Receiver {
    shared: Arc<Shared>,
    terminated: bool,
}

struct Shared {
    state: Mutex<State>,
    sender_waker: AtomicWaker,
    receiver_waker: AtomicWaker,
}

struct State {
    // Next item to deliver. A single-capacity channel uses only this slot,
    // which `poll_ready` gates before each send.
    item: Option<Result<Bytes, crate::Error>>,
    // Further accepted items in order; allocated only once two are queued.
    rest: VecDeque<Result<Bytes, crate::Error>>,
    // Bytes of data currently queued.
    queued: usize,
    // Maximum queued items before `poll_ready` applies backpressure.
    capacity: usize,
    // An error must not displace data which was already accepted. The old
    // mpsc channel achieved this by sending the error from a cloned sender.
    pending_error: Option<crate::Error>,
    trailers: Option<HeaderMap>,
    sender_open: bool,
    receiver_open: bool,
    want: bool,
}

// ===== impl State =====

impl State {
    fn len(&self) -> usize {
        usize::from(self.item.is_some()) + self.rest.len()
    }

    fn has_room(&self) -> bool {
        let last = self.rest.back().or(self.item.as_ref());
        self.len() < self.capacity
            && self.queued < MAX_QUEUED_BYTES
            && !matches!(last, Some(Err(_)))
    }

    fn push(&mut self, entry: Result<Bytes, crate::Error>) {
        if let Ok(data) = &entry {
            self.queued += data.len();
        }
        if self.item.is_none() && self.rest.is_empty() {
            self.item = Some(entry);
        } else {
            if self.rest.capacity() == 0 {
                self.rest.reserve(8);
            }
            self.rest.push_back(entry);
        }
    }

    fn pop(&mut self) -> Option<Result<Bytes, crate::Error>> {
        let entry = self.item.take()?;
        self.item = self.rest.pop_front();
        if let Ok(data) = &entry {
            self.queued -= data.len();
        }
        Some(entry)
    }
}

// ===== impl Sender =====

impl Sender {
    pub(crate) fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<crate::Result<()>> {
        self.shared.sender_waker.register(cx.waker());
        let state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            Poll::Ready(Err(crate::Error::new_closed()))
        } else if state.want && state.has_room() && state.pending_error.is_none() {
            Poll::Ready(Ok(()))
        } else {
            Poll::Pending
        }
    }

    pub(crate) fn send_data(&mut self, chunk: Bytes) -> Result<(), Bytes> {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open || !state.want || !state.has_room() || state.pending_error.is_some()
        {
            return Err(chunk);
        }
        state.push(Ok(chunk));
        drop(state);
        self.shared.receiver_waker.wake();
        Ok(())
    }

    pub(crate) fn send_trailers(&mut self, trailers: HeaderMap) -> Result<(), Option<HeaderMap>> {
        if self.trailers_sent {
            return Err(None);
        }
        self.trailers_sent = true;

        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            return Err(Some(trailers));
        }
        state.trailers = Some(trailers);
        drop(state);
        self.shared.receiver_waker.wake();
        Ok(())
    }

    pub(crate) fn send_error(&mut self, err: crate::Error) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        if !state.receiver_open {
            return;
        }
        if state.len() == 0 {
            state.push(Err(err));
        } else if state.pending_error.is_none() {
            state.pending_error = Some(err);
        }
        drop(state);
        self.shared.receiver_waker.wake();
    }

    #[cfg(feature = "http3")]
    pub(crate) fn poll_closed(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        self.shared.sender_waker.register(cx.waker());
        if self.is_closed() {
            Poll::Ready(())
        } else {
            Poll::Pending
        }
    }

    fn is_closed(&self) -> bool {
        !self.shared.state.lock().panic_if_poisoned().receiver_open
    }
}

// ===== impl Receiver =====

impl Receiver {
    pub(crate) fn poll_next(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<bytes::Bytes, crate::Error>>> {
        if self.terminated {
            return Poll::Ready(None);
        }

        self.shared.receiver_waker.register(cx.waker());
        let mut state = self.shared.state.lock().panic_if_poisoned();
        let wake_sender = if !state.want {
            state.want = true;
            true
        } else {
            false
        };
        // Wake the sender only when this take frees capacity it waits for.
        let was_full = !state.has_room();
        if let Some(item) = state.pop() {
            let sender_open = state.sender_open;
            drop(state);
            if sender_open && (was_full || wake_sender) {
                self.shared.sender_waker.wake();
            }
            return Poll::Ready(Some(item));
        }
        if let Some(err) = state.pending_error.take() {
            drop(state);
            if wake_sender {
                self.shared.sender_waker.wake();
            }
            return Poll::Ready(Some(Err(err)));
        }
        let sender_open = state.sender_open;
        drop(state);
        if wake_sender {
            self.shared.sender_waker.wake();
        }
        if sender_open {
            Poll::Pending
        } else {
            self.terminated = true;
            Poll::Ready(None)
        }
    }

    pub(crate) fn take_trailers(&mut self) -> Option<HeaderMap> {
        debug_assert!(self.terminated, "data channel still open before trailers");
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.trailers.take()
    }
}

impl Drop for Sender {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.sender_open = false;
        drop(state);
        self.shared.receiver_waker.wake();
    }
}

impl Drop for Receiver {
    fn drop(&mut self) {
        let mut state = self.shared.state.lock().panic_if_poisoned();
        state.receiver_open = false;
        drop(state);
        self.shared.sender_waker.wake();
    }
}

impl fmt::Debug for Sender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_closed() {
            f.debug_tuple("Sender").field(&"Closed").finish()
        } else {
            f.debug_tuple("Sender").field(&"Open").finish()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn recv(rx: &mut Receiver) -> Option<Result<Bytes, crate::Error>> {
        futures_util::future::poll_fn(|cx| rx.poll_next(cx)).await
    }

    #[tokio::test]
    async fn error_queued_behind_accepted_data() {
        let (mut tx, mut rx) = channel(false);
        tx.send_data(Bytes::from_static(b"data")).unwrap();
        tx.send_error(crate::Error::new_incomplete());
        drop(tx);

        assert_eq!(recv(&mut rx).await.unwrap().unwrap(), "data");
        assert!(recv(&mut rx).await.unwrap().is_err());
        assert!(recv(&mut rx).await.is_none());
    }

    #[tokio::test]
    async fn trailers_follow_data_close() {
        let (mut tx, mut rx) = channel(false);
        let mut trailers = HeaderMap::new();
        trailers.insert("x-trailer", "value".parse().unwrap());
        tx.send_trailers(trailers).unwrap();
        drop(tx);

        assert!(recv(&mut rx).await.is_none());
        assert_eq!(rx.take_trailers().unwrap()["x-trailer"], "value");
    }
}
