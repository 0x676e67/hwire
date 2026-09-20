//! Runs a pending exchange synchronously when transport close wakes it.
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use futures_util::{future::poll_fn, task::AtomicWaker};
use wreq_proto::rt::quic::{self as rt, DatagramConnection, OpenStreams};

use super::*;

type Job = Pin<Box<dyn Future<Output = ()> + Send>>;

#[derive(Clone, Default)]
struct Jobs(Arc<Queue>);

#[derive(Default)]
struct Queue {
    jobs: Mutex<Vec<Job>>,
    waker: AtomicWaker,
}

#[derive(Clone)]
struct Transport<T> {
    inner: T,
    jobs: Jobs,
}

// ===== impl Jobs =====

impl Jobs {
    fn poll(&self, cx: &mut Context<'_>) {
        self.0.waker.register(cx.waker());
        self.0
            .jobs
            .lock()
            .unwrap()
            .retain_mut(|job| job.as_mut().poll(cx).is_pending());
    }
}

impl Executor<Job> for Jobs {
    fn execute(&self, job: Job) {
        self.0.jobs.lock().unwrap().push(job);
        self.0.waker.wake();
    }
}

// ===== impl Transport =====

impl<T: rt::Connection<Bytes>> rt::Connection<Bytes> for Transport<T> {
    type RecvStream = T::RecvStream;

    type OpenStreams = Transport<T::OpenStreams>;

    fn poll_accept_recv(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::RecvStream>, rt::ConnectionError>> {
        self.inner.poll_accept_recv(cx)
    }

    fn poll_accept_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Option<Self::BidiStream>, rt::ConnectionError>> {
        self.inner.poll_accept_bidi(cx)
    }

    fn opener(&self) -> Self::OpenStreams {
        Transport {
            inner: self.inner.opener(),
            jobs: self.jobs.clone(),
        }
    }
}

impl<T: OpenStreams<Bytes>> OpenStreams<Bytes> for Transport<T> {
    type SendStream = T::SendStream;

    type BidiStream = T::BidiStream;

    fn poll_open_bidi(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::BidiStream, rt::StreamError>> {
        self.inner.poll_open_bidi(cx)
    }

    fn poll_open_send(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Result<Self::SendStream, rt::StreamError>> {
        self.inner.poll_open_send(cx)
    }

    fn close(&mut self, code: u64, reason: &[u8]) {
        self.inner.close(code, reason);
        // Model another executor polling the awakened request before close returns.
        self.jobs
            .poll(&mut Context::from_waker(std::task::Waker::noop()));
    }
}

impl<T: DatagramConnection> DatagramConnection for Transport<T> {
    type Sender = T::Sender;

    type Receiver = T::Receiver;

    fn take_datagrams(&mut self) -> Option<(Self::Sender, Self::Receiver)> {
        self.inner.take_datagrams()
    }
}

#[tokio::test]
async fn datagram_failure_preserves_cause_when_close_wakes_pending_request() {
    for response_started in [false, true] {
        check_cause(response_started).await;
    }
}

async fn check_cause(response_started: bool) {
    bounded(async {
        let (_, server_config, client_config) = tls::config();
        let (client, server, _endpoints) = quic_pair(server_config, client_config).await;
        let peer = server.clone();
        let jobs = Jobs::default();
        let ((mut tx, driver), mut server) = tokio::join!(
            async {
                Builder::new(jobs.clone())
                    .handshake_with_datagrams(Transport {
                        inner: native::Connection::new(client),
                        jobs: jobs.clone(),
                    })
                    .await
                    .unwrap()
            },
            async {
                h3::server::builder()
                    .enable_datagram(true)
                    .build::<_, Bytes>(h3_quinn::Connection::new(server))
                    .await
                    .unwrap()
            }
        );
        let driver = tokio::spawn(driver);
        let (seen, mut received) = oneshot::channel();
        let server = tokio::spawn(async move {
            let (_, mut stream) = server
                .accept()
                .await
                .unwrap()
                .unwrap()
                .resolve_request()
                .await
                .unwrap();
            if response_started {
                stream
                    .send_response(
                        Response::builder()
                            .header("content-length", 1)
                            .body(())
                            .unwrap(),
                    )
                    .await
                    .unwrap();
            }
            seen.send(()).unwrap();
            assert!(server.accept().await.is_err());
            drop(stream);
            server
        });
        let mut response = std::pin::pin!(tx.try_send_request(
            Request::get("https://localhost/pending")
                .body(Full::new(Bytes::new()))
                .unwrap()
        ));
        let mut head = None;
        let mut seen = false;
        poll_fn(|cx| {
            jobs.poll(cx);
            if head.is_none() {
                if let Poll::Ready(result) = response.as_mut().poll(cx) {
                    assert!(response_started);
                    head = Some(result.unwrap());
                }
            }
            if !seen {
                if let Poll::Ready(result) = Pin::new(&mut received).poll(cx) {
                    result.unwrap();
                    seen = true;
                }
            }
            if seen && (!response_started || head.is_some()) {
                Poll::Ready(())
            } else {
                Poll::Pending
            }
        })
        .await;
        // The missing Quarter Stream ID is a connection-level HTTP Datagram error.
        // https://www.rfc-editor.org/rfc/rfc9297.html#section-2.1
        peer.send_datagram(Bytes::new()).unwrap();
        let connection_error = driver.await.unwrap().unwrap_err();
        let request_error = if let Some(head) = head {
            head.into_body().collect().await.unwrap_err()
        } else {
            response.await.unwrap_err().into_error()
        };
        eprintln!("connection={connection_error:?}; request={request_error:?}");
        assert!(jobs.0.jobs.lock().unwrap().is_empty());
        assert_datagram_cause(&connection_error);
        assert_datagram_cause(&request_error);
        let _server = server.await.unwrap();
        match peer.closed().await {
            quinn::ConnectionError::ApplicationClosed(error) => {
                assert_eq!(error.error_code.into_inner(), 0x33);
            }
            error => panic!("unexpected peer close: {error:?}"),
        }
    })
    .await;
}

fn assert_datagram_cause(error: &wreq_proto::Error) {
    use std::error::Error;
    let mut source = error.source();
    while let Some(cause) = source {
        if let Some(http3::error::ConnectionError::Local {
            error: http3::error::LocalError::Application { code, .. },
        }) = cause.downcast_ref::<http3::error::ConnectionError>()
        {
            assert_eq!(*code, http3::error::Code::H3_DATAGRAM_ERROR);
            return;
        }
        source = cause.source();
    }
    panic!("missing HTTP Datagram connection cause: {error:?}");
}
