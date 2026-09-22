//! HTTP/3 request exchange. Validation, admission, stream open, the upload
//! and the response head all run in the caller's future; only an upload that
//! outlives the head and CONNECT tunnels move to the executor.

use std::{
    future::{pending, poll_fn, Future},
    pin::pin,
    sync::Arc,
    task::{Context, Poll, Waker},
};

use bytes::{Buf, Bytes};
use futures_util::future::BoxFuture;
use http::{header, HeaderMap, Method, Request, Response, StatusCode};
use http3::{
    client::{RequestStream, SendRequest},
    error::{Code, StreamError},
    ext::Protocol,
    quic,
};
use http_body::Body;
use http_body_util::combinators::BoxBody;
use tokio_util::sync::CancellationToken;

use super::{
    body::{RecvBody, UploadGuard},
    shared::{Active, Shared},
    upgrade,
};
use crate::{
    body::Incoming, dispatch::TrySendError, error::BoxError, ext::OnPreserveHeader, proto::headers,
    rt::Executor, Error, Result,
};
#[cfg(feature = "http3-datagram")]
use crate::{conn::http3::datagram::DatagramRequest, proto::http3::datagram::RequestState};

/// Largest chunk handed to the QUIC send half at once.
pub(super) const CHUNK: usize = 16 * 1024;

/// Send half of a request stream. Dropping it before `finished` resets the
/// upload, so a completed FIN must set the flag first.
pub(super) struct SendGuard<S: quic::SendStream<Bytes>> {
    #[cfg(feature = "http3-datagram")]
    pub(super) datagrams: Option<Arc<RequestState>>,
    pub(super) stream: RequestStream<S, Bytes>,
    pub(super) finished: bool,
}

/// Receive half of a request stream. Dropping it before `finished` sends
/// STOP_SENDING with `code`, which defaults to a local cancellation.
pub(super) struct RecvGuard<S: quic::RecvStream> {
    #[cfg(feature = "http3-datagram")]
    pub(super) datagrams: Option<Arc<RequestState>>,
    pub(super) stream: RequestStream<S, Bytes>,
    pub(super) finished: bool,
    pub(super) code: Code,
}

/// Runs one request. Errors before the stream opens return the request;
/// dropping the future before it resolves cancels both directions. `exec`
/// runs an upload that outlives the response head or a CONNECT tunnel.
#[allow(clippy::result_large_err)]
pub(crate) async fn request<O, B, E>(
    mut sender: SendRequest<O, Bytes>,
    exec: E,
    shared: Arc<Shared>,
    mut request: Request<B>,
    reservation: Option<Active>,
) -> Result<Response<Incoming>, TrySendError<Request<B>>>
where
    O: quic::OpenStreams<Bytes>,
    O::BidiStream: quic::BidiStream<Bytes>,
    <O::BidiStream as quic::BidiStream<Bytes>>::SendStream: Send + 'static,
    <O::BidiStream as quic::BidiStream<Bytes>>::RecvStream: Send + 'static,
    B: Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<BoxError>,
    E: Executor<BoxFuture<'static, ()>>,
{
    let Some(reservation) = reservation else {
        return Err(rejected(
            Error::new_canceled().with("connection closed"),
            request,
        ));
    };

    let connect = request.method() == Method::CONNECT;
    let head = request.method() == Method::HEAD;

    #[cfg(feature = "http3-datagram")]
    let datagram_request = request.extensions().get::<DatagramRequest>().is_some();
    #[cfg(feature = "http3-datagram")]
    if datagram_request
        && (shared.datagrams.is_none()
            || !connect
            || request.extensions().get::<Protocol>().is_none())
    {
        return Err(rejected(
            Error::new_user_invalid_request(
                "Datagram requests require an Extended CONNECT and a Datagram-enabled connection",
            ),
            request,
        ));
    }

    if connect && !request.body().is_end_stream() {
        return Err(rejected(Error::new_user_invalid_connect(), request));
    }

    headers::strip_connection_headers(request.headers_mut(), true);
    let length = match validate_request(&request) {
        Ok(length) => length,
        Err(error) => return Err(rejected(Error::new_user_invalid_request(error), request)),
    };

    if request.extensions().get::<Protocol>().is_some() {
        shared.settings_ready.cancelled().await;
        if !shared
            .peer_extended_connect
            .get()
            .is_some_and(|enabled| *enabled)
        {
            let error = if shared.permits.is_closed() {
                shared.error()
            } else {
                Error::new_user_invalid_request("peer did not enable Extended CONNECT")
            };
            return Err(rejected(error, request));
        }
    }

    let length = if length.is_none() && !connect {
        let size = request.body().size_hint().exact();
        if let Some(size) = size {
            if size != 0 || headers::method_has_defined_payload_semantics(request.method()) {
                headers::set_content_length_if_missing(request.headers_mut(), size);
            }
        }
        size
    } else {
        length
    };

    if request.body().is_end_stream() && length.is_some_and(|n| n != 0) {
        return Err(rejected(
            Error::new_user_body("body shorter than content-length"),
            request,
        ));
    }

    let active = match reservation.admit().await {
        Ok(active) => active,
        Err(error) => return Err(rejected(error, request)),
    };

    let (mut parts, body) = request.into_parts();
    if let Some(header_sort) = parts.extensions.remove::<OnPreserveHeader>() {
        header_sort.call(&mut parts.headers);
    }

    // From here on the request has left the caller, so failures cannot return it.
    let stream = sender
        .send_request(Request::from_parts(parts, ()))
        .await
        .map_err(|error| lost(shared.error_or(Error::new_h3(error))))?;
    if stream.id().into_inner() % 4 != 0 {
        return Err(lost(Error::new_h3(
            "QUIC backend returned a non-client request stream ID",
        )));
    }

    #[cfg(feature = "http3-datagram")]
    let registration = shared
        .datagrams
        .as_ref()
        .map(|registry| registry.register(stream.id(), datagram_request, CancellationToken::new()));

    #[cfg(feature = "http3-datagram")]
    let datagrams = registration.as_ref().map(|registration| &registration.0);

    #[cfg(feature = "http3-datagram")]
    let invalid = datagrams.as_ref().map(|state| state.invalid.clone());

    #[cfg(not(feature = "http3-datagram"))]
    let invalid: Option<CancellationToken> = None;

    let (send, recv) = stream.split();
    let send = SendGuard {
        #[cfg(feature = "http3-datagram")]
        datagrams: datagrams.cloned(),
        stream: send,
        finished: false,
    };

    let mut recv = RecvGuard {
        #[cfg(feature = "http3-datagram")]
        datagrams: datagrams.cloned(),
        stream: recv,
        finished: false,
        code: Code::H3_REQUEST_CANCELLED,
    };

    let mut invalid_watch = pin!(invalid_datagram(invalid.clone()));

    // CONNECT keeps its send direction open for the tunnel, so its head is
    // read before anything is finished.
    let initial_response = if connect {
        let headers = {
            let mut response = pin!(response_headers(&mut recv));
            poll_fn(|cx| {
                if let Poll::Ready(error) = invalid_watch.as_mut().poll(cx) {
                    return Poll::Ready(Err(error));
                }
                response.as_mut().poll(cx)
            })
            .await
        }
        .map_err(|error| lost(shared.error_or(error)))?;
        if headers.status().is_success() {
            #[cfg(feature = "http3-datagram")]
            let datagrams = match registration {
                None => upgrade::TunnelDatagrams::Disabled,
                Some(registration) if datagram_request => {
                    upgrade::TunnelDatagrams::Datagram(registration)
                }
                Some(registration) => upgrade::TunnelDatagrams::Ordinary(registration),
            };
            return Ok(upgrade::tunnel(
                send,
                recv,
                headers,
                active,
                #[cfg(feature = "http3-datagram")]
                datagrams,
                &exec,
            ));
        }
        Some(headers)
    } else {
        None
    };

    // An empty body finishes inline; its FIN acknowledgment is checked once
    // the head is in, so a drain cannot discard an unacknowledged FIN.
    let mut finished = None;
    let mut upload = if body.is_end_stream() {
        finished = Some(
            finish(send)
                .await
                .map_err(|error| lost(shared.error_or(error)))?,
        );
        None
    } else {
        Some(Box::pin(upload(send, body, length)) as BoxFuture<'static, Result<()>>)
    };

    let mut headers = match initial_response {
        Some(headers) => headers,
        None => {
            let mut response = pin!(response_headers(&mut recv));
            poll_fn(|cx| {
                if let Some(pending) = upload.as_mut() {
                    match pending.as_mut().poll(cx) {
                        Poll::Ready(Ok(())) => upload = None,
                        Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                        Poll::Pending => {}
                    }
                }
                if let Poll::Ready(error) = invalid_watch.as_mut().poll(cx) {
                    return Poll::Ready(Err(error));
                }
                response.as_mut().poll(cx)
            })
            .await
            .map_err(|error| lost(shared.error_or(error)))?
        }
    };

    let mut remaining = content_length(headers.headers()).map_err(|error| {
        // A malformed response is a stream error, not a local cancellation.
        // https://www.rfc-editor.org/rfc/rfc9114.html#section-4.1.2
        recv.code = Code::H3_MESSAGE_ERROR;
        lost(error)
    })?;
    if head
        || matches!(
            headers.status(),
            StatusCode::NO_CONTENT | StatusCode::NOT_MODIFIED
        )
    {
        remaining = Some(0);
    }
    *headers.version_mut() = http::Version::HTTP_3;

    // The peer's response packet normally carries the acknowledgment of the
    // packet that held HEADERS and FIN; only a late one needs a task.
    if let Some(mut send) = finished.take() {
        if send.finished {
            match send
                .stream
                .poll_stopped(&mut Context::from_waker(Waker::noop()))
            {
                Poll::Ready(Ok(_)) => {}
                Poll::Ready(Err(error)) => {
                    return Err(lost(shared.error_or(Error::new_h3(error))));
                }
                Poll::Pending => {
                    upload = Some(Box::pin(async move {
                        poll_fn(|cx| send.stream.poll_stopped(cx))
                            .await
                            .map(|_| ())
                            .map_err(Error::new_h3)
                    }));
                }
            }
        }
    }

    #[cfg(feature = "http3-datagram")]
    let datagrams = datagrams.cloned();
    let body = RecvBody::new(
        recv,
        remaining,
        active,
        #[cfg(feature = "http3-datagram")]
        registration,
        upload.is_some(),
    );

    #[cfg(feature = "http3-datagram")]
    if let Some(datagrams) = &datagrams {
        body.attach(datagrams);
    }

    if let Some(upload) = upload {
        exec.execute(Box::pin(continue_upload(
            upload,
            body.upload_guard(),
            invalid,
        )));
    }

    Ok(headers.map(|()| Incoming::h3(BoxBody::new(body))))
}

/// Finishes an upload that outlived the response head. Either direction
/// failing cancels the other; the guard releases the permit once both are
/// done, even if the executor drops this task before polling it.
async fn continue_upload<S>(
    mut upload: BoxFuture<'static, Result<()>>,
    mut guard: UploadGuard<S>,
    invalid: Option<CancellationToken>,
) where
    S: quic::RecvStream + Send + 'static,
{
    let mut invalid_watch = pin!(invalid_datagram(invalid));
    let result = poll_fn(|cx| {
        if let Poll::Ready(error) = invalid_watch.as_mut().poll(cx) {
            return Poll::Ready(Err(error));
        }
        if !guard.watch(cx) {
            return Poll::Ready(Err(Error::new_canceled()));
        }
        upload.as_mut().poll(cx)
    })
    .await;
    // Dropping an unfinished upload resets it before the response learns why.
    drop(upload);
    if let Err(error) = result {
        guard.fail(error);
    }
}

/// Resolves when a Datagram arrives for a request without Datagram semantics.
pub(super) async fn invalid_datagram(invalid: Option<CancellationToken>) -> Error {
    match invalid {
        Some(invalid) => invalid.cancelled().await,
        None => pending().await,
    }
    invalid_datagram_error()
}

/// A Datagram on a request without Datagram semantics is a stream error.
/// https://www.rfc-editor.org/rfc/rfc9297.html#section-2.1
pub(super) fn invalid_datagram_error() -> Error {
    Error::new_h3("HTTP Datagram on a request without Datagram semantics")
}

/// Closes the send direction of an empty upload. After a peer STOP_SENDING
/// the guard stays unfinished so dropping it answers with a reset.
async fn finish<S: quic::SendStream<Bytes>>(mut send: SendGuard<S>) -> Result<SendGuard<S>> {
    match send.stream.finish().await {
        Ok(()) => {}
        Err(StreamError::RemoteTerminate { .. }) => return Ok(send),
        Err(error) => return Err(Error::new_h3(error)),
    }
    send.finished = true;
    #[cfg(feature = "http3-datagram")]
    if let Some(datagrams) = &send.datagrams {
        datagrams.close_send();
    }
    Ok(send)
}

/// Failure before the request left the caller; the request is returned.
fn rejected<B>(error: Error, request: Request<B>) -> TrySendError<Request<B>> {
    TrySendError {
        error,
        message: Some(request),
    }
}

/// Failure after the stream opened; the request cannot be returned.
fn lost<B>(error: Error) -> TrySendError<Request<B>> {
    TrySendError {
        error,
        message: None,
    }
}

/// Streams the request body and FIN, then waits for the acknowledgment or
/// the peer's STOP_SENDING so a drain cannot discard queued upload bytes.
async fn upload<S, B>(mut send: SendGuard<S>, body: B, mut remaining: Option<u64>) -> Result<()>
where
    S: quic::SendStream<Bytes>,
    B: Body,
    B::Error: Into<BoxError>,
{
    let mut body = pin!(body);
    let mut trailers_sent = false;
    let mut budget = 0;
    let mut stopped = false;
    while let Some(frame) = poll_fn(|cx| {
        // Check cancellation before Body: continuously ready empty frames never
        // reach the QUIC writer, so checking only on Pending would miss STOP.
        // A complete early response remains valid: RFC 9114, Section 4.1.
        // https://www.rfc-editor.org/rfc/rfc9114.html#section-4.1
        if let Poll::Ready(result) = send.stream.poll_stopped(cx) {
            stopped = true;
            return Poll::Ready(result.err().map(|error| Err(Error::new_h3(error))));
        }
        body.as_mut()
            .poll_frame(cx)
            .map(|frame| frame.map(|result| result.map_err(Error::new_user_body)))
    })
    .await
    {
        let frame = frame?;
        if trailers_sent {
            return Err(Error::new_user_body("body frame after trailers"));
        }
        match frame.into_data() {
            Ok(mut data) => {
                consume_length(&mut remaining, data.remaining()).map_err(Error::new_user_body)?;
                while data.has_remaining() {
                    let size = data.remaining().min(CHUNK);
                    match send.stream.send_data(data.copy_to_bytes(size)).await {
                        Ok(()) => {}
                        Err(StreamError::RemoteTerminate { .. }) => return Ok(()),
                        Err(error) => return Err(Error::new_h3(error)),
                    }
                    cooperate(&mut budget).await;
                }
            }
            Err(frame) => {
                if let Ok(trailers) = frame.into_trailers() {
                    if remaining.is_some_and(|n| n != 0) {
                        return Err(Error::new_user_body("body shorter than content-length"));
                    }
                    match send.stream.send_trailers(trailers).await {
                        Ok(()) => {}
                        Err(StreamError::RemoteTerminate { .. }) => return Ok(()),
                        Err(error) => return Err(Error::new_h3(error)),
                    }
                    trailers_sent = true;
                }
            }
        }
        cooperate(&mut budget).await;
    }
    if stopped {
        return Ok(());
    }
    if remaining.is_some_and(|n| n != 0) {
        return Err(Error::new_user_body("body shorter than content-length"));
    }
    match send.stream.finish().await {
        Ok(()) => {}
        Err(StreamError::RemoteTerminate { .. }) => return Ok(()),
        Err(error) => return Err(Error::new_h3(error)),
    }
    send.finished = true;
    #[cfg(feature = "http3-datagram")]
    if let Some(datagrams) = &send.datagrams {
        datagrams.close_send();
    }
    // FIN is only queued by finish(). Keep the exchange active until transport
    // delivery or peer cancellation, so connection drain cannot discard it.
    poll_fn(|cx| send.stream.poll_stopped(cx))
        .await
        .map(|_| ())
        .map_err(Error::new_h3)
}

/// Reads the final response head, skipping informational responses.
async fn response_headers<S: quic::RecvStream>(recv: &mut RecvGuard<S>) -> Result<Response<()>> {
    let mut budget = 0;
    let headers = loop {
        let headers = recv.stream.recv_response().await.map_err(Error::new_h3)?;
        if headers.status() == StatusCode::SWITCHING_PROTOCOLS {
            recv.code = Code::H3_MESSAGE_ERROR;
            return Err(Error::new_h3("HTTP/3 response cannot use status 101"));
        }
        if !headers.status().is_informational() {
            break headers;
        }
        // Ignore the length on a response without content, but still validate
        // the field syntax. RFC 9114, Section 4.1.2.
        // https://www.rfc-editor.org/rfc/rfc9114.html#section-4.1.2
        content_length(headers.headers()).inspect_err(|_| {
            recv.code = Code::H3_MESSAGE_ERROR;
        })?;
        cooperate(&mut budget).await;
    };
    Ok(headers)
}

/// Validates the request and returns its declared Content-Length.
fn validate_request<B>(request: &Request<B>) -> Result<Option<u64>> {
    if request.extensions().get::<Protocol>().is_some() && request.method() != Method::CONNECT {
        return Err(Error::new_h3("Extended CONNECT protocol requires CONNECT"));
    }
    let ordinary_connect =
        request.method() == Method::CONNECT && request.extensions().get::<Protocol>().is_none();
    if request.uri().authority().is_none()
        || (!ordinary_connect && request.uri().scheme().is_none())
    {
        return Err(Error::new_h3(
            "HTTP/3 request requires scheme and authority",
        ));
    }
    // Reject this before the lower layer consumes the request and turns a local
    // header construction error into a connection failure.
    // https://www.rfc-editor.org/rfc/rfc9114.html#section-4.3.1
    if request
        .uri()
        .authority()
        .zip(request.headers().get(header::HOST))
        .is_some_and(|(authority, host)| authority.as_str() != host)
    {
        return Err(Error::new_h3("Host conflicts with HTTP/3 authority"));
    }
    if request
        .headers()
        .get_all(header::TE)
        .iter()
        .any(|v| !v.as_bytes().eq_ignore_ascii_case(b"trailers"))
    {
        return Err(Error::new_h3("HTTP/3 TE must be trailers"));
    }
    content_length(request.headers())
}

/// Parses every Content-Length field, rejecting malformed or conflicting values.
fn content_length(headers: &HeaderMap) -> Result<Option<u64>> {
    let mut length = None;
    for value in headers.get_all(header::CONTENT_LENGTH) {
        let value = value.to_str().map_err(Error::new_h3)?;
        for item in value.split(',') {
            let item = item.trim_matches([' ', '\t']);
            if item.is_empty() || !item.bytes().all(|b| b.is_ascii_digit()) {
                return Err(Error::new_h3("invalid content-length"));
            }
            let parsed = item.parse::<u64>().map_err(Error::new_h3)?;
            if length.is_some_and(|n| n != parsed) {
                return Err(Error::new_h3("conflicting content-length"));
            }
            length = Some(parsed);
        }
    }
    Ok(length)
}

/// Counts `size` against the declared length.
pub(super) fn consume_length(remaining: &mut Option<u64>, size: usize) -> Result<(), &'static str> {
    if let Some(left) = remaining {
        *left = left
            .checked_sub(u64::try_from(size).map_err(|_| "body size overflow")?)
            .ok_or("body exceeds content-length")?;
    }
    Ok(())
}

/// Yields after a burst of ready frames so one stream cannot starve the runtime.
pub(super) async fn cooperate(budget: &mut usize) {
    *budget += 1;
    if *budget < 32 {
        return;
    }
    *budget = 0;
    let mut yielded = false;
    poll_fn(|cx| {
        if yielded {
            Poll::Ready(())
        } else {
            yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    })
    .await;
}

// ===== impl SendGuard =====

impl<S: quic::SendStream<Bytes>> Drop for SendGuard<S> {
    fn drop(&mut self) {
        if !self.finished {
            let code = Code::H3_REQUEST_CANCELLED;
            #[cfg(feature = "http3-datagram")]
            let code = if self
                .datagrams
                .as_ref()
                .is_some_and(|d| d.invalid.is_cancelled())
            {
                Code::H3_DATAGRAM_ERROR
            } else {
                code
            };
            self.stream.stop_stream(code);
        }
    }
}

// ===== impl RecvGuard =====

impl<S: quic::RecvStream> Drop for RecvGuard<S> {
    fn drop(&mut self) {
        if !self.finished {
            // Late Datagrams after receive cancellation must be discarded,
            // without canceling an upload that still owns the send direction.
            // https://www.rfc-editor.org/rfc/rfc9297.html#section-2.1
            #[cfg(feature = "http3-datagram")]
            if let Some(datagrams) = &self.datagrams {
                datagrams.close_recv();
            }
            let code = self.code;
            #[cfg(feature = "http3-datagram")]
            let code = if self
                .datagrams
                .as_ref()
                .is_some_and(|d| d.invalid.is_cancelled())
            {
                Code::H3_DATAGRAM_ERROR
            } else {
                code
            };
            self.stream.stop_sending(code);
        }
    }
}
