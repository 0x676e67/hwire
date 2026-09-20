use std::convert::Infallible;

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

#[tokio::test]
async fn early_response_drain_waits_for_upload_fin_ack() {
    check_drain(false).await;
}

#[tokio::test]
async fn tunnel_drain_waits_for_upload_fin_ack() {
    check_drain(true).await;
}

async fn check_drain(connect: bool) {
    for graceful in [false, true] {
        bounded(async {
            type Body = http_body_util::combinators::BoxBody<Bytes, Infallible>;
            let (_, server_config, client_config) = tls::config();
            let (client, server, _endpoints) = quic_pair(server_config, client_config).await;
            let pause = pause::Pause::default();
            pause.hold_finish_ack();
            let ((mut tx, driver), mut server) = tokio::join!(
                async {
                    Builder::new(Exec).handshake::<_, Body>(pause.wrap(native::Connection::new(client))).await.unwrap()
                },
                async {
                    h3::server::Connection::<_, Bytes>::new(h3_quinn::Connection::new(server)).await.unwrap()
                },
            );
            let (shutdown, requested) = oneshot::channel();
            let mut drive = tokio::spawn(async move {
                let mut driver = std::pin::pin!(driver);
                tokio::select! {
                    result = &mut driver => return result,
                    _ = requested => {
                        if graceful { driver.as_mut().graceful_shutdown(); }
                    }
                }
                driver.await
            });
            let (read, received) = oneshot::channel();
            let peer = tokio::spawn(async move {
                let resolver = server.accept().await.unwrap().unwrap();
                let transfer = tokio::spawn(async move {
                    let (_, mut stream) = resolver.resolve_request().await.unwrap();
                    // Complete the response before the client starts its upload.
                    stream.send_response(Response::new(())).await.unwrap();
                    stream.finish().await.unwrap();
                    let mut total = 0;
                    while let Some(mut data) = stream.recv_data().await.unwrap() {
                        total += data.remaining();
                        while data.has_remaining() { assert_eq!(data.get_u8(), 9); }
                    }
                    assert_eq!(total, 128 * 1024);
                    read.send(()).unwrap();
                });
                let _ = server.accept().await;
                transfer.await.unwrap();
            });
            let (upload, allowed) = oneshot::channel();
            let body = if connect {
                Full::new(Bytes::new()).boxed()
            } else {
                http_body_util::StreamBody::new(futures_util::stream::once(async move {
                    allowed.await.unwrap();
                    Ok::<_, Infallible>(http_body::Frame::data(Bytes::from(vec![9; 128 * 1024])))
                })).boxed()
            };
            let request = if connect { Request::connect("localhost:443") } else { Request::post("https://localhost/") };
            let mut response = tx.try_send_request(request.body(body).unwrap()).await.unwrap();
            if graceful {
                shutdown.send(()).unwrap();
            } else {
                drop(tx);
                drop(shutdown);
            }
            if connect {
                let mut tunnel = wreq_proto::upgrade::on(&mut response).await.unwrap();
                let mut incoming = Vec::new();
                tunnel.read_to_end(&mut incoming).await.unwrap();
                assert!(incoming.is_empty());
                tunnel.write_all(&vec![9; 128 * 1024]).await.unwrap();
                tunnel.shutdown().await.unwrap();
            } else {
                assert!(response.into_body().collect().await.unwrap().to_bytes().is_empty());
                upload.send(()).unwrap();
            }
            tokio::select! {
                _ = pause.waiting_for_finish_ack() => {},
                result = &mut drive => panic!("driver closed before FIN acknowledgment: {result:?}"),
            }
            assert!(!drive.is_finished());
            received.await.unwrap();
            pause.release_finish_ack();
            drive.await.unwrap().unwrap();
            peer.await.unwrap();
        }).await;
    }
}
