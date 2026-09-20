use super::*;

struct Preserve;

impl wreq_proto::ext::OnPreserveHeaderCallback for Preserve {
    fn call(&self, headers: &mut HeaderMap) {
        // The callback must see automatically inserted framing fields.
        assert_eq!(headers["content-length"], "0");
        let mut old = std::mem::take(headers);
        headers.insert("x-last", old.remove("x-last").unwrap());
        headers.insert("x-first", old.remove("x-first").unwrap());
        headers.extend(old);
        headers.insert("x-preserved", "yes".parse().unwrap());
    }

    fn call_visit(
        &self,
        _: &mut HeaderMap,
        _: &mut dyn FnMut(&dyn AsRef<[u8]>, &http::HeaderValue),
    ) {
        panic!("HTTP/3 uses the header map callback, like HTTP/2");
    }
}

#[tokio::test]
async fn preserve_header_callback_reaches_peer_in_order() {
    bounded(async {
        let Pair {
            mut tx,
            driver,
            mut server,
            _endpoints,
            ..
        } = pair(Http3Options::default()).await;
        let drive = tokio::spawn(driver);
        let peer = tokio::spawn(async move {
            let (request, mut stream) = server
                .accept()
                .await
                .unwrap()
                .unwrap()
                .resolve_request()
                .await
                .unwrap();
            assert_eq!(request.headers()["x-preserved"], "yes");
            let names: Vec<_> = request.headers().keys().map(|name| name.as_str()).collect();
            assert_eq!(
                names,
                ["x-last", "x-first", "content-length", "x-preserved"]
            );
            stream.send_response(Response::new(())).await.unwrap();
            stream.finish().await.unwrap();
            let _ = server.accept().await;
        });
        let mut request = Request::post("https://localhost/")
            .header("x-first", "one")
            .header("x-last", "two")
            .body(Full::new(Bytes::new()))
            .unwrap();
        wreq_proto::ext::on_preserve_header(&mut request, Preserve);
        tx.try_send_request(request)
            .await
            .unwrap()
            .into_body()
            .collect()
            .await
            .unwrap();
        drop(tx);
        drive.await.unwrap().unwrap();
        peer.await.unwrap();
    })
    .await;
}
