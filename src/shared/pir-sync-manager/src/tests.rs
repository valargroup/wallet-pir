use super::*;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Tls {
    origin: String,
    cert: reqwest::Certificate,
    requests: Arc<Mutex<Vec<String>>>,
    /// Discovery response selector: ok, missing, redirect, oversize, slow-body or
    /// `body:<raw body>`.
    discovery: Arc<Mutex<String>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Tls {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl Tls {
    async fn new() -> Self {
        use rustls::pki_types::{pem::PemObject, CertificateDer, PrivateKeyDer};
        let root = tempfile::tempdir().unwrap();
        let cert = root.path().join("cert.pem");
        let key = root.path().join("key.pem");
        let generated = std::process::Command::new("openssl")
            .args([
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-addext",
                "subjectAltName=DNS:localhost",
                "-addext",
                "basicConstraints=critical,CA:FALSE",
                "-keyout",
                key.to_str().unwrap(),
                "-out",
                cert.to_str().unwrap(),
            ])
            .output()
            .unwrap();
        assert!(
            generated.status.success(),
            "local TLS certificate generation failed"
        );
        let cert_bytes = std::fs::read(cert).unwrap();
        let tls = rustls::ServerConfig::builder_with_provider(Arc::new(
            rustls::crypto::ring::default_provider(),
        ))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(
            vec![CertificateDer::from_pem_slice(&cert_bytes).unwrap()],
            PrivateKeyDer::from_pem_slice(&std::fs::read(key).unwrap()).unwrap(),
        )
        .unwrap();
        let acceptor = tokio_rustls::TlsAcceptor::from(Arc::new(tls));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!(
            "https://localhost:{}",
            listener.local_addr().unwrap().port()
        );
        let requests = Arc::new(Mutex::new(Vec::new()));
        let seen = requests.clone();
        let discovery = Arc::new(Mutex::new("ok".to_string()));
        let mode = discovery.clone();
        let task = tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                let Ok(mut socket) = acceptor.accept(socket).await else {
                    continue;
                };
                let mut request = Vec::new();
                while !request.ends_with(b"\r\n\r\n") {
                    let mut byte = [0];
                    if socket.read_exact(&mut byte).await.is_err() {
                        break;
                    }
                    request.push(byte[0]);
                }
                let headers = String::from_utf8(request.clone()).unwrap();
                let length = headers
                    .lines()
                    .find_map(|line| {
                        let (name, value) = line.split_once(':')?;
                        name.eq_ignore_ascii_case("content-length")
                            .then(|| value.trim().parse::<usize>().unwrap())
                    })
                    .unwrap_or(0);
                let mut body = vec![0; length];
                socket.read_exact(&mut body).await.unwrap();
                request.extend_from_slice(&body);
                let request = String::from_utf8(request).unwrap();
                let mode = mode.lock().unwrap().clone();
                let ok = pir_service_discovery::Document::publisher().to_json();
                let large = "x".repeat(pir_service_discovery::MAX_DOCUMENT_BYTES + 1);
                let discovered = request.starts_with("GET /.well-known/pir-service-discovery ");
                seen.lock().unwrap().push(request.clone());
                if discovered && mode == "slow-body" {
                    // Complete TLS and send headers plus a partial JSON body,
                    // then stall so the deadline must cover reading the body.
                    let response = format!(
                        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{{",
                        ok.len()
                    );
                    socket.write_all(response.as_bytes()).await.unwrap();
                    socket.flush().await.unwrap();
                    tokio::time::sleep(Duration::from_secs(30)).await;
                    continue;
                }
                let (status, headers, body) = if discovered {
                    match mode.as_str() {
                        "ok" => ("200 OK", "", String::from_utf8(ok).unwrap()),
                        "missing" => ("404 Not Found", "", "missing".into()),
                        "redirect" => (
                            "302 Found",
                            "Location: /.well-known/pir-service-discovery-moved\r\n",
                            String::new(),
                        ),
                        "oversize" => ("200 OK", "", large),
                        body => ("200 OK", "", body.strip_prefix("body:").unwrap().into()),
                    }
                } else if request.starts_with("GET /feed/redirect ") {
                    ("302 Found", "Location: /feed/ok\r\n", "refused".into())
                } else if request.contains(" /feed/fail ") {
                    ("503 Unavailable", "", "refused".into())
                } else if request
                    .to_ascii_lowercase()
                    .contains("if-none-match: \"saved\"")
                {
                    ("304 Not Modified", "", String::new())
                } else {
                    ("200 OK", "", "abcdef".into())
                };
                let response = format!("HTTP/1.1 {status}\r\n{headers}Content-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len());
                let _ = socket.write_all(response.as_bytes()).await;
                let _ = socket.shutdown().await;
            }
        });
        Self {
            origin,
            cert: reqwest::Certificate::from_pem(&cert_bytes).unwrap(),
            requests,
            discovery,
            task,
        }
    }
    fn source(&self, budget: u64) -> HttpsSource {
        HttpsSource::new(
            &self.origin,
            "/feed/",
            budget,
            std::slice::from_ref(&self.cert),
        )
        .unwrap()
    }
}

#[tokio::test]
async fn tls_identity_origin_and_namespace_are_enforced() {
    let tls = Tls::new().await;
    let mut untrusted = HttpsSource::new(&tls.origin, "/feed/", 100, &[]).unwrap();
    assert!(untrusted.request("ok", None).await.is_err());
    let mut source = tls.source(100);
    assert_eq!(
        source
            .request("ok", None)
            .await
            .unwrap()
            .read_limited(6)
            .await
            .unwrap(),
        b"abcdef"
    );
    let wrong_host = tls.origin.replace("localhost", "127.0.0.1");
    let mut wrong =
        HttpsSource::new(&wrong_host, "/feed/", 100, std::slice::from_ref(&tls.cert)).unwrap();
    assert!(wrong.request("ok", None).await.is_err());
    for origin in [
        "http://example.com",
        "https://user@example.com",
        "https://example.com/path",
        "https://example.com/?q=1",
        "https://example.com/#fragment",
    ] {
        assert!(HttpsSource::new(origin, "/feed/", 10, &[]).is_err());
    }
    for path in [
        "/escape",
        "//example.com/file",
        "../escape",
        "%2e%2e/escape",
        "a/../escape",
        "a\\escape",
        "https://example.com/file",
        "ok?query",
        "ok#fragment",
    ] {
        assert!(
            source.request(path, None).await.is_err(),
            "escaped namespace: {path}"
        );
    }
    for prefix in ["//example.com/", "/feed/../", "/feed/%2e/", "feed/"] {
        assert!(
            HttpsSource::new(&tls.origin, prefix, 100, std::slice::from_ref(&tls.cert)).is_err()
        );
    }
    assert_eq!(
        tls.requests.lock().unwrap().len(),
        1,
        "invalid paths must not reach the server"
    );
}

#[tokio::test]
async fn conditional_requests_redirects_and_refusals_remain_product_decisions() {
    let tls = Tls::new().await;
    let mut source = tls.source(100);
    let response = source.request("ok", Some("\"saved\"")).await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_MODIFIED);
    drop(response);
    assert_eq!(source.downloaded(), 0);
    let mut response = source.request("redirect", None).await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::FOUND);
    response.discard().await.unwrap();
    drop(response);
    assert_eq!(source.downloaded(), 7);
    let mut response = source.request("fail", None).await.unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    response.discard().await.unwrap();
    drop(response);
    assert_eq!(source.downloaded(), 14);
    let requests = tls.requests.lock().unwrap();
    assert_eq!(requests.len(), 3, "transport must not redirect or retry");
    assert!(requests
        .iter()
        .all(|r| r.to_ascii_lowercase().contains("cache-control: no-cache")));
}

#[tokio::test]
async fn metadata_and_error_bodies_share_one_attempt_budget() {
    let tls = Tls::new().await;
    let mut source = tls.source(10);
    assert_eq!(
        source
            .request("ok", None)
            .await
            .unwrap()
            .read_limited(6)
            .await
            .unwrap(),
        b"abcdef"
    );
    let mut refused = source.request("fail", None).await.unwrap();
    assert!(refused
        .discard()
        .await
        .unwrap_err()
        .to_string()
        .contains("download budget"));
    drop(refused);
    assert_eq!(source.downloaded(), 13);
    assert!(source.request("ok", None).await.is_err());
    assert_eq!(
        tls.requests.lock().unwrap().len(),
        2,
        "exhaustion must prevent the next request"
    );
    source.reset_budget();
    assert_eq!(source.downloaded(), 0);
    assert!(source
        .request("ok", None)
        .await
        .unwrap()
        .read_limited(5)
        .await
        .is_err());
    assert_eq!(
        source.downloaded(),
        6,
        "metadata rejection must retain accounting"
    );
}

#[tokio::test]
async fn json_posts_keep_the_namespace_and_share_the_get_response_budget() {
    let tls = Tls::new().await;
    let mut source = tls.source(10);
    let body = br#"{"anchors":[]}"#.to_vec();
    for path in ["../escape", "https://example.com/file", "/escape"] {
        assert!(source.post_json(path, body.clone()).await.is_err());
    }
    assert_eq!(
        source
            .post_json("anchors/check", body.clone())
            .await
            .unwrap()
            .read_limited(6)
            .await
            .unwrap(),
        b"abcdef"
    );
    let mut refused = source.post_json("fail", body).await.unwrap();
    assert_eq!(refused.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    assert!(refused
        .discard()
        .await
        .unwrap_err()
        .to_string()
        .contains("download budget"));
    drop(refused);
    assert_eq!(source.downloaded(), 13);
    assert!(source.request("ok", None).await.is_err());
    assert!(source.post_json("anchors/check", Vec::new()).await.is_err());
    let requests = tls.requests.lock().unwrap();
    assert_eq!(
        requests.len(),
        2,
        "invalid or exhausted requests must not reach the server"
    );
    assert!(requests[0].starts_with("POST /feed/anchors/check "));
    assert!(requests[0]
        .to_ascii_lowercase()
        .contains("content-type: application/json"));
    assert!(requests[0].ends_with(r#"{"anchors":[]}"#));
}

#[tokio::test]
async fn discovery_accepts_only_a_trusted_compatible_publisher() {
    let tls = Tls::new().await;
    let source = tls.source(0);
    assert_eq!(
        source.discover().await.unwrap(),
        pir_service_discovery::version()
    );
    // Discovery stays outside the product prefix and byte budget.
    assert_eq!(source.downloaded(), 0);
    let untrusted = HttpsSource::new(&tls.origin, "/feed/", 100, &[]).unwrap();
    let error = untrusted.discover().await.unwrap_err();
    assert!(error.incompatible().is_none());
    assert!(error.to_string().contains("unreachable"), "{error}");
    assert!(error.to_string().contains(&tls.origin), "{error}");

    let refused = |mode: &str| {
        *tls.discovery.lock().unwrap() = mode.into();
        let source = tls.source(100);
        async move { source.discover().await.unwrap_err() }
    };
    let missing = refused("missing").await;
    assert!(missing.to_string().contains("HTTP 404"), "{missing}");
    assert!(missing.to_string().contains(pir_service_discovery::PATH));
    let redirect = refused("redirect").await;
    assert!(redirect.to_string().contains("HTTP 302"), "{redirect}");
    let oversize = refused("oversize").await;
    assert!(oversize.to_string().contains("exceeds"), "{oversize}");
    for (body, expected) in [
        ("{", "malformed"),
        (
            r#"{"service":"pir-query-server","version":"1.0.0"}"#,
            "expected",
        ),
        (
            r#"{"service":"pir-publisher","version":"1.0"}"#,
            "invalid publisher version",
        ),
        (
            r#"{"service":"pir-publisher","version":"1.0.0-rc.1"}"#,
            "prerelease",
        ),
    ] {
        let error = refused(&format!("body:{body}")).await;
        assert!(error.to_string().contains(expected), "{body}: {error}");
        assert!(error.incompatible().is_none(), "{body}");
        assert!(!is_incompatible(&error));
    }
    let mismatch = refused(r#"body:{"service":"pir-publisher","version":"2.0.0"}"#).await;
    let incompatible = mismatch.incompatible().expect("typed mismatch");
    assert!(incompatible.query_server_is_older());
    let boxed: Error = Box::new(mismatch);
    assert!(is_incompatible(boxed.as_ref()));
    let text = boxed.to_string();
    assert!(text.contains(&tls.origin) && text.contains("2.0.0") && text.contains("1.0.0"));
    assert!(text.contains("upgrade the query server"), "{text}");
    // The same-major additive fields of a newer publisher are accepted.
    *tls.discovery.lock().unwrap() =
        r#"body:{"service":"pir-publisher","version":"1.9.0","added":true}"#.into();
    assert_eq!(
        tls.source(1).discover().await.unwrap(),
        pir_service_discovery::Version::new(1, 9, 0)
    );
    assert!(tls
        .requests
        .lock()
        .unwrap()
        .iter()
        .all(|r| r.starts_with("GET /.well-known/pir-service-discovery ")));
}

#[tokio::test]
async fn discovery_has_a_whole_request_deadline() {
    let tls = Tls::new().await;
    *tls.discovery.lock().unwrap() = "slow-body".into();
    let source = tls.source(100);
    // Real DNS/TCP/TLS setup must run with real time. A paused Tokio clock can
    // expire the connection timeout before the server receives the request.
    let started = std::time::Instant::now();
    let error = tokio::time::timeout(
        DISCOVERY_DEADLINE + Duration::from_secs(5),
        source.discover(),
    )
    .await
    .expect("discovery did not stop while the response body was stalled")
    .unwrap_err();
    assert!(started.elapsed() >= DISCOVERY_DEADLINE);
    assert_eq!(tls.requests.lock().unwrap().len(), 1);
    assert!(error.to_string().contains("within 10s"), "{error}");
    assert!(error.incompatible().is_none());
}

#[tokio::test]
async fn streamed_objects_preserve_length_digest_and_aggregate_budget() {
    let tls = Tls::new().await;
    let digest = "bef57ec7f53a6d40beb640a780a639c83bc29ac8a9816f1fc6c5c6dcd93c4721";
    let mut source = tls.source(100);
    for (length, verification, succeeds) in [
        (6, Verification::Sha256(digest), true),
        (6, Verification::Length, true),
        (7, Verification::Length, false),
        (5, Verification::Length, false),
        (6, Verification::Sha256(&"00".repeat(32)), false),
    ] {
        let mut output = Vec::new();
        let result = source
            .request("ok", None)
            .await
            .unwrap()
            .copy_to(&mut output, length, verification)
            .await;
        assert_eq!(result.is_ok(), succeeds);
        if let Ok(report) = result {
            assert_eq!(report.bytes, 6);
            assert_eq!(output, b"abcdef");
        }
    }
    assert_eq!(source.downloaded(), 30);
    let mut source = tls.source(10);
    source
        .request("ok", None)
        .await
        .unwrap()
        .copy_to(&mut Vec::new(), 6, Verification::Length)
        .await
        .unwrap();
    assert!(source
        .request("ok", None)
        .await
        .unwrap()
        .copy_to(&mut Vec::new(), 6, Verification::Length)
        .await
        .is_err());
    assert_eq!(source.downloaded(), 12);
}

#[tokio::test]
async fn streamed_objects_propagate_writer_errors_and_reject_refused_bodies() {
    let tls = Tls::new().await;
    let mut source = tls.source(100);
    let (mut closed_writer, reader) = tokio::io::duplex(2);
    drop(reader);
    let error = source
        .request("ok", None)
        .await
        .unwrap()
        .copy_to(&mut closed_writer, 6, Verification::Length)
        .await
        .err()
        .unwrap();
    assert!(error.downcast_ref::<std::io::Error>().is_some());
    let mut output = Vec::new();
    assert!(source
        .request("fail", None)
        .await
        .unwrap()
        .copy_to(&mut output, 7, Verification::Length)
        .await
        .is_err());
    assert!(output.is_empty());
    assert_eq!(source.downloaded(), 13);
}

#[tokio::test]
async fn artifact_replacement_and_capacity_are_explicit_transfer_policies() {
    use crate::objects::ObjectFetcher;
    use pir_publication::{ObjectCache, ObjectReference};
    let tls = Tls::new().await;
    let root = tempfile::tempdir().unwrap();
    let cache = ObjectCache::open(root.path().into()).unwrap();
    let object = ObjectReference::for_bytes(b"abcdef");
    let path = cache.object_path(&object).unwrap();
    std::fs::write(&path, b"ABCDEF").unwrap();
    let mut source = tls.source(100);
    assert!(ObjectFetcher::new(&mut source, &cache)
        .fetch(&object)
        .await
        .is_err());
    assert!(
        tls.requests.lock().unwrap().is_empty(),
        "refusal does not repair silently"
    );
    let checked = ObjectFetcher::new(&mut source, &cache)
        .with_corruption(crate::objects::CorruptionPolicy::ReplaceFile)
        .with_cache_limit(6)
        .fetch(&object)
        .await
        .unwrap();
    assert_eq!(checked.file.read_bytes().unwrap(), b"abcdef");
    assert_eq!(tls.requests.lock().unwrap().len(), 1);
    std::fs::remove_file(path).unwrap();
    let count = tls.requests.lock().unwrap().len();
    assert!(ObjectFetcher::new(&mut source, &cache)
        .with_cache_limit(5)
        .fetch(&object)
        .await
        .is_err());
    assert_eq!(
        tls.requests.lock().unwrap().len(),
        count,
        "capacity is reserved before downloading"
    );
}
