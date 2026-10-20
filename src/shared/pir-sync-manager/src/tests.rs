use super::*;
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

struct Tls {
    origin: String,
    cert: reqwest::Certificate,
    requests: Arc<Mutex<Vec<String>>>,
    /// Discovery response selector: ok, missing, redirect, oversize, slow or
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
                        "slow" => {
                            tokio::time::sleep(Duration::from_secs(30)).await;
                            ("200 OK", "", String::new())
                        }
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
                seen.lock().unwrap().push(request);
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

#[derive(Default)]
struct Probe {
    calls: usize,
    events: Vec<&'static str>,
    completed: Vec<(tokio::time::Instant, bool)>,
}
struct Task(Arc<Mutex<Probe>>);
impl SyncTask for Task {
    type Report = ();
    type Error = ();
    fn started(&mut self) {
        self.0.lock().unwrap().events.push("started");
    }
    async fn synchronize(&mut self) -> Result<(), ()> {
        let n = {
            let mut p = self.0.lock().unwrap();
            p.calls += 1;
            p.events.push("work");
            p.calls
        };
        tokio::time::sleep(Duration::from_secs(2)).await;
        if (2..=7).contains(&n) {
            Err(())
        } else {
            Ok(())
        }
    }
    fn finished(&mut self, result: &Result<(), ()>, _: SystemTime, elapsed: Duration) {
        assert_eq!(elapsed, Duration::from_secs(2));
        let mut p = self.0.lock().unwrap();
        p.events.push("finished");
        p.completed
            .push((tokio::time::Instant::now(), result.is_ok()));
    }
}
#[tokio::test(start_paused = true)]
async fn polling_is_sequential_and_backs_off_after_complete_passes() {
    let probe = Arc::new(Mutex::new(Probe::default()));
    let manager = SyncManager::new(Task(probe.clone()));
    let running = tokio::spawn(manager.run());
    // Explicit bounded advances keep the continuously running task from making
    // the test's paused clock auto-advance without an observation limit.
    for _ in 0..850 {
        tokio::time::advance(Duration::from_secs(1)).await;
        tokio::task::yield_now().await;
    }
    running.abort();
    let _ = running.await;
    let p = probe.lock().unwrap();
    assert!(p.completed.len() >= 9);
    for (pair, failures) in p.completed.windows(2).zip([0, 1, 2, 3, 4, 5, 6, 0]) {
        let gap = pair[1].0 - pair[0].0;
        let minimum = delay(failures, 0) + Duration::from_secs(2);
        assert!(
            gap >= minimum && gap <= minimum + Duration::from_secs(2),
            "unexpected gap {gap:?}"
        );
    }
    assert!(
        !p.completed[1].1 && p.completed[7].1,
        "a success must reset backoff"
    );
    for events in p.events.chunks_exact(3) {
        assert_eq!(events, ["started", "work", "finished"]);
    }
    assert_eq!(delay(u32::MAX, 999), Duration::from_millis(120999));
    assert_eq!(delay(0, 1000), Duration::from_secs(5));
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

#[tokio::test(start_paused = true)]
async fn discovery_has_a_whole_request_deadline() {
    let tls = Tls::new().await;
    *tls.discovery.lock().unwrap() = "slow".into();
    let source = tls.source(100);
    let error = source.discover().await.unwrap_err();
    assert!(error.to_string().contains("within 10s"), "{error}");
    assert!(error.incompatible().is_none());
}

struct Terminal(usize);
impl SyncTask for Terminal {
    type Report = ();
    type Error = &'static str;
    async fn synchronize(&mut self) -> Result<(), &'static str> {
        self.0 += 1;
        match self.0 {
            1 => Ok(()),
            2 | 3 => Err("temporary"),
            _ => Err("fatal"),
        }
    }
    fn terminal(&self, error: &&'static str) -> bool {
        *error == "fatal"
    }
}
#[tokio::test(start_paused = true)]
async fn continuous_sync_retries_ordinary_failures_and_returns_a_terminal_error() {
    let manager = SyncManager::new(Terminal(0));
    assert_eq!(manager.run().await, "fatal");
}
