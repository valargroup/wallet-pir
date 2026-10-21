use super::*;
use transparent_shared::history::shardset::Table;
use transparent_shared::runtime::metrics::Metrics;

fn scope() -> Scope {
    Scope {
        core: new_core(),
        revisions: BTreeMap::new(),
        errors: BTreeMap::new(),
    }
}
fn cache() -> Arc<RuntimeCache> {
    Arc::new(RuntimeCache::new(1 << 20, 1, Arc::new(Metrics::default())))
}
fn key(revision: &str) -> RuntimeKey {
    (revision.into(), Table::Directory, 0)
}
fn artifact(root: &Path, good: &[u8], actual: &[u8]) -> (PathBuf, publication::Artifact) {
    let objects = root.join("objects");
    std::fs::create_dir_all(&objects).unwrap();
    let artifact = publication::Artifact {
        path: "rows.bin".into(),
        sha256: publication::digest(good),
        bytes: good.len() as u64,
        kind: publication::Kind::RecentTable,
    };
    let path = objects.join(&artifact.sha256);
    std::fs::write(&path, actual).unwrap();
    publication::record_received_file(&path, &artifact, &path).unwrap();
    (path, artifact)
}
async fn phase(job: &Job, phase: u8) {
    tokio::time::timeout(Duration::from_secs(3), async {
        while job.phase.load(Ordering::Acquire) != phase {
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
    })
    .await
    .expect("background validation completed");
}

#[tokio::test]
async fn receipt_and_activation_do_not_hash_while_queries_have_priority() {
    let root = tempfile::tempdir().unwrap();
    let (path, _) = artifact(root.path(), b"expected", b"wrong!!!");
    // Length is a foreground requirement, so use equally sized wrong bytes.
    std::fs::write(&path, b"wrong!!!").unwrap();
    let mut scope = scope();
    scope.add("first", 1, &path, &cache(), &key("first"));
    let job = scope.revisions["first"].1[0].clone();
    assert!(scope.core.shared.state.lock().unwrap().queue.is_empty());
    let query = activity(scope.core.clone());
    scope.activate();
    // A synchronization barrier, not a timing assertion: every idle check
    // observes this admission until its guard is released.
    assert_eq!(scope.core.shared.state.lock().unwrap().queries, 1);
    assert_eq!(job.phase.load(Ordering::Acquire), PENDING);
    assert!(!scope.unavailable("first"));
    drop(query);
    phase(&job, FAILED).await;
    assert!(scope.unavailable("first"));
}

#[tokio::test]
async fn checked_hard_links_reuse_validation_after_restart_but_copies_do_not() {
    let root = tempfile::tempdir().unwrap();
    let (path, artifact) = artifact(root.path(), b"correct!", b"correct!");
    let cache = cache();
    let mut first = scope();
    first.add("first", 1, &path, &cache, &key("first"));
    let job = first.revisions["first"].1[0].clone();
    first.activate();
    phase(&job, PASSED).await;
    let linked = root.path().join("linked");
    std::fs::hard_link(&path, &linked).unwrap();
    publication::record_received_file(&linked, &artifact, &path).unwrap();
    let mut reopened = scope();
    reopened.add("next", 2, &linked, &cache, &key("next"));
    assert_eq!(reopened.status()["validated"], 1);
    reopened.activate();
    assert!(reopened.core.shared.state.lock().unwrap().queue.is_empty());

    let copy = root.path().join("copy");
    std::fs::write(&copy, b"wrong!!!").unwrap();
    publication::record_received_file(&copy, &artifact, &path).unwrap();
    reopened.add("copy", 3, &copy, &cache, &key("copy"));
    let copied = reopened.revisions["copy"].1[0].clone();
    reopened.activate();
    phase(&copied, FAILED).await;
    assert!(reopened.unavailable("copy"));
    assert!(!reopened.unavailable("next"));
}

#[tokio::test]
async fn shared_failures_withdraw_only_dependent_shards_and_survive_restart() {
    let root = tempfile::tempdir().unwrap();
    let (bad, _) = artifact(root.path(), b"correct!", b"wrong!!!");
    let (good, _) = artifact(root.path(), b"other!!!", b"other!!!");
    let cache = cache();
    let mut scope = scope();
    scope.add("one", 1, &bad, &cache, &key("one"));
    scope.add("two", 2, &bad, &cache, &key("two"));
    scope.add("three", 3, &good, &cache, &key("three"));
    assert!(Arc::ptr_eq(
        &scope.revisions["one"].1[0],
        &scope.revisions["two"].1[0]
    ));
    scope.activate();
    phase(&scope.revisions["one"].1[0], FAILED).await;
    assert!(scope.unavailable("one") && scope.unavailable("two"));
    assert!(!scope.unavailable("three"));
    let mut restart = super::tests::scope();
    restart.add("one", 1, &bad, &cache, &key("one"));
    assert!(
        restart.unavailable("one"),
        "known failure cannot serve optimistically after restart"
    );
}

#[tokio::test]
async fn a_replaced_object_cannot_receive_an_old_check_result() {
    let root = tempfile::tempdir().unwrap();
    let (path, _) = artifact(root.path(), b"correct!", b"correct!");
    let cache = cache();
    let mut scope = scope();
    scope.add("one", 1, &path, &cache, &key("one"));
    let replacement = root.path().join("replacement");
    std::fs::write(&replacement, b"correct!").unwrap();
    std::fs::rename(replacement, &path).unwrap();
    scope.activate();
    phase(&scope.revisions["one"].1[0], FAILED).await;
}

#[tokio::test]
async fn withdrawal_is_checked_again_before_a_query_response_is_released() {
    use axum::{body::Body, routing::get, Router};
    use tower::ServiceExt;
    let root = tempfile::tempdir().unwrap();
    let (path, _) = artifact(root.path(), b"correct!", b"correct!");
    let mut scope = scope();
    scope.add("one", 1, &path, &cache(), &key("one"));
    let job = scope.revisions["one"].1[0].clone();
    let scope = Arc::new(scope);
    let routed = scope.clone();
    let app = Router::new()
        .route(
            "/v2/shards/1/revisions/one/query/table",
            get(move || {
                let job = job.clone();
                async move {
                    job.fail(&io::Error::other(
                        "failure discovered while a query was executing",
                    ));
                    "answer that must not be released"
                }
            }),
        )
        .layer(axum::middleware::from_fn(move |request, next| {
            let scope = routed.clone();
            async move { scope.serve(request, next).await }
        }));
    let response = app
        .oneshot(
            axum::http::Request::builder()
                .uri("/v2/shards/1/revisions/one/query/table")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        axum::http::StatusCode::SERVICE_UNAVAILABLE
    );
}

#[tokio::test]
async fn real_https_defers_bulk_digest_checks_and_repairs_before_reenabling() {
    use axum::{routing::get, Router};
    use std::process::Stdio;
    use tokio::io::{AsyncBufReadExt, BufReader};
    let temporary = tempfile::tempdir().unwrap();
    let root = std::fs::canonicalize(temporary.path()).unwrap();
    let generated = tokio::process::Command::new("openssl")
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
            root.join("key.pem").to_str().unwrap(),
            "-out",
            root.join("ca.pem").to_str().unwrap(),
        ])
        .output()
        .await
        .unwrap();
    assert!(
        generated.status.success(),
        "{}",
        String::from_utf8_lossy(&generated.stderr)
    );
    let body = Arc::new(Mutex::new(b"wrong!!!".to_vec()));
    let reply = body.clone();
    let app = Router::new().fallback(get(move || {
        let reply = reply.clone();
        async move { reply.lock().unwrap().clone() }
    }));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let mut tls = tokio::process::Command::new("python3")
        .arg(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/support/provider_tls.py"
        ))
        .arg(address.to_string())
        .arg(root.join("ca.pem"))
        .arg(root.join("key.pem"))
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()
        .unwrap();
    let mut port = String::new();
    tokio::time::timeout(
        Duration::from_secs(5),
        BufReader::new(tls.stdout.take().unwrap()).read_line(&mut port),
    )
    .await
    .unwrap()
    .unwrap();
    let config = crate::source::Config {
        publisher: format!("https://localhost:{}", port.trim()),
        network: "main".into(),
        genesis_hash: "11".repeat(32),
        directory: root.clone(),
        cache_bytes: 1 << 20,
        max_download_bytes: 1 << 20,
        roots: vec![
            reqwest::Certificate::from_pem(&std::fs::read(root.join("ca.pem")).unwrap()).unwrap(),
        ],
    };
    let mut source = crate::source::Source::for_product(&config, "/distribution/", "test").unwrap();
    let objects = crate::objects::prepare_objects(&root).unwrap();
    let artifact = publication::Artifact {
        path: "rows.bin".into(),
        sha256: publication::digest(b"correct!"),
        bytes: 8,
        kind: publication::Kind::RecentTable,
    };
    let mut transfer = Duration::ZERO;
    for bytes in [7, 9] {
        let wrong_length = publication::Artifact {
            bytes,
            ..artifact.clone()
        };
        assert!(
            crate::objects::ensure_object(&mut source, &objects, &wrong_length, &mut transfer)
                .await
                .is_err()
        );
        assert!(!objects.join(&artifact.sha256).exists());
        assert_eq!(std::fs::read_dir(&objects).unwrap().count(), 0);
    }
    let metadata = publication::Artifact {
        kind: publication::Kind::Metadata,
        ..artifact.clone()
    };
    assert!(
        crate::objects::ensure_object(&mut source, &objects, &metadata, &mut transfer)
            .await
            .is_err()
    );
    assert!(
        crate::objects::ensure_object(&mut source, &objects, &artifact, &mut transfer)
            .await
            .unwrap()
    );
    let object = objects.join(&artifact.sha256);
    let alias = root.join("alias");
    crate::objects::link_object(&object, &alias, &artifact, |a, b| std::fs::hard_link(a, b))
        .unwrap();
    let cache = cache();
    let mut scope = scope();
    scope.add("one", 1, &alias, &cache, &key("one"));
    let job = scope.revisions["one"].1[0].clone();
    assert!(!scope.unavailable("one"));
    scope.activate();
    phase(&job, FAILED).await;
    repair(&scope.core, &mut source, &root).await;
    phase(&job, FAILED).await;
    assert!(
        scope.unavailable("one"),
        "a bad replacement cannot re-enable a shard"
    );
    *body.lock().unwrap() = b"correct!".to_vec();
    let query = activity(scope.core.clone());
    repair(&scope.core, &mut source, &root).await;
    assert_eq!(
        job.phase.load(Ordering::Acquire),
        FAILED,
        "repair yields to query work"
    );
    drop(query);
    repair(&scope.core, &mut source, &root).await;
    phase(&job, PASSED).await;
    assert!(!scope.unavailable("one"));
    assert_eq!(std::fs::read(&object).unwrap(), b"correct!");
    assert_eq!(std::fs::read(&alias).unwrap(), b"correct!");
    tls.kill().await.unwrap();
    let _ = tls.wait().await;
    server.abort();
}
