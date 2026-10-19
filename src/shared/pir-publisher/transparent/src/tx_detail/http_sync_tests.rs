//! Real HTTPS full-coverage display synchronization and durable recovery.
use super::{distribution::Distribution, fixture, publisher::bootstrap};
use crate::history::controller::distribution_tests::tls_router;
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tower::ServiceExt;
use transparent_query_server::{
    config::ServiceConfig, routing::bootstrap::Config, tx_detail::manager::Manager,
};
use transparent_shared::journal::events::EventStore;

use super::display_world;

fn manager(root: &Path, origin: &str, cert: reqwest::Certificate) -> Manager {
    Manager::new(
        Config {
            publisher: origin.into(),
            network: "main".into(),
            genesis_hash: fixture::GENESIS.into(),
            directory: root.into(),
            cache_bytes: 4 << 30,
            max_download_bytes: 4 << 30,
            roots: vec![cert],
        },
        ServiceConfig {
            cache_bytes: 4 << 30,
            ..Default::default()
        },
    )
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn full_display_consumers_follow_https_and_restart_without_building() {
    let scratch = tempfile::tempdir().unwrap();
    let journal = scratch.path().join("journal");
    fixture::write_journal(&journal, 0, 8, 0);
    let store = EventStore::open_existing(&journal).unwrap();
    let root = scratch.path().join("publisher");
    let initial = bootstrap(&store, &root, &fixture::layout(&store, 1, 16), 7).unwrap();
    assert!(
        !initial.seals.is_empty(),
        "the fixture must include archive holdings"
    );
    let canonical = Arc::new(AtomicBool::new(true));
    let state = canonical.clone();
    let node = axum::Router::new().route(
        "/",
        axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            let canonical = state.clone();
            async move {
                assert_eq!(request["method"], "getblockhash");
                let height = request["params"][0].as_u64().unwrap();
                let hash = if height == 0 {
                    fixture::GENESIS.to_string()
                } else if canonical.load(Ordering::Acquire) {
                    fixture::block_hash(0, height).to_display_hex()
                } else {
                    "11".repeat(32)
                };
                axum::Json(serde_json::json!({"id":request["id"],"result":hash,"error":null}))
            }
        }),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let node_task = tokio::spawn(async move {
        axum::serve(listener, node).await.unwrap();
    });
    let cookie = scratch.path().join("cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let rpc = crate::ingest::zakura::connect(format!("http://{address}"), cookie).unwrap();
    let distribution = Distribution::new(&root, rpc).unwrap();
    distribution
        .finalize(initial.candidate.clone(), vec![])
        .await
        .unwrap();
    let tls_root = scratch.path().join("tls");
    std::fs::create_dir(&tls_root).unwrap();
    let tls = tls_router(&tls_root, distribution.router()).await;
    for fault in [
        "redirect",
        "missing",
        "corrupt",
        "truncated",
        "malformed-current",
        "runtime",
        "unsafe-path",
    ] {
        std::fs::write(&tls.control, fault).unwrap();
        let mut refused = manager(
            &scratch.path().join(format!("refused-{fault}")),
            &tls.origin,
            tls.cert.clone(),
        );
        assert!(refused.poll_once().await.is_err(), "accepted {fault}");
        assert!(!refused.ready().await, "served {fault}");
    }
    std::fs::write(&tls.control, "").unwrap();
    let first_root = scratch.path().join("first");
    let mut first = manager(&first_root, &tls.origin, tls.cert.clone());
    let mut second = manager(
        &scratch.path().join("second"),
        &tls.origin,
        tls.cert.clone(),
    );
    first.poll_once().await.unwrap();
    second.poll_once().await.unwrap();
    assert!(first.ready().await && second.ready().await);
    assert_eq!(first.poll_once().await.unwrap()["unchanged"], true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let router = first.router();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let query_task = tokio::spawn(async move {
        axum::serve(listener, router)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap();
    });
    let mut wallet = display_world::Wallet::at(&origin);
    let mut old_query = None;
    for height in [1, 4, 7] {
        let (_, transcript) = wallet.found(&fixture::chain_records(0, height)[0], height);
        old_query = transcript
            .into_iter()
            .find(|request| request.path.contains("/recent/") && request.path.contains("/query/"))
            .or(old_query);
    }
    let old_query = old_query.expect("the wallet queried the serving recent revision");
    let metrics = reqwest::get(format!("{origin}/metrics"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    let builds = metrics
        .lines()
        .find(|line| line.starts_with("transparent_shard_builds_total"))
        .and_then(|line| line.split_whitespace().last())
        .expect("runtime build counter is required");
    assert_eq!(
        builds, "0",
        "managed servers must not construct PIR runtimes"
    );
    tokio::task::block_in_place(|| drop(wallet));
    stop.send(()).unwrap();
    query_task.await.unwrap();
    drop(first);
    let mut restarted = manager(&first_root, &tls.origin, tls.cert.clone());
    let result = restarted.poll_once().await.unwrap();
    assert!(restarted.ready().await);
    // The budget includes live metadata and attestations, not cached runtime bodies.
    assert!(result["downloaded_bytes"].as_u64().unwrap() < 1 << 20);
    // The serving recent revision must differ from the incoming candidate:
    // withdrawing only the candidate must not leave its predecessor answerable.
    let next = bootstrap(
        &store,
        &scratch.path().join("next"),
        &fixture::layout(&store, 1, 16),
        8,
    )
    .unwrap();
    let old = initial.map.shards.last().unwrap();
    assert_ne!(
        old.manifest_digest,
        next.map.shards.last().unwrap().manifest_digest
    );
    let old_request = || {
        axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri(&old_query.path)
            .body(axum::body::Body::from(old_query.body.clone()))
            .unwrap()
    };
    assert_eq!(
        second
            .router()
            .oneshot(old_request())
            .await
            .unwrap()
            .status(),
        axum::http::StatusCode::OK
    );
    distribution
        .finalize(next.candidate.clone(), vec![initial.candidate.clone()])
        .await
        .unwrap();
    // Change authority after staging but before the checked activation commits.
    std::fs::write(&tls.control, "pause-current-2").unwrap();
    let checking = tokio::spawn(async move {
        let result = second.poll_once().await;
        (second, result)
    });
    // A different map performs real native preparation before this check;
    // the former same-map case skipped that work.
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !tls.control.with_extension("entered").exists() && !checking.is_finished() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if !tls.control.with_extension("entered").exists() {
        let (_, result) = checking.await.unwrap();
        panic!("synchronization ended before the activation check: {result:?}");
    }
    distribution.withdraw(0).unwrap();
    std::fs::write(&tls.control, "").unwrap();
    let (mut second, result) = checking.await.unwrap();
    assert!(result.is_err(), "a withdrawn candidate must not activate");
    assert_eq!(
        second
            .router()
            .oneshot(old_request())
            .await
            .unwrap()
            .status(),
        axum::http::StatusCode::CONFLICT,
        "a withdrawn predecessor must stop answering immediately"
    );
    assert!(
        !second.ready().await,
        "withdrawal must fence the previous view too"
    );
    distribution
        .finalize(next.candidate.clone(), vec![initial.candidate.clone()])
        .await
        .unwrap();
    second.poll_once().await.unwrap();
    assert!(second.ready().await);
    canonical.store(false, Ordering::Release);
    assert!(restarted.poll_once().await.is_err());
    assert!(second.poll_once().await.is_err());
    drop(restarted);
    let mut refused = manager(&first_root, &tls.origin, tls.cert.clone());
    assert!(refused.poll_once().await.is_err());
    assert!(
        !refused.ready().await,
        "saved bytes cannot revive revoked service"
    );
    node_task.abort();
}
