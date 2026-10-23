//! Real HTTPS full-coverage display synchronization and durable recovery.
use super::{distribution::Distribution, fixture, publisher::bootstrap};
use crate::history::controller::distribution_tests::tls_router;
use pir_query_server::transparent::{
    config::ServiceConfig, routing::bootstrap::Config, tx_detail::manager::Manager,
};
use std::{
    path::Path,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};
use tower::ServiceExt;
use transparent_shared::journal::events::EventStore;

use super::display_world;

async fn manager(root: &Path, origin: &str, cert: reqwest::Certificate) -> Manager {
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
    .await
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
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            initial.candidate.clone(),
            vec![],
        )
        .await
        .unwrap());
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
        )
        .await;
        assert!(refused.poll_once().await.is_err(), "accepted {fault}");
        assert!(!refused.ready().await, "served {fault}");
    }
    std::fs::write(&tls.control, "").unwrap();
    let first_root = scratch.path().join("first");
    let mut first = manager(&first_root, &tls.origin, tls.cert.clone()).await;
    let mut second = manager(
        &scratch.path().join("second"),
        &tls.origin,
        tls.cert.clone(),
    )
    .await;
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
    let mut restarted = manager(&first_root, &tls.origin, tls.cert.clone()).await;
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
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            next.candidate.clone(),
            vec![initial.candidate.clone()],
        )
        .await
        .unwrap());
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
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            next.candidate.clone(),
            vec![initial.candidate.clone()],
        )
        .await
        .unwrap());
    second.poll_once().await.unwrap();
    assert!(second.ready().await);
    canonical.store(false, Ordering::Release);
    assert!(restarted.poll_once().await.is_err());
    assert!(second.poll_once().await.is_err());
    drop(restarted);
    let mut refused = manager(&first_root, &tls.origin, tls.cert.clone()).await;
    assert!(refused.poll_once().await.is_err());
    assert!(
        !refused.ready().await,
        "saved bytes cannot revive revoked service"
    );
    node_task.abort();
}

/// An orphaned saved tip is an input to recovery, never startup authority.
#[tokio::test(flavor = "multi_thread")]
async fn http_display_publisher_recovers_after_an_offline_reorg() {
    use super::{
        controller::{Controller, Outcome, Settings},
        source::{open_writer, Script, ScriptStep, Source},
    };

    for journal_already_rolled_back in [false, true] {
        let scratch = tempfile::tempdir().unwrap();
        let journal = scratch.path().join("journal");
        fixture::write_journal(&journal, 0, 3, 0);
        let root = scratch.path().join("publisher");
        let store = EventStore::open_existing(&journal).unwrap();
        let mut layout = fixture::layout(&store, 1, 16);
        layout.seal.archive_target = 1_000;
        let initial = bootstrap(&store, &root, &layout, 3).unwrap();
        drop(store);
        let fork_hash = fixture::block_hash(1, 3);
        let wrong_genesis = Arc::new(AtomicBool::new(false));
        let state = wrong_genesis.clone();
        let node = axum::Router::new().route(
            "/",
            axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
                let wrong_genesis = state.clone();
                async move {
                    assert_eq!(request["method"], "getblockhash");
                    let height = request["params"][0].as_u64().unwrap();
                    let hash = match height {
                        0 if wrong_genesis.load(Ordering::Acquire) => "11".repeat(32),
                        0 => fixture::GENESIS.to_string(),
                        3 => fork_hash.to_display_hex(),
                        _ => fixture::block_hash(0, height).to_display_hex(),
                    };
                    axum::Json(serde_json::json!({"id":request["id"],"result":hash,"error":null}))
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let node_task = tokio::spawn(async move { axum::serve(listener, node).await.unwrap() });
        let cookie = scratch.path().join("cookie");
        std::fs::write(&cookie, "fixture:fixture").unwrap();
        let rpc = crate::ingest::zakura::connect(format!("http://{address}"), cookie).unwrap();
        let distribution = Distribution::new(&root, rpc).unwrap();
        let records = fixture::chain_sources(1, 3);
        let steps = if journal_already_rolled_back {
            // A crash after journal replacement left the old active pointer.
            let mut store = open_writer(&journal).unwrap();
            store.rollback_to(Some(2)).unwrap();
            let events =
                transparent_shared::journal::display_journal::implied_events(3, &records).unwrap();
            store
                .append_block_with_display(3, fork_hash, &events, &records)
                .unwrap();
            store.commit().unwrap();
            vec![]
        } else {
            // The chain changed while stopped; normal source polling must run.
            vec![ScriptStep::Reorg {
                ancestor: 2,
                blocks: vec![(fork_hash, records)],
            }]
        };
        let mut controller = Controller::open(
            &root,
            open_writer(&journal).unwrap(),
            Source::Script(Script::new(steps)),
            Settings {
                distribution: Some(distribution.clone()),
                exit_when_idle: true,
                ..Settings::default()
            },
        )
        .unwrap();
        assert_eq!(
            controller
                .run(crate::host_engine::PublisherEngine)
                .await
                .unwrap(),
            Outcome::Idle
        );
        assert_ne!(controller.active().map_sha256, initial.map_sha256);
        assert_eq!(controller.active().tip_hash, fork_hash.to_display_hex());
        let response = distribution
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/provider/txid/v1/current")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), axum::http::StatusCode::OK);
        let current: transparent_shared::publication::Current = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 16 << 10)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(current.eligible());
        assert_eq!(current.anchor_hash, fork_hash.to_display_hex());
        drop(controller);
        wrong_genesis.store(true, Ordering::Release);
        let mut refused = Controller::open(
            &root,
            open_writer(&journal).unwrap(),
            Source::Script(Script::new([])),
            Settings {
                distribution: Some(distribution.clone()),
                exit_when_idle: true,
                ..Settings::default()
            },
        )
        .unwrap();
        assert!(refused
            .run(crate::host_engine::PublisherEngine)
            .await
            .unwrap_err()
            .to_string()
            .contains("genesis differs"));
        node_task.abort();
    }
}

#[derive(Clone, Copy)]
enum Recheck {
    AfterPreparation,
    FloorWriteFailure,
    AfterAnchors,
    CandidateAnchor,
}

#[tokio::test(flavor = "multi_thread")]
async fn display_rechecks_serving_anchors_after_preparation_skips_a_withdrawal() {
    recheck_after_recovery(Recheck::AfterPreparation, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn display_rechecks_serving_anchors_after_final_current_skips_a_withdrawal() {
    recheck_after_recovery(Recheck::AfterAnchors, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn display_rechecks_serving_anchors_after_a_candidate_anchor_turns_noncanonical() {
    recheck_after_recovery(Recheck::CandidateAnchor, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn display_rechecks_fence_memory_when_the_revocation_floor_cannot_be_saved() {
    recheck_after_recovery(Recheck::FloorWriteFailure, true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn display_rechecks_preserve_a_canonical_predecessor_when_an_update_is_superseded() {
    recheck_after_recovery(Recheck::AfterPreparation, false).await;
}

async fn recheck_after_recovery(phase: Recheck, reorg: bool) {
    let scratch = tempfile::tempdir().unwrap();
    let journal = scratch.path().join("journal");
    fixture::write_journal(&journal, 0, 4, 0);
    let store = EventStore::open_existing(&journal).unwrap();
    let mut layout = fixture::layout(&store, 1, 16);
    layout.seal.archive_target = 1_000;
    let root = scratch.path().join("publisher");
    let initial = bootstrap(&store, &root, &layout, 3).unwrap();
    let next = bootstrap(&store, &scratch.path().join("next"), &layout, 4).unwrap();
    let fork_journal = scratch.path().join("fork-journal");
    fixture::write_journal(&fork_journal, 0, 2, 0);
    {
        let mut fork = super::source::open_writer(&fork_journal).unwrap();
        for height in 3..=4 {
            let records = fixture::chain_sources(1, height);
            let events =
                transparent_shared::journal::display_journal::implied_events(height, &records)
                    .unwrap();
            fork.append_block_with_display(
                height,
                fixture::block_hash(1, height),
                &events,
                &records,
            )
            .unwrap();
        }
        fork.commit().unwrap();
    }
    let recovered = bootstrap(
        &EventStore::open_existing(&fork_journal).unwrap(),
        &scratch.path().join("recovered"),
        &layout,
        4,
    )
    .unwrap();
    let forked = Arc::new(AtomicBool::new(false));
    let state = forked.clone();
    let node = axum::Router::new().route(
        "/",
        axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            let forked = state.clone();
            async move {
                assert_eq!(request["method"], "getblockhash");
                let height = request["params"][0].as_u64().unwrap();
                let tag = u8::from(height >= 3 && forked.load(Ordering::Acquire));
                let hash = if height == 0 {
                    fixture::GENESIS.to_string()
                } else {
                    fixture::block_hash(tag, height).to_display_hex()
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
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            initial.candidate.clone(),
            vec![],
        )
        .await
        .unwrap());
    let tls_root = scratch.path().join("tls");
    std::fs::create_dir(&tls_root).unwrap();
    let tls = tls_router(&tls_root, distribution.router()).await;
    let consumer_root = scratch.path().join("consumer");
    let mut consumer = manager(&consumer_root, &tls.origin, tls.cert.clone()).await;
    consumer.poll_once().await.unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let origin = format!("http://{}", listener.local_addr().unwrap());
    let router = consumer.router();
    let query_task = tokio::spawn(async move {
        axum::serve(listener, router).await.unwrap();
    });
    let mut wallet = display_world::Wallet::at(&origin);
    let (_, transcript) = wallet.found(&fixture::chain_records(0, 3)[0], 3);
    let query = transcript
        .into_iter()
        .find(|r| r.path.contains("/query/"))
        .unwrap();
    tokio::task::block_in_place(|| drop(wallet));
    let old_query = || {
        axum::http::Request::builder()
            .method(axum::http::Method::POST)
            .uri(&query.path)
            .body(axum::body::Body::from(query.body.clone()))
            .unwrap()
    };
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            next.candidate.clone(),
            vec![initial.candidate.clone()],
        )
        .await
        .unwrap());
    let pause = match phase {
        Recheck::AfterPreparation | Recheck::FloorWriteFailure => "pause-current-2".to_string(),
        Recheck::AfterAnchors => "pause-current-3".to_string(),
        Recheck::CandidateAnchor => format!(
            "pause-anchor-{}",
            fixture::block_hash(0, 4).to_display_hex()
        ),
    };
    std::fs::write(&tls.control, pause).unwrap();
    let checking = tokio::spawn(async move {
        let result = consumer.poll_once().await;
        (consumer, result)
    });
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !tls.control.with_extension("entered").exists() && !checking.is_finished() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if !tls.control.with_extension("entered").exists() {
        let (_, result) = checking.await.unwrap();
        panic!("synchronization ended before the authority check: {result:?}");
    }
    forked.store(reorg, Ordering::Release);
    if !matches!(phase, Recheck::CandidateAnchor) {
        if reorg {
            distribution.withdraw(3).unwrap();
        }
        let offered = if reorg { &recovered } else { &next };
        assert!(distribution
            .restore(
                crate::host_engine::PublisherEngine,
                offered.candidate.clone(),
                vec![initial.candidate.clone()],
            )
            .await
            .unwrap());
        // The consumer receives the recovered canonical pointer, not the
        // intervening withdrawal. Negative ranges are not a cumulative log.
        let response = distribution
            .router()
            .oneshot(
                axum::http::Request::builder()
                    .uri("/provider/txid/v1/current")
                    .body(axum::body::Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let current: transparent_shared::publication::Current = serde_json::from_slice(
            &axum::body::to_bytes(response.into_body(), 16 << 10)
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(current.eligible() && current.invalidated_from.is_none());
    }
    let failed_floor = consumer_root.join("revocation-floor.json");
    if matches!(phase, Recheck::FloorWriteFailure) {
        std::fs::create_dir(&failed_floor).unwrap();
    }
    std::fs::write(&tls.control, "").unwrap();
    let (mut consumer, result) = checking.await.unwrap();
    assert!(result.is_err(), "the prepared candidate lost authority");
    let expected = if reorg {
        axum::http::StatusCode::CONFLICT
    } else {
        axum::http::StatusCode::OK
    };
    assert_eq!(
        consumer
            .router()
            .oneshot(old_query())
            .await
            .unwrap()
            .status(),
        expected,
        "serving predecessors must be checked even when the withdrawal was skipped"
    );
    assert_eq!(consumer.ready().await, !reorg);
    if matches!(phase, Recheck::FloorWriteFailure) {
        std::fs::remove_dir(&failed_floor).unwrap();
    }
    if matches!(phase, Recheck::CandidateAnchor) {
        assert!(distribution
            .restore(
                crate::host_engine::PublisherEngine,
                recovered.candidate.clone(),
                vec![initial.candidate.clone()],
            )
            .await
            .unwrap());
    }
    consumer.poll_once().await.unwrap();
    assert!(
        consumer.ready().await,
        "fresh canonical data can recover service"
    );
    query_task.abort();
    let _ = query_task.await;
    drop(consumer);
    let mut restarted = manager(&consumer_root, &tls.origin, tls.cert.clone()).await;
    restarted.poll_once().await.unwrap();
    assert!(restarted.ready().await);
    if reorg {
        assert_eq!(
            restarted
                .router()
                .oneshot(old_query())
                .await
                .unwrap()
                .status(),
            expected,
            "restart must retain the orphaned predecessor fence"
        );
    }
    node_task.abort();
}

/// Publisher service discovery gates startup and every pass of a real HTTPS
/// display consumer. Only a confirmed major version mismatch stops the process.
#[tokio::test(flavor = "multi_thread")]
async fn display_consumers_check_publisher_discovery_at_startup_and_every_pass() {
    let scratch = tempfile::tempdir().unwrap();
    let journal = scratch.path().join("journal");
    fixture::write_journal(&journal, 0, 8, 0);
    let store = EventStore::open_existing(&journal).unwrap();
    let root = scratch.path().join("publisher");
    let initial = bootstrap(&store, &root, &fixture::layout(&store, 1, 16), 7).unwrap();
    let node = axum::Router::new().route(
        "/",
        axum::routing::post(
            |axum::Json(request): axum::Json<serde_json::Value>| async move {
                let height = request["params"][0].as_u64().unwrap();
                let hash = if height == 0 {
                    fixture::GENESIS.to_string()
                } else {
                    fixture::block_hash(0, height).to_display_hex()
                };
                axum::Json(serde_json::json!({"id":request["id"],"result":hash,"error":null}))
            },
        ),
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let _node = tokio::spawn(async move { axum::serve(listener, node).await.unwrap() });
    let cookie = scratch.path().join("cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let rpc = crate::ingest::zakura::connect(format!("http://{address}"), cookie).unwrap();
    let distribution = Distribution::new(&root, rpc).unwrap();
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            initial.candidate.clone(),
            vec![],
        )
        .await
        .unwrap());
    let tls_root = scratch.path().join("tls");
    std::fs::create_dir(&tls_root).unwrap();
    let tls = tls_router(&tls_root, distribution.router()).await;
    let requests = tls.control.with_extension("requests");
    let product_requests = || std::fs::read_to_string(&requests).unwrap_or_default();
    let config = |directory: &Path| Config {
        publisher: tls.origin.clone(),
        network: "main".into(),
        genesis_hash: fixture::GENESIS.into(),
        directory: directory.into(),
        cache_bytes: 4 << 30,
        max_download_bytes: 4 << 30,
        roots: vec![tls.cert.clone()],
    };
    let service = || ServiceConfig {
        cache_bytes: 4 << 30,
        ..Default::default()
    };
    let newer = r#"discovery-body:{"service":"pir-publisher","version":"2.0.0"}"#;

    // Startup refuses every discovery failure before opening any state or
    // sending any product request.
    for (fault, expected) in [
        ("discovery-missing", "HTTP 404"),
        ("discovery-redirect", "HTTP 302"),
        ("discovery-unavailable", "HTTP 503"),
        ("discovery-oversize", "exceeds"),
        ("discovery-body:{", "malformed"),
        (
            r#"discovery-body:{"service":"other","version":"1.0.0"}"#,
            "expected \"pir-publisher\"",
        ),
        (
            r#"discovery-body:{"service":"pir-publisher","version":"0.9.0"}"#,
            "0.x",
        ),
        (newer, "upgrade the query server"),
    ] {
        std::fs::write(&tls.control, fault).unwrap();
        std::fs::write(&requests, "").unwrap();
        let directory = scratch.path().join("refused");
        let error = match Manager::new(config(&directory), service()).await {
            Ok(_) => panic!("started against {fault}"),
            Err(error) => error,
        };
        let text = error.to_string();
        assert!(text.contains(expected), "{fault}: {text}");
        assert!(text.contains(&tls.origin), "{fault}: {text}");
        assert_eq!(
            pir_query_server::transparent::is_incompatible(&error),
            fault == newer,
            "{fault}"
        );
        assert!(!directory.exists(), "{fault} opened consumer state");
        assert_eq!(product_requests(), "", "{fault} fetched product data");
    }
    let mut untrusted = config(&scratch.path().join("untrusted"));
    untrusted.roots.clear();
    std::fs::write(&tls.control, "").unwrap();
    assert!(Manager::new(untrusted, service()).await.is_err());

    // A compatible publisher proceeds into the existing product validation.
    let directory = scratch.path().join("consumer");
    let mut consumer = Manager::new(config(&directory), service()).await.unwrap();
    consumer.poll_once().await.unwrap();
    assert!(consumer.ready().await);

    // An ordinary discovery failure skips the whole pass and keeps serving.
    std::fs::write(&tls.control, "discovery-unavailable").unwrap();
    std::fs::write(&requests, "").unwrap();
    let error = consumer.poll_once().await.unwrap_err();
    assert!(
        !pir_query_server::transparent::is_incompatible(&error),
        "{error}"
    );
    assert_eq!(
        product_requests(),
        "",
        "a refused pass fetched product data"
    );
    assert!(consumer.ready().await, "temporary refusal withdrew serving");
    std::fs::write(&tls.control, "").unwrap();
    assert_eq!(consumer.poll_once().await.unwrap()["unchanged"], true);
    drop(consumer);

    // A live process stops wallet serving and returns the original typed error
    // when the publisher's major version changes.
    let process: pir_query_server::transparent::tx_detail::process::DetailProcess =
        serde_json::from_value(serde_json::json!({
            "publisher": tls.origin,
            "state_dir": directory,
            "publisher_network": "main",
            "publisher_genesis": fixture::GENESIS,
            "publisher_ca": tls_root.join("ca.pem"),
            "role": "combined",
        }))
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let serving = listener.local_addr().unwrap();
    let running = tokio::spawn(pir_query_server::transparent::tx_detail::process::run_with(
        process.clone(),
        Some(listener),
    ));
    let status = format!("http://{serving}/v1/provider-status");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(60);
    while reqwest::get(&status).await.is_err() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "process never served"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    std::fs::write(&tls.control, newer).unwrap();
    let error = tokio::time::timeout(std::time::Duration::from_secs(60), running)
        .await
        .expect("an incompatible publisher must stop the process")
        .unwrap()
        .unwrap_err();
    assert!(
        pir_query_server::transparent::is_incompatible(&error),
        "{error}"
    );
    assert!(error.to_string().contains("2.0.0"), "{error}");
    assert!(
        tokio::net::TcpStream::connect(serving).await.is_err(),
        "wallet serving outlived an incompatible publisher"
    );

    // The recovered predecessor on disk cannot mask the mismatch at restart.
    let error = pir_query_server::transparent::tx_detail::process::run_with(process, None)
        .await
        .unwrap_err();
    assert!(
        pir_query_server::transparent::is_incompatible(&error),
        "{error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn cold_display_bootstrap_retries_a_publication_changed_during_loading() {
    let scratch = tempfile::tempdir().unwrap();
    let journal = scratch.path().join("journal");
    fixture::write_journal(&journal, 0, 4, 0);
    let store = EventStore::open_existing(&journal).unwrap();
    let mut layout = fixture::layout(&store, 1, 16);
    layout.seal.archive_target = 1_000;
    let root = scratch.path().join("publisher");
    let initial = bootstrap(&store, &root, &layout, 3).unwrap();
    let next = bootstrap(&store, &scratch.path().join("next"), &layout, 4).unwrap();
    let node = axum::Router::new().route(
        "/",
        axum::routing::post(
            |axum::Json(request): axum::Json<serde_json::Value>| async move {
                let height = request["params"][0].as_u64().unwrap();
                let hash = if height == 0 {
                    fixture::GENESIS.to_string()
                } else {
                    fixture::block_hash(0, height).to_display_hex()
                };
                axum::Json(serde_json::json!({"id": request["id"], "result": hash, "error": null}))
            },
        ),
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
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            initial.candidate.clone(),
            vec![],
        )
        .await
        .unwrap());
    let tls_root = scratch.path().join("tls");
    std::fs::create_dir(&tls_root).unwrap();
    let tls = tls_router(&tls_root, distribution.router()).await;
    std::fs::write(&tls.control, "pause-current-2").unwrap();
    let consumer_root = scratch.path().join("consumer");
    let mut consumer = manager(&consumer_root, &tls.origin, tls.cert.clone()).await;
    let checking = tokio::spawn(async move {
        let result = consumer.poll_once().await;
        (consumer, result)
    });
    tokio::time::timeout(std::time::Duration::from_secs(20), async {
        while !tls.control.with_extension("entered").exists() && !checking.is_finished() {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    if checking.is_finished() {
        let (_, result) = checking.await.unwrap();
        panic!("bootstrap stopped before the final source check: {result:?}");
    }
    assert!(tls.control.with_extension("entered").exists());
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            next.candidate.clone(),
            vec![initial.candidate],
        )
        .await
        .unwrap());
    std::fs::write(&tls.control, "").unwrap();
    let (mut consumer, result) = checking.await.unwrap();
    assert!(
        result.is_err(),
        "a changed source must refuse the first activation"
    );
    assert!(
        !consumer.ready().await,
        "checked bootstrap data cannot serve before activation"
    );
    assert!(!consumer_root.join("active.json").exists());
    consumer.poll_once().await.unwrap();
    assert!(
        consumer.ready().await,
        "the same manager must recover on the fresh publication"
    );
    let active: serde_json::Value =
        serde_json::from_slice(&std::fs::read(consumer_root.join("active.json")).unwrap()).unwrap();
    assert_eq!(active["map_sha256"], next.map_sha256);
    node_task.abort();
}

/// A node serving the fixture chain's hashes, counting the requests it
/// answers, and a client for it.
async fn counting_node(
    scratch: &Path,
) -> (
    crate::ingest::zakura::NodeClient,
    Arc<std::sync::atomic::AtomicU32>,
    tokio::task::JoinHandle<()>,
) {
    let asked = Arc::new(std::sync::atomic::AtomicU32::new(0));
    let counter = asked.clone();
    let node = axum::Router::new().route(
        "/",
        axum::routing::post(move |axum::Json(request): axum::Json<serde_json::Value>| {
            let asked = counter.clone();
            async move {
                asked.fetch_add(1, Ordering::SeqCst);
                let height = request["params"][0].as_u64().unwrap();
                let hash = if height == 0 {
                    fixture::GENESIS.to_string()
                } else {
                    fixture::block_hash(0, height).to_display_hex()
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
    let cookie = scratch.join("cookie");
    std::fs::write(&cookie, "fixture:fixture").unwrap();
    let rpc = crate::ingest::zakura::connect(format!("http://{address}"), cookie).unwrap();
    (rpc, asked, node_task)
}

/// A stop already requested ends a distribution restore before it reads the
/// set or asks the node, and commits nothing; a later restore completes.
#[tokio::test(flavor = "multi_thread")]
async fn a_stopped_display_restore_commits_nothing_and_can_resume() {
    let scratch = tempfile::tempdir().unwrap();
    let journal = scratch.path().join("journal");
    fixture::write_journal(&journal, 0, 8, 0);
    let store = EventStore::open_existing(&journal).unwrap();
    let root = scratch.path().join("publisher");
    let initial = bootstrap(&store, &root, &fixture::layout(&store, 1, 16), 7).unwrap();
    let (rpc, asked, node_task) = counting_node(scratch.path()).await;
    let distribution = Distribution::new(&root, rpc).unwrap();
    let shutdown = pir_pipeline::service::Shutdown::default();
    shutdown.request();
    let error = distribution
        .restore_until(
            crate::host_engine::PublisherEngine,
            initial.candidate.clone(),
            vec![],
            shutdown,
        )
        .await
        .unwrap_err();
    assert!(error.is::<transparent_shared::publication::Stopped>());
    assert_eq!(asked.load(Ordering::SeqCst), 0, "the node was not asked");
    assert!(
        !root.join("http-current.json").exists(),
        "nothing committed"
    );
    assert!(distribution
        .restore_until(
            crate::host_engine::PublisherEngine,
            initial.candidate.clone(),
            vec![],
            Default::default()
        )
        .await
        .unwrap());
    assert!(root.join("http-current.json").exists());
    node_task.abort();
}

/// A restore stopped at any point of its blocking work, while it loads and
/// verifies the set, checks its runtimes or finalizes the inventory, commits
/// nothing, asks the node nothing, and keeps the verified set and runtime
/// evidence of the last complete restore; the next restore completes and
/// loads no runtime again.
#[tokio::test(flavor = "multi_thread")]
async fn a_display_restore_stopped_mid_scan_keeps_its_evidence() {
    use std::sync::atomic::AtomicU32;
    let scratch = tempfile::tempdir().unwrap();
    let journal = scratch.path().join("journal");
    fixture::write_journal(&journal, 0, 8, 0);
    let store = EventStore::open_existing(&journal).unwrap();
    let root = scratch.path().join("publisher");
    let initial = bootstrap(&store, &root, &fixture::layout(&store, 1, 16), 7).unwrap();
    let (rpc, asked, node_task) = counting_node(scratch.path()).await;
    let distribution = Distribution::new(&root, rpc).unwrap();
    // Asks for a stop at the `at`-th check, as a shutdown arriving then would.
    let stop_at = |at: u32, shutdown: pir_pipeline::service::Shutdown| {
        let checks = Arc::new(AtomicU32::new(0));
        let counted = checks.clone();
        let stop = move || {
            if counted.fetch_add(1, Ordering::SeqCst) + 1 >= at {
                shutdown.request();
            }
            shutdown.requested()
        };
        (checks, stop)
    };
    let directory = initial.candidate.clone();
    // The first restore builds the runtimes and self-checks every one.
    assert!(distribution
        .restore(
            crate::host_engine::PublisherEngine,
            directory.clone(),
            vec![]
        )
        .await
        .unwrap());
    let (set, loaded, _) = distribution.evidence();
    assert!(set.is_some() && loaded > 0);
    // Later restores reuse that evidence. Files hashed right after they were
    // written are vouched for only by a later hash, so restore until one
    // makes as many checks as the one before; count them.
    let (mut total, mut steady) = (0, false);
    for _ in 0..5 {
        let shutdown = pir_pipeline::service::Shutdown::default();
        let (checks, stop) = stop_at(u32::MAX, shutdown.clone());
        assert!(distribution
            .restore_asking(
                crate::host_engine::PublisherEngine,
                directory.clone(),
                vec![],
                shutdown,
                stop
            )
            .await
            .unwrap());
        let made = checks.swap(0, Ordering::SeqCst);
        steady = made == total;
        if steady {
            break;
        }
        total = made;
    }
    assert!(steady, "restores keep making different numbers of checks");
    let (set, loaded, reused) = distribution.evidence();
    assert_eq!(loaded, 0);
    assert!(reused > 0);
    assert!(total > 2, "{total} checks");
    let current = std::fs::read(root.join("http-current.json")).unwrap();
    let answered = asked.load(Ordering::SeqCst);

    let points: std::collections::BTreeSet<u32> =
        (0..=32).map(|i| 1 + (total - 1) * i / 32).collect();
    for at in points {
        let shutdown = pir_pipeline::service::Shutdown::default();
        let (checks, stop) = stop_at(at, shutdown.clone());
        let error = match distribution
            .restore_asking(
                crate::host_engine::PublisherEngine,
                directory.clone(),
                vec![],
                shutdown,
                stop,
            )
            .await
        {
            Ok(_) => panic!(
                "a stop at check {at} of {total} completed after {} checks",
                checks.load(Ordering::SeqCst)
            ),
            Err(error) => error,
        };
        assert!(
            error.is::<transparent_shared::publication::Stopped>(),
            "stopped at check {at} of {total}: {error}"
        );
        assert!(checks.load(Ordering::SeqCst) >= at);
        assert_eq!(
            std::fs::read(root.join("http-current.json")).unwrap(),
            current,
            "stopped at check {at}: nothing committed or withdrawn"
        );
        assert_eq!(
            distribution.evidence().0,
            set,
            "stopped at check {at}: the verified set is kept"
        );
    }
    assert_eq!(
        asked.load(Ordering::SeqCst),
        answered,
        "the node was not asked"
    );

    assert!(distribution
        .restore(crate::host_engine::PublisherEngine, directory, vec![])
        .await
        .unwrap());
    assert_eq!(
        distribution.evidence(),
        (set, 0, reused),
        "every stop kept the runtime evidence"
    );
    assert_ne!(
        std::fs::read(root.join("http-current.json")).unwrap(),
        current,
        "the completed restore commits"
    );
    node_task.abort();
}
