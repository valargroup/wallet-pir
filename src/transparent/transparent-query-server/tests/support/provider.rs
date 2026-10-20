//! Complete endpoint tests; original released adapter runs in a separate process.
use zakura_pir_client::transparent::native::TableClientExt as _;

use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;
use zakura_pir_query_server::transparent::native as transparent_native;

use super::{generated, genesis, hash_at, revision, script, FIRST};
use std::sync::Arc;
use transparent_filter::ShardMap;
use transparent_query_server::history::service::{
    router, ReadinessMode, ServiceConfig, ServiceState,
};
use transparent_query_server::routing::archive::Archive;
use transparent_query_server::routing::router::plan;
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K, DECOUPLED_32K_32K};
use transparent_shared::history::shardset::{LoadOptions, LoadScope, ShardSet};

#[path = "provider_fixture.rs"]
mod fixture;
use fixture::*;

#[tokio::test]
async fn distinct_archive_geometry_is_declared_without_archive_runtime_or_reachability() {
    let dir = tempfile::tempdir().unwrap();
    let map = fixture(dir.path(), &DECOUPLED_32K_32K);
    let state = recent(
        dir.path(),
        &map,
        Archive::new("http://127.0.0.1:1", vec![]).unwrap(),
    );
    let (origin, server) = serve(state).await;
    let init = reqwest::get(format!("{origin}/v2/shards/init"))
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    let names: Vec<_> = init["geometries"]
        .as_array()
        .unwrap()
        .iter()
        .map(|g| g["name"].as_str().unwrap())
        .collect();
    assert_eq!(names, vec!["decoupled-2k-8k", "decoupled-32k-32k"]);
    let digest = &map.shards[0].manifest_digest;
    assert_eq!(
        reqwest::get(format!(
            "{origin}/v2/shards/0/revisions/{digest}/setup/directory/0"
        ))
        .await
        .unwrap()
        .status(),
        503
    );
    server.abort();
}
#[tokio::test]
#[ignore = "explicit frozen released391 process qualification; build isolated harness first"]
async fn released391_recovers_archive_and_recent_then_recent_during_archive_outage() {
    released_recovery(&DECOUPLED_2K_8K).await;
}
#[tokio::test]
#[ignore = "explicit frozen released391 process qualification; build isolated harness first"]
async fn released391_recovers_32k_archive_pages_with_2k_recent() {
    released_recovery(&DECOUPLED_32K_32K).await;
}
async fn released_recovery(archive_geometry: &'static Geometry) {
    let dir = tempfile::tempdir().unwrap();
    let map = fixture_in(dir.path(), archive_geometry, true);
    retain_fixture(
        &format!("released391-{}", archive_geometry.name),
        dir.path(),
    );
    let terminal = terminal(dir.path(), &map);
    terminal.spawn_prewarm().await.unwrap();
    let archive_metrics = terminal.metrics().clone();
    use std::sync::atomic::{AtomicU8, Ordering};
    let corrupt = Arc::new(AtomicU8::new(0));
    let corruption = corrupt.clone();
    let submissions = Arc::new(std::sync::Mutex::new(Vec::new()));
    let observed = submissions.clone();
    let app = router(terminal).layer(axum::middleware::from_fn(
        move |mut request: axum::extract::Request, next: axum::middleware::Next| {
            let corrupt = corruption.load(Ordering::SeqCst);
            let observed = observed.clone();
            async move {
                let page = request.uri().path().contains("/query/pages-");
                let query = request.uri().path().contains("/query/");
                let page_setup = request.uri().path().contains("/setup/pages-");
                if query {
                    let body = axum::body::to_bytes(
                        std::mem::replace(request.body_mut(), axum::body::Body::empty()),
                        1 << 20,
                    )
                    .await
                    .unwrap();
                    observed.lock().unwrap().push((
                        request.uri().path().to_owned(),
                        transparent_shared::publication::digest(&body),
                    ));
                    *request.body_mut() = axum::body::Body::from(body);
                }
                let mut response = next.run(request).await;
                // Exercise pages separately: directory success alone must not
                // turn an incomplete historical restore into complete recovery.
                if (query && corrupt == 1 || page && corrupt == 2 || page_setup && corrupt == 3)
                    && response.status().is_success()
                {
                    let mut bytes = axum::body::to_bytes(
                        std::mem::replace(response.body_mut(), axum::body::Body::empty()),
                        1 << 20,
                    )
                    .await
                    .unwrap()
                    .to_vec();
                    if corrupt != 3 {
                        bytes[0] ^= 1;
                    } else {
                        let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                        value["public_params_epoch"] = serde_json::json!("00".repeat(8));
                        bytes = serde_json::to_vec(&value).unwrap();
                    }
                    response.headers_mut().remove("content-length");
                    *response.body_mut() = axum::body::Body::from(bytes);
                }
                response
            }
        },
    ));
    let (archive, archive_server) = serve_app(app).await;
    let state = recent(dir.path(), &map, Archive::new(&archive, vec![]).unwrap());
    let (origin, server) = serve(state).await;
    let recovered = frozen(origin.clone(), 1, FIRST).await;
    // Independent source-event oracle: one historical receive, then its spend.
    assert_eq!(
        recovered,
        serde_json::json!({"covered_through":FIRST+3,"matched_shards":[0,1],"utxos":[],"unresolved":0,
        "spends":[{"spent_txid":"04".repeat(32),"spent_index":0,"spending_txid":"05".repeat(32),"input_index":0,"height":FIRST+1,"value":7,"script":hex::encode(script(1).as_slice())},
        {"spent_txid":"01".repeat(32),"spent_index":0,"spending_txid":"02".repeat(32),"input_index":0,"height":FIRST+2,"value":123456,"script":hex::encode(script(1).as_slice())}],
        "history":[{"txid":"01".repeat(32),"height":FIRST,"received":123456,"spent":0},
        {"txid":"04".repeat(32),"height":FIRST+1,"received":7,"spent":0},{"txid":"05".repeat(32),"height":FIRST+1,"received":0,"spent":7},
        {"txid":"02".repeat(32),"height":FIRST+2,"received":0,"spent":123456}]})
    );
    assert_eq!(
        transparent_shared::runtime::metrics::Metrics::get(&archive_metrics.queries),
        3,
        "recent execution must remain local"
    );
    // Without a public choice table the released client always fetches both
    // directory candidates for privacy, even if the first contains a hit.
    // This fixture then needs one historical page. A bad page setup must stop
    // before that page evaluation, after the two valid directory evaluations.
    for (fault, directory_queries, page_queries) in [(1, 1, 0), (2, 2, 1), (3, 2, 0)] {
        corrupt.store(fault, Ordering::SeqCst);
        submissions.lock().unwrap().clear();
        let before = transparent_shared::runtime::metrics::Metrics::get(&archive_metrics.queries);
        assert!(
            !frozen_output(origin.clone(), 1, FIRST).await.status.success(),
            "released client must refuse directory/page corruption and page-only setup identity mismatch"
        );
        assert_eq!(
            transparent_shared::runtime::metrics::Metrics::get(&archive_metrics.queries),
            before + directory_queries + page_queries,
            "fault {fault}: refuse mismatched setup before evaluation; never replay an uncertain submission"
        );
        let trace = submissions.lock().unwrap();
        assert_eq!(trace.len(), (directory_queries + page_queries) as usize);
        assert_eq!(
            trace
                .iter()
                .filter(|(path, _)| path.ends_with("/query/directory"))
                .count(),
            directory_queries as usize
        );
        assert_eq!(
            trace
                .iter()
                .filter(|(path, _)| path.contains("/query/pages-"))
                .count(),
            page_queries as usize
        );
        assert_eq!(
            trace
                .iter()
                .collect::<std::collections::BTreeSet<_>>()
                .len(),
            trace.len(),
            "no identical encrypted submission may be replayed"
        );
    }
    archive_server.stop().await;
    let recent = frozen(origin.clone(), 2, FIRST + 2).await;
    assert_eq!(recent["matched_shards"], serde_json::json!([1]));
    assert_eq!(
        recent["utxos"],
        serde_json::json!([{"txid":"03".repeat(32),"index":0,"value":42,"script":hex::encode(script(2).as_slice()),"height":FIRST+3,"coinbase":false}])
    );
    let digest = &map.shards[0].manifest_digest;
    assert_eq!(
        reqwest::get(format!(
            "{origin}/v2/shards/0/revisions/{digest}/setup/directory/0"
        ))
        .await
        .unwrap()
        .status(),
        503
    );
    server.abort();
}

#[tokio::test]
async fn archive_readiness_and_setup_are_checked_over_real_http() {
    use std::sync::atomic::{AtomicU8, Ordering};
    let dir = tempfile::tempdir().unwrap();
    let map = fixture(dir.path(), &DECOUPLED_2K_8K);
    let terminal = terminal(dir.path(), &map);
    terminal.spawn_prewarm().await.unwrap();
    let mode = Arc::new(AtomicU8::new(0));
    let injection = mode.clone();
    let archive_metrics = terminal.metrics().clone();
    let terminal_hold = terminal.clone();
    let app = router(terminal).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let mode = injection.load(Ordering::SeqCst);
            async move {
                let path = request.uri().path().to_owned();
                let mut response = next.run(request).await;
                if mode == 1 && path == "/v1/ready" {
                    *response.status_mut() = axum::http::StatusCode::SERVICE_UNAVAILABLE;
                }
                if (mode == 2 && path == "/v2/shards/init")
                    || (mode == 3 && path == "/v1/ready")
                    || (mode == 4 && path.contains("/setup/"))
                {
                    let bytes = axum::body::to_bytes(
                        std::mem::replace(response.body_mut(), axum::body::Body::empty()),
                        1 << 20,
                    )
                    .await
                    .unwrap();
                    let mut value: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
                    match mode {
                        2 => value["schema"] = serde_json::json!("wrong-schema"),
                        3 => value["archive_forwarding"] = serde_json::json!(true),
                        4 => value["public_params_epoch"] = serde_json::json!("00".repeat(8)),
                        _ => unreachable!(),
                    };
                    response.headers_mut().remove("content-length");
                    *response.body_mut() =
                        axum::body::Body::from(serde_json::to_vec(&value).unwrap());
                }
                if (5..=7).contains(&mode)
                    && path.contains("/query/")
                    && response.status().is_success()
                {
                    let mut bytes = axum::body::to_bytes(
                        std::mem::replace(response.body_mut(), axum::body::Body::empty()),
                        1 << 20,
                    )
                    .await
                    .unwrap()
                    .to_vec();
                    match mode {
                        5 => bytes[0] ^= 1,
                        6 => bytes[8] ^= 1,
                        7 => {
                            bytes.pop();
                        }
                        _ => unreachable!(),
                    }
                    response.headers_mut().remove("content-length");
                    *response.body_mut() = axum::body::Body::from(bytes);
                }
                response
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let archive_server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let state = recent(
        dir.path(),
        &map,
        Archive::new(&format!("http://{address}"), vec![]).unwrap(),
    );
    let (origin, server) = serve(state.clone()).await;
    let client = reqwest::Client::new();
    let setup = format!(
        "{origin}/v2/shards/0/revisions/{}/setup/directory/0",
        map.shards[0].manifest_digest
    );
    assert_eq!(client.get(&setup).send().await.unwrap().status(), 200);
    let held = terminal_hold.hold_query_slot().await;
    let capacity = client.get(&setup).send().await.unwrap();
    assert_eq!(capacity.status(), 503);
    assert_eq!(
        capacity.headers()["retry-after"],
        "1",
        "explicit terminal capacity must remain retryable"
    );
    drop(capacity);
    drop(held);
    for fault in 1..=4 {
        mode.store(fault, Ordering::SeqCst);
        assert_eq!(
            client.get(&setup).send().await.unwrap().status(),
            503,
            "fault {fault}"
        );
        assert_eq!(
            client
                .get(format!("{origin}/v2/shards/init"))
                .send()
                .await
                .unwrap()
                .status(),
            200
        );
    }
    mode.store(0, Ordering::SeqCst);
    assert_eq!(client.get(&setup).send().await.unwrap().status(), 200);
    assert_eq!(
        client
            .get(format!("{origin}/v1/ready"))
            .header("x-transparent-archive-hop", "1")
            .send()
            .await
            .unwrap()
            .status(),
        421
    );
    let profile = transparent_native::TableProfile::new(
        transparent_shard::manifest::SCHEMA,
        DECOUPLED_2K_8K.name,
        "directory",
        DECOUPLED_2K_8K.directory_rows,
        DECOUPLED_2K_8K.directory_row_bytes as u32,
    )
    .unwrap();
    let (_, upload) = profile.prepare(0).unwrap();
    let mut body =
        transparent_shard::manifest::query_binding(&map.shards[0].manifest_digest, "directory")
            .to_vec();
    body.extend(upload);
    let query = format!(
        "{origin}/v2/shards/0/revisions/{}/query/directory",
        map.shards[0].manifest_digest
    );
    for fault in 5..=7 {
        mode.store(fault, Ordering::SeqCst);
        let before = transparent_shared::runtime::metrics::Metrics::get(&archive_metrics.queries);
        let failed = client.post(&query).body(body.clone()).send().await.unwrap();
        assert_eq!(failed.status(), 503, "response fault {fault}");
        assert!(
            failed.headers().get("retry-after").is_none(),
            "uncertain answers must not invite replay"
        );
        assert_eq!(
            transparent_shared::runtime::metrics::Metrics::get(&archive_metrics.queries),
            before + 1,
            "no uncertain retry"
        );
    }
    mode.store(0, Ordering::SeqCst);
    assert_eq!(
        client
            .post(&query)
            .body(body.clone())
            .send()
            .await
            .unwrap()
            .status(),
        200
    );
    // Both polling to EOF and dropping the front response retain ownership.
    use http_body::Body as _;
    use tower::ServiceExt;
    let response = router(state.clone())
        .oneshot(
            axum::http::Request::builder()
                .method("POST")
                .uri(format!(
                    "/v2/shards/0/revisions/{}/query/directory",
                    map.shards[0].manifest_digest
                ))
                .header("content-length", body.len())
                .body(axum::body::Body::from(body.clone()))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let mut retained = response.into_body();
    while let Some(frame) =
        std::future::poll_fn(|cx| std::pin::Pin::new(&mut retained).poll_frame(cx)).await
    {
        frame.unwrap();
    }
    assert!(
        transparent_shared::runtime::metrics::Metrics::get(&state.metrics().body_bytes_in_flight)
            > 0
    );
    let health = client
        .get(format!("{origin}/v1/health"))
        .send()
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert!(health["work_memory_reserved_bytes"].as_u64().unwrap() > 0);
    let capacity = client.get(&setup).send().await.unwrap();
    assert_eq!(capacity.status(), 503);
    assert_eq!(capacity.headers()["retry-after"], "1");
    capacity.bytes().await.unwrap();
    body[..8].copy_from_slice(&transparent_shard::manifest::query_binding(
        &map.shards[1].manifest_digest,
        "directory",
    ));
    let independent = client
        .post(format!(
            "{origin}/v2/shards/1/revisions/{}/query/directory",
            map.shards[1].manifest_digest
        ))
        .body(body)
        .send()
        .await
        .unwrap();
    assert_eq!(
        independent.status(),
        200,
        "EOF-held archive reply must not consume local execution"
    );
    independent.bytes().await.unwrap();
    drop(retained);
    assert_eq!(
        transparent_shared::runtime::metrics::Metrics::get(&state.metrics().body_bytes_in_flight),
        0
    );
    archive_server.abort();
    server.abort();
}

#[tokio::test]
async fn archive_upload_overload_and_disconnect_release_existing_admission() {
    use tokio::io::AsyncWriteExt;
    let dir = tempfile::tempdir().unwrap();
    let map = fixture(dir.path(), &DECOUPLED_2K_8K);
    let state = recent_with_config(
        dir.path(),
        &map,
        Archive::new("http://127.0.0.1:1", vec![]).unwrap(),
        ServiceConfig {
            query_slots: 1,
            max_waiters: 0,
            ..ServiceConfig::default()
        },
    );
    let (origin, server) = serve(state.clone()).await;
    let path = format!(
        "/v2/shards/0/revisions/{}/query/directory",
        map.shards[0].manifest_digest
    );
    let client = reqwest::Client::new();
    let shared = transparent_shared::runtime::SharedParams::build(
        &DECOUPLED_2K_8K,
        transparent_shared::history::shardset::Table::Directory,
    )
    .unwrap();
    let mut body = vec![0u8; shared.query_bytes()];
    body[..8].copy_from_slice(&transparent_shard::manifest::query_binding(
        &map.shards[0].manifest_digest,
        "directory",
    ));
    let mut connection = tokio::net::TcpStream::connect(origin.trim_start_matches("http://"))
        .await
        .unwrap();
    connection
        .write_all(
            format!(
                "POST {path} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\n\r\n",
                body.len()
            )
            .as_bytes(),
        )
        .await
        .unwrap();
    connection.write_all(&body[..8]).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while transparent_shared::runtime::metrics::Metrics::get(
            &state.metrics().body_bytes_in_flight,
        ) != body.len() as u64
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let response = client
        .post(format!("{origin}{path}"))
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    assert_eq!(response.headers()["retry-after"], "1");
    response.bytes().await.unwrap();
    drop(connection);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if transparent_shared::runtime::metrics::Metrics::get(
                &state.metrics().body_bytes_in_flight,
            ) == 0
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let response = client
        .post(format!("{origin}{path}"))
        .body(vec![0; 3])
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 400);
    let stale = format!(
        "{origin}/v2/shards/0/revisions/{}/setup/directory/0",
        "ff".repeat(32)
    );
    assert_eq!(client.get(stale).send().await.unwrap().status(), 409);
    server.abort();
}

#[tokio::test]
#[ignore = "explicit frozen released391 process qualification; build isolated harness first"]
async fn released391_accepts_distinct_archive_geometry_and_recovers_recent_while_archive_is_down() {
    let dir = tempfile::tempdir().unwrap();
    let map = fixture(dir.path(), &DECOUPLED_32K_32K);
    let state = recent(
        dir.path(),
        &map,
        Archive::new("http://127.0.0.1:1", vec![]).unwrap(),
    );
    let (origin, server) = serve(state).await;
    let recovered = frozen(origin, 2, FIRST + 2).await;
    assert_eq!(recovered["covered_through"], FIRST + 3);
    assert_eq!(recovered["matched_shards"], serde_json::json!([1]));
    assert_eq!(recovered["unresolved"], 0);
    assert_eq!(
        recovered["utxos"],
        serde_json::json!([{"txid":"03".repeat(32),"index":0,"value":42,"script":hex::encode(script(2).as_slice()),"height":FIRST+3,"coinbase":false}])
    );
    server.abort();
}

#[tokio::test]
async fn owner_qualifies_archive_and_retains_inflight_ownership_across_handover() {
    use std::sync::atomic::{AtomicBool, Ordering};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use transparent_query_server::history::live::{Command, LiveService, Publication};
    let v1 = tempfile::tempdir().unwrap();
    let map = fixture(v1.path(), &DECOUPLED_2K_8K);
    let terminal = terminal(v1.path(), &map);
    terminal.spawn_prewarm().await.unwrap();
    let available = Arc::new(AtomicBool::new(false));
    let availability = available.clone();
    let app = router(terminal).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let available = availability.load(Ordering::SeqCst);
            async move {
                let readiness = request.uri().path() == "/v1/ready";
                let mut response = next.run(request).await;
                if readiness && !available {
                    *response.status_mut() = axum::http::StatusCode::SERVICE_UNAVAILABLE;
                }
                response
            }
        },
    ));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let archive_server = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    let archive = Archive::new(&format!("http://{address}"), vec![]).unwrap();
    fn options(dir: &std::path::Path, map: &ShardMap, first_recent: u64) -> LoadOptions {
        let assignment = plan(
            map,
            &super::map_sha256(map),
            &super::roster(),
            first_recent,
            0.1,
            generated(),
        )
        .unwrap();
        std::fs::write(
            dir.join("assignment.json"),
            serde_json::to_vec(&assignment).unwrap(),
        )
        .unwrap();
        LoadOptions {
            scope: LoadScope::Assigned {
                assignment: Arc::new(assignment),
                worker_id: "recent-01".into(),
            },
            ..LoadOptions::whole(0)
        }
    }
    let initial_options = options(v1.path(), &map, 0);
    let config = ServiceConfig {
        readiness: ReadinessMode::Warm,
        ..ServiceConfig::default()
    };
    let old = ServiceState::build(
        ShardSet::open_with(v1.path(), &initial_options).unwrap(),
        config,
    )
    .unwrap()
    .with_archive(archive);
    old.spawn_prewarm().await.unwrap();
    let old_metrics = old.metrics().clone();
    let record = tempfile::tempdir().unwrap();
    let live = LiveService::new(
        old,
        Publication {
            directory: v1.path().into(),
            assignment: Some(v1.path().join("assignment.json")),
            map_sha256: super::map_sha256(&map),
            runtimes: None,
        },
        config,
        initial_options,
        record.path().join("active.json"),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    let serving_live = live.clone();
    let serving =
        tokio::spawn(async move { axum::serve(listener, serving_live.router()).await.unwrap() });
    let digest = &map.shards[0].manifest_digest;
    let setup = reqwest::get(format!(
        "{origin}/v2/shards/0/revisions/{digest}/setup/directory/0"
    ))
    .await
    .unwrap()
    .json::<serde_json::Value>()
    .await
    .unwrap();
    let masks = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        setup["public_params"].as_str().unwrap(),
    )
    .unwrap();
    let producer = std::fs::read(v1.path().join(digest).join("directory.0.bin")).unwrap();
    let row_bytes = DECOUPLED_2K_8K.directory_row_bytes;
    let row = producer
        .chunks_exact(row_bytes)
        .position(|row| row[..4] != [0; 4])
        .expect("occupied original producer row");
    let expected = producer[row * row_bytes..(row + 1) * row_bytes].to_vec();
    let profile = transparent_native::TableProfile::new(
        transparent_shard::manifest::SCHEMA,
        DECOUPLED_2K_8K.name,
        "directory",
        DECOUPLED_2K_8K.directory_rows,
        row_bytes as u32,
    )
    .unwrap();
    let (secret, upload) = profile.prepare(row).unwrap();
    let binding = transparent_shard::manifest::query_binding(digest, "directory");
    let mut query = binding.to_vec();
    query.extend(upload);
    let mut inflight = tokio::net::TcpStream::connect(address).await.unwrap();
    inflight.write_all(format!("POST /v2/shards/0/revisions/{digest}/query/directory HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: {}\r\n\r\n",query.len()).as_bytes()).await.unwrap();
    inflight.write_all(&query[..8]).await.unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while transparent_shared::runtime::metrics::Metrics::get(&old_metrics.body_bytes_in_flight)
            != query.len() as u64
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let v2 = tempfile::tempdir().unwrap();
    fn copy(from: &std::path::Path, to: &std::path::Path) {
        for entry in std::fs::read_dir(from).unwrap() {
            let entry = entry.unwrap();
            let target = to.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                std::fs::create_dir(&target).unwrap();
                copy(&entry.path(), &target);
            } else {
                std::fs::copy(entry.path(), target).unwrap();
            }
        }
    }
    copy(v1.path(), v2.path());
    let mut next = map.clone();
    let (_, entry) = revision::write(
        v2.path(),
        &revision::Revision {
            shard_id: 2,
            start: FIRST + 4,
            end: FIRST + 5,
            geometry: &DECOUPLED_2K_8K,
            sealed: false,
            revision: 0,
            supersedes: "",
            parent_digest: &map.shards[1].manifest_digest,
            children: 1,
            genesis: genesis(),
            hash: &hash_at,
        },
        &[],
    );
    next.shards.push(entry);
    next.check_shape().unwrap();
    std::fs::write(
        v2.path().join("shards.json"),
        serde_json::to_vec(&next).unwrap(),
    )
    .unwrap();
    options(v2.path(), &next, 1);
    let publication = Publication {
        directory: v2.path().into(),
        assignment: Some(v2.path().join("assignment.json")),
        map_sha256: super::map_sha256(&next),
        runtimes: None,
    };
    let prepare = || Command::Prepare {
        expected: super::map_sha256(&map),
        publication: publication.clone(),
    };
    assert!(live.command(prepare()).await.is_err());
    assert_eq!(
        live.command(Command::Status).await.unwrap()["active"]["map_sha256"],
        super::map_sha256(&map)
    );
    available.store(true, Ordering::SeqCst);
    live.command(prepare()).await.unwrap();
    available.store(false, Ordering::SeqCst);
    let activate = || Command::Activate {
        expected: super::map_sha256(&map),
        map_sha256: super::map_sha256(&next),
    };
    assert!(live.command(activate()).await.is_err());
    assert_eq!(
        live.command(Command::Status).await.unwrap()["active"]["map_sha256"],
        super::map_sha256(&map)
    );
    available.store(true, Ordering::SeqCst);
    live.command(activate()).await.unwrap();
    let status = live.command(Command::Status).await.unwrap();
    assert_eq!(status["active"]["map_sha256"], super::map_sha256(&next));
    assert_eq!(status["retired_snapshots"], 1);
    inflight.write_all(&query[8..]).await.unwrap();
    let mut wire = Vec::new();
    tokio::time::timeout(
        std::time::Duration::from_secs(5),
        inflight.read_to_end(&mut wire),
    )
    .await
    .unwrap()
    .unwrap();
    let separator = wire.windows(4).position(|w| w == b"\r\n\r\n").unwrap();
    let headers = std::str::from_utf8(&wire[..separator]).unwrap();
    assert!(headers.starts_with("HTTP/1.1 200"), "{headers}");
    let answer = &wire[separator + 4..];
    assert_eq!(answer.len(), 16 + profile.scheme.response_bytes);
    assert_eq!(&answer[..8], &binding);
    assert_eq!(
        profile.decode(&secret, &masks, &answer[16..]).unwrap(),
        expected
    );
    let init = reqwest::get(format!("{origin}/v2/shards/init"))
        .await
        .unwrap()
        .json::<serde_json::Value>()
        .await
        .unwrap();
    assert_eq!(init["assigned_shards"], 2);
    assert_eq!(init["shards"], 3);
    assert_eq!(
        reqwest::get(format!(
            "{origin}/v2/shards/0/revisions/{}/setup/directory/0",
            map.shards[0].manifest_digest
        ))
        .await
        .unwrap()
        .status(),
        200
    );
    archive_server.abort();
    serving.abort();
}

// Delay actual upstream response bodies, after the terminal has accepted the
// request. Signalling from poll_frame proves the remote operation is blocked.
struct ArchiveStallBody {
    body: axum::body::Body,
    gate: std::pin::Pin<Box<dyn std::future::Future<Output = ()> + Send>>,
    opened: bool,
}
impl http_body::Body for ArchiveStallBody {
    type Data = axum::body::Bytes;
    type Error = axum::Error;
    fn poll_frame(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Result<http_body::Frame<Self::Data>, Self::Error>>> {
        if !self.opened {
            std::task::ready!(self.gate.as_mut().poll(cx));
            self.opened = true;
        }
        std::pin::Pin::new(&mut self.body).poll_frame(cx)
    }
    fn size_hint(&self) -> http_body::SizeHint {
        self.body.size_hint()
    }
}

async fn recent_survives_archive_stall(operation: &str, stage: &str, ending: &str) {
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::time::Duration;
    let dir = tempfile::tempdir().unwrap();
    let map = fixture(dir.path(), &DECOUPLED_2K_8K);
    let terminal = terminal(dir.path(), &map);
    terminal.spawn_prewarm().await.unwrap();
    let terminal_metrics = terminal.metrics().clone();
    let enabled = Arc::new(AtomicBool::new(false));
    let entered = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let calls = Arc::new(AtomicUsize::new(0));
    let app = router(terminal).layer(axum::middleware::from_fn({
        let enabled = enabled.clone();
        let entered = entered.clone();
        let release = release.clone();
        let calls = calls.clone();
        let stage = stage.to_string();
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let stall = enabled.load(Ordering::SeqCst)
                && match stage.as_str() {
                    "ready" => request.uri().path() == "/v1/ready",
                    "setup" => request.uri().path().contains("/setup/"),
                    "query" => request.uri().path().contains("/query/"),
                    _ => unreachable!(),
                };
            let entered = entered.clone();
            let release = release.clone();
            let calls = calls.clone();
            async move {
                let mut response = next.run(request).await;
                if stall {
                    assert_eq!(response.status(), 200);
                    calls.fetch_add(1, Ordering::SeqCst);
                    let body = std::mem::replace(response.body_mut(), axum::body::Body::empty());
                    *response.body_mut() = axum::body::Body::new(ArchiveStallBody {
                        body,
                        opened: false,
                        gate: Box::pin(async move {
                            entered.notify_one();
                            release.notified().await
                        }),
                    });
                }
                response
            }
        }
    }));
    let (archive, archive_server) = serve_app(app).await;
    let profile = transparent_native::TableProfile::new(
        transparent_shard::manifest::SCHEMA,
        DECOUPLED_2K_8K.name,
        "directory",
        DECOUPLED_2K_8K.directory_rows,
        DECOUPLED_2K_8K.directory_row_bytes as u32,
    )
    .unwrap();
    let producer = std::fs::read(
        dir.path()
            .join(&map.shards[1].manifest_digest)
            .join("directory.0.bin"),
    )
    .unwrap();
    let row_bytes = DECOUPLED_2K_8K.directory_row_bytes;
    let row = producer
        .chunks_exact(row_bytes)
        .position(|r| r[..4] != [0; 4])
        .unwrap();
    let expected = &producer[row * row_bytes..(row + 1) * row_bytes];
    let (secret, upload) = profile.prepare(row).unwrap();
    let body = |id: usize| {
        let mut body = transparent_shard::manifest::query_binding(
            &map.shards[id].manifest_digest,
            "directory",
        )
        .to_vec();
        body.extend_from_slice(&upload);
        body
    };
    // Exactly enough for the compiled local maximum plus this one forward.
    let mut local_reserve = 0u64;
    for geometry in transparent_shard::layout::PROFILES {
        for table in transparent_shared::history::shardset::Table::HISTORY {
            let shared = transparent_shared::runtime::SharedParams::build(geometry, table).unwrap();
            local_reserve = local_reserve.max((shared.query_bytes() as u64).div_ceil(1024) * 1024);
        }
    }
    let state = recent_with_config(
        dir.path(),
        &map,
        Archive::new(&archive, vec![]).unwrap(),
        ServiceConfig {
            query_slots: 1,
            max_waiters: 0,
            readiness: ReadinessMode::Warm,
            max_body_bytes: local_reserve + (body(1).len() as u64).div_ceil(1024) * 1024,
            query_deadline: Duration::from_secs(5),
            ..ServiceConfig::default()
        },
    );
    state.spawn_prewarm().await.unwrap();
    let (origin, server) = serve(state.clone()).await;
    let path = |id: usize, op: &str| {
        format!(
            "{origin}/v2/shards/{id}/revisions/{}/{op}/directory{}",
            map.shards[id].manifest_digest,
            if op == "setup" { "/0" } else { "" }
        )
    };
    let client = reqwest::Client::new();
    let setup: serde_json::Value = client
        .get(path(1, "setup"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    let masks = base64::Engine::decode(
        &base64::engine::general_purpose::STANDARD,
        setup["public_params"].as_str().unwrap(),
    )
    .unwrap();
    let local = || client.post(path(1, "query")).body(body(1)).send();
    let check = |answer: &[u8]| {
        assert_eq!(&answer[..8], &body(1)[..8]);
        assert_eq!(
            profile.decode(&secret, &masks, &answer[16..]).unwrap(),
            expected
        );
    };
    let baseline = local().await.unwrap();
    assert_eq!(baseline.status(), 200);
    check(&baseline.bytes().await.unwrap());
    enabled.store(true, Ordering::SeqCst);
    let request = if operation == "setup" {
        client.get(path(0, "setup"))
    } else {
        client.post(path(0, "query")).body(body(0))
    };
    let forwarding = tokio::spawn(async move { request.send().await.unwrap() });
    tokio::time::timeout(Duration::from_secs(2), entered.notified())
        .await
        .unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert!(
        !forwarding.is_finished(),
        "forward must still await its blocked upstream body"
    );
    if stage == "query" {
        assert_eq!(
            transparent_shared::runtime::metrics::Metrics::get(&terminal_metrics.queries),
            1,
            "terminal accepted exactly one encrypted submission before its answer stalled"
        );
    }
    let response = local().await.unwrap();
    assert_eq!(
        response.status(),
        200,
        "valid recent query must survive archive {operation}/{stage} stall"
    );
    check(&response.bytes().await.unwrap());
    let capacity = client.get(path(0, "setup")).send().await.unwrap();
    assert_eq!(capacity.status(), 503);
    assert_eq!(capacity.headers()["retry-after"], "1");
    capacity.bytes().await.unwrap();
    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "overload must refuse before remote work"
    );
    // Isolation must not expand the original local evaluation capacity.
    let held = state.hold_query_slot().await;
    let capacity = local().await.unwrap();
    assert_eq!(capacity.status(), 503);
    assert_eq!(capacity.headers()["retry-after"], "1");
    capacity.bytes().await.unwrap();
    drop(held);
    match ending {
        "release" => {
            enabled.store(false, Ordering::SeqCst);
            release.notify_one();
            let response = forwarding.await.unwrap();
            assert_eq!(response.status(), 200);
            response.bytes().await.unwrap();
        }
        "disconnect" => {
            forwarding.abort();
            assert!(forwarding.await.unwrap_err().is_cancelled());
        }
        "deadline" => {
            let response = tokio::time::timeout(Duration::from_secs(6), forwarding)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(response.status(), 503);
            assert!(
                response.headers().get("retry-after").is_none(),
                "uncertain timeout must not invite replay"
            );
            response.bytes().await.unwrap();
        }
        _ => unreachable!(),
    }
    tokio::time::timeout(Duration::from_secs(2), async {
        while transparent_shared::runtime::metrics::Metrics::get(&state.metrics().query_queue_depth)
            != 0
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("disconnect/deadline/body drop must release the forwarding place");
    assert_eq!(
        transparent_shared::runtime::metrics::Metrics::get(&state.metrics().body_bytes_in_flight),
        0
    );
    enabled.store(false, Ordering::SeqCst);
    release.notify_one();
    if stage == "query" {
        assert_eq!(
            transparent_shared::runtime::metrics::Metrics::get(&terminal_metrics.queries),
            1,
            "stalled accepted answer is never replayed"
        );
    }
    let response = local().await.unwrap();
    assert_eq!(response.status(), 200);
    check(&response.bytes().await.unwrap());
    server.stop().await;
    archive_server.stop().await;
}
#[tokio::test]
async fn recent_query_survives_archive_query_readiness_stall() {
    recent_survives_archive_stall("query", "ready", "release").await
}
#[tokio::test]
async fn recent_query_survives_archive_setup_readiness_stall() {
    recent_survives_archive_stall("setup", "ready", "release").await
}
#[tokio::test]
async fn recent_query_survives_archive_setup_body_stall() {
    recent_survives_archive_stall("setup", "setup", "release").await
}
#[tokio::test]
async fn recent_query_survives_archive_accepted_answer_body_stall() {
    recent_survives_archive_stall("query", "query", "release").await
}

#[tokio::test]
async fn recent_query_survives_archive_disconnect_after_acceptance() {
    recent_survives_archive_stall("query", "query", "disconnect").await
}
#[tokio::test]
async fn recent_query_survives_archive_deadline_after_acceptance() {
    recent_survives_archive_stall("query", "query", "deadline").await
}
