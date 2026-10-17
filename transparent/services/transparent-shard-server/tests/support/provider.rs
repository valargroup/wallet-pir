//! Complete endpoint tests; original released adapter runs in a separate process.
use super::{generated, genesis, hash_at, revision, script, FIRST};
use std::{collections::BTreeMap, sync::Arc};
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
use transparent_filter::{SealParameters, ShardMap};
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K, DECOUPLED_32K_32K};
use transparent_shard_server::{
    archive::Archive,
    assignment::WorkerRole,
    router::{plan, RosterEntry},
    service::{router, ReadinessMode, ServiceConfig, ServiceState},
    shardset::{LoadOptions, LoadScope, ShardSet},
};

fn fixture(dir: &std::path::Path, archive_geometry: &'static Geometry) -> ShardMap {
    fixture_in(dir, archive_geometry, false)
}
fn fixture_in(dir: &std::path::Path, archive_geometry: &'static Geometry, paged: bool) -> ShardMap {
    let mut shards = Vec::new();
    let mut parent = String::new();
    for id in 0..2 {
        let start = FIRST + id * 2;
        let mut events = if id == 0 {
            vec![(
                script(1),
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: None,
                    height: start as u32,
                    txid: Txid([1; 32]),
                    transaction_index: 0,
                    output_index: 0,
                    value: 123456,
                    coinbase: false,
                }),
            )]
        } else {
            vec![
                (
                    script(1),
                    TransparentEvent::Spend(SpendEvent {
                        metadata: None,
                        height: start as u32,
                        spending_txid: Txid([2; 32]),
                        transaction_index: 0,
                        input_index: 0,
                        spent_txid: Txid([1; 32]),
                        spent_output_index: 0,
                    }),
                ),
                (
                    script(2),
                    TransparentEvent::Receive(ReceiveEvent {
                        metadata: None,
                        height: (start + 1) as u32,
                        txid: Txid([3; 32]),
                        transaction_index: 0,
                        output_index: 0,
                        value: 42,
                        coinbase: false,
                    }),
                ),
            ]
        };
        if id == 0 && paged {
            events.push((
                script(1),
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: None,
                    height: (FIRST + 1) as u32,
                    txid: Txid([4; 32]),
                    transaction_index: 0,
                    output_index: 0,
                    value: 7,
                    coinbase: false,
                }),
            ));
            events.push((
                script(1),
                TransparentEvent::Spend(SpendEvent {
                    metadata: None,
                    height: (FIRST + 1) as u32,
                    spending_txid: Txid([5; 32]),
                    transaction_index: 1,
                    input_index: 0,
                    spent_txid: Txid([4; 32]),
                    spent_output_index: 0,
                }),
            ));
        }
        let (_, entry) = revision::write(
            dir,
            &revision::Revision {
                shard_id: id,
                start,
                end: start + 1,
                geometry: if id == 0 {
                    archive_geometry
                } else {
                    &DECOUPLED_2K_8K
                },
                sealed: true,
                revision: 0,
                supersedes: "",
                parent_digest: &parent,
                children: 1,
                genesis: genesis(),
                hash: &hash_at,
            },
            &events,
        );
        parent = entry.manifest_digest.clone();
        shards.push(entry);
    }
    let seal: BTreeMap<_, _> = shards
        .iter()
        .map(|s| {
            (
                s.geometry.clone(),
                SealParameters {
                    max_scripts: 8192,
                    max_page_rows: 2048,
                    max_txids: 0,
                },
            )
        })
        .collect();
    let map = ShardMap {
        genesis_hash: super::GENESIS.into(),
        network: transparent_filter::NETWORK.into(),
        profile: transparent_filter::RANGE_PROFILE.into(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal,
        shards,
        recuts: Vec::new(),
    };
    map.check_shape().unwrap();
    std::fs::write(dir.join("shards.json"), serde_json::to_vec(&map).unwrap()).unwrap();
    map
}
fn recent(dir: &std::path::Path, map: &ShardMap, archive: Arc<Archive>) -> ServiceState {
    recent_with_config(dir, map, archive, ServiceConfig::default())
}
fn recent_with_config(
    dir: &std::path::Path,
    map: &ShardMap,
    archive: Arc<Archive>,
    config: ServiceConfig,
) -> ServiceState {
    let roster = vec![
        RosterEntry {
            id: "archive".into(),
            role: WorkerRole::ArchiveOwner,
            replica_group: None,
            ssh_host: "unused".into(),
            upstream: "unused:1".into(),
            cache_bytes: 8 << 30,
            archive_range: None,
        },
        RosterEntry {
            id: "provider".into(),
            role: WorkerRole::RecentReplica,
            replica_group: Some("recent".into()),
            ssh_host: "unused".into(),
            upstream: "unused:2".into(),
            cache_bytes: 8 << 30,
            archive_range: None,
        },
    ];
    let assignment = plan(map, &super::map_sha256(map), &roster, 1, 0.1, generated()).unwrap();
    let set = ShardSet::open_with(
        dir,
        &LoadOptions {
            scope: LoadScope::Assigned {
                assignment: Arc::new(assignment),
                worker_id: "provider".into(),
            },
            ..LoadOptions::whole(0)
        },
    )
    .unwrap();
    ServiceState::build(set, config)
        .unwrap()
        .with_archive(archive)
}
struct Server {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    fn abort(self) {
        let _ = self.stop.send(());
    }
    async fn stop(self) {
        let _ = self.stop.send(());
        self.task.await.unwrap();
    }
}
async fn serve(state: ServiceState) -> (String, Server) {
    serve_app(router(state)).await
}
async fn serve_app(app: axum::Router) -> (String, Server) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let (stop, stopped) = tokio::sync::oneshot::channel();
    let task = tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = stopped.await;
            })
            .await
            .unwrap()
    });
    (format!("http://{addr}"), Server { stop, task })
}
fn terminal(dir: &std::path::Path, map: &ShardMap) -> ServiceState {
    let assignment = plan(
        map,
        &super::map_sha256(map),
        &super::roster(),
        1,
        0.1,
        generated(),
    )
    .unwrap();
    let options = LoadOptions {
        scope: LoadScope::Assigned {
            assignment: Arc::new(assignment),
            worker_id: "archive-01".into(),
        },
        ..LoadOptions::whole(0)
    };
    ServiceState::build(
        ShardSet::open_with(dir, &options).unwrap(),
        ServiceConfig {
            readiness: ReadinessMode::Warm,
            ..ServiceConfig::default()
        },
    )
    .unwrap()
}
async fn frozen_output(origin: String, tag: u32, birthday: u64) -> std::process::Output {
    let binary = std::env::var("WALLET_PIR_FROZEN_CLIENT")
        .expect("build the isolated frozen client and set WALLET_PIR_FROZEN_CLIENT");
    let script = hex::encode(script(tag).as_slice());
    let output = tokio::process::Command::new(binary)
        .args([origin, script, birthday.to_string()])
        .output()
        .await
        .unwrap();
    output
}
async fn frozen(origin: String, tag: u32, birthday: u64) -> serde_json::Value {
    let output = frozen_output(origin, tag, birthday).await;
    assert!(
        output.status.success(),
        "frozen client: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
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
    let dir = tempfile::tempdir().unwrap();
    let map = fixture_in(dir.path(), &DECOUPLED_2K_8K, true);
    let terminal = terminal(dir.path(), &map);
    terminal.spawn_prewarm().await.unwrap();
    let archive_metrics = terminal.metrics().clone();
    use std::sync::atomic::{AtomicBool, Ordering};
    let corrupt = Arc::new(AtomicBool::new(false));
    let corruption = corrupt.clone();
    let app = router(terminal).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| {
            let corrupt = corruption.load(Ordering::SeqCst);
            async move {
                let query = request.uri().path().contains("/query/");
                let mut response = next.run(request).await;
                if query && corrupt && response.status().is_success() {
                    let mut bytes = axum::body::to_bytes(
                        std::mem::replace(response.body_mut(), axum::body::Body::empty()),
                        1 << 20,
                    )
                    .await
                    .unwrap()
                    .to_vec();
                    bytes[0] ^= 1;
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
        transparent_shard_server::metrics::Metrics::get(&archive_metrics.queries),
        3,
        "recent execution must remain local"
    );
    corrupt.store(true, Ordering::SeqCst);
    let before = transparent_shard_server::metrics::Metrics::get(&archive_metrics.queries);
    let failed = frozen_output(origin.clone(), 1, FIRST).await;
    assert!(
        !failed.status.success(),
        "original released client must refuse an uncertain answer"
    );
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&archive_metrics.queries),
        before + 1,
        "original released client must not replay the uncertain submission"
    );
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
        let before = transparent_shard_server::metrics::Metrics::get(&archive_metrics.queries);
        let failed = client.post(&query).body(body.clone()).send().await.unwrap();
        assert_eq!(failed.status(), 503, "response fault {fault}");
        assert!(
            failed.headers().get("retry-after").is_none(),
            "uncertain answers must not invite replay"
        );
        assert_eq!(
            transparent_shard_server::metrics::Metrics::get(&archive_metrics.queries),
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
                .body(axum::body::Body::from(body))
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
        transparent_shard_server::metrics::Metrics::get(&state.metrics().body_bytes_in_flight) > 0
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
    drop(retained);
    assert_eq!(
        transparent_shard_server::metrics::Metrics::get(&state.metrics().body_bytes_in_flight),
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
    let shared = transparent_shard_server::runtime::SharedParams::build(
        &DECOUPLED_2K_8K,
        transparent_shard_server::shardset::Table::Directory,
    )
    .unwrap();
    let mut body = vec![0u8; shared.query_bytes()];
    body[..8].copy_from_slice(&transparent_shard::manifest::query_binding(
        &map.shards[0].manifest_digest,
        "directory",
    ));
    let held = state.hold_query_slot().await;
    let response = client
        .post(format!("{origin}{path}"))
        .body(body.clone())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), 503);
    assert_eq!(response.headers()["retry-after"], "1");
    drop(response);
    drop(held);
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
        while transparent_shard_server::metrics::Metrics::get(&state.metrics().body_bytes_in_flight)
            != body.len() as u64
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    drop(connection);
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        loop {
            if transparent_shard_server::metrics::Metrics::get(
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
    use transparent_shard_server::live::{Command, LiveService, Publication};
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
        while transparent_shard_server::metrics::Metrics::get(&old_metrics.body_bytes_in_flight)
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
