//! Provider fixtures: published history sets, in-process recent and terminal
//! workers, and the released client. Shared by the provider tests and the
//! host installation tests in pir-e2e-tests.
#![allow(dead_code)]

use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_protocol::transparent::filter as transparent_filter;
use zakura_pir_publisher::transparent as transparent_shard;

use crate::{generated, genesis, hash_at, revision, script, FIRST};
use std::{collections::BTreeMap, sync::Arc};
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
use transparent_filter::{SealParameters, ShardMap};
use transparent_query_server::history::service::{
    router, ReadinessMode, ServiceConfig, ServiceState,
};
use transparent_query_server::routing::archive::Archive;
use transparent_query_server::routing::router::{plan, RosterEntry};
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K};
use transparent_shared::history::assignment::WorkerRole;
use transparent_shared::history::shardset::{LoadOptions, LoadScope, ShardSet};

pub(crate) fn fixture(dir: &std::path::Path, archive_geometry: &'static Geometry) -> ShardMap {
    fixture_in(dir, archive_geometry, false)
}
pub(crate) fn retain_fixture(name: &str, source: &std::path::Path) {
    let Some(output) = std::env::var_os("WALLET_PIR_QUALIFICATION_OUTPUT") else {
        return;
    };
    let target = std::path::PathBuf::from(output).join(name);
    fn copy(source: &std::path::Path, target: &std::path::Path) {
        std::fs::create_dir(target).unwrap();
        for entry in std::fs::read_dir(source).unwrap() {
            let entry = entry.unwrap();
            let path = target.join(entry.file_name());
            if entry.file_type().unwrap().is_dir() {
                copy(&entry.path(), &path);
            } else {
                assert!(entry.file_type().unwrap().is_file());
                // Inputs are finalized here and never mutated. Preserve the
                // original bytes after tempfile teardown without another RAM
                // copy; cross-filesystem runs fall back to a normal file copy.
                if std::fs::hard_link(entry.path(), &path).is_err() {
                    std::fs::copy(entry.path(), &path).unwrap();
                }
            }
        }
    }
    copy(source, &target);
}
pub(crate) fn retain_file(name: &str, source: &std::path::Path) {
    let Some(output) = std::env::var_os("WALLET_PIR_QUALIFICATION_OUTPUT") else {
        return;
    };
    let target = std::path::PathBuf::from(output).join(name);
    assert!(
        !target.exists(),
        "qualification inputs must never be overwritten"
    );
    if std::fs::hard_link(source, &target).is_err() {
        std::fs::copy(source, &target).unwrap();
    }
}
pub(crate) fn fixture_in(
    dir: &std::path::Path,
    archive_geometry: &'static Geometry,
    paged: bool,
) -> ShardMap {
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
        genesis_hash: crate::GENESIS.into(),
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
pub(crate) fn recent(dir: &std::path::Path, map: &ShardMap, archive: Arc<Archive>) -> ServiceState {
    recent_with_config(dir, map, archive, ServiceConfig::default())
}
pub(crate) fn recent_with_config(
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
    let assignment = plan(map, &crate::map_sha256(map), &roster, 1, 0.1, generated()).unwrap();
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
pub(crate) struct Server {
    stop: tokio::sync::oneshot::Sender<()>,
    task: tokio::task::JoinHandle<()>,
}
impl Server {
    pub(crate) fn abort(self) {
        let _ = self.stop.send(());
    }
    pub(crate) async fn stop(self) {
        let _ = self.stop.send(());
        self.task.await.unwrap();
    }
}
pub(crate) async fn serve(state: ServiceState) -> (String, Server) {
    serve_app(router(state)).await
}
pub(crate) async fn serve_app(app: axum::Router) -> (String, Server) {
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
pub(crate) fn terminal(dir: &std::path::Path, map: &ShardMap) -> ServiceState {
    terminal_prepared(dir, map, None)
}
pub(crate) fn terminal_prepared(
    dir: &std::path::Path,
    map: &ShardMap,
    shipped: Option<transparent_shared::runtime::disk::ShippedRuntimes>,
) -> ServiceState {
    let assignment = plan(
        map,
        &crate::map_sha256(map),
        &crate::roster(),
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
    ServiceState::build_with_runtimes(
        ShardSet::open_with(dir, &options).unwrap(),
        ServiceConfig {
            readiness: ReadinessMode::Warm,
            query_slots: 1,
            max_waiters: 0,
            ..ServiceConfig::default()
        },
        None,
        shipped,
    )
    .unwrap()
}
pub(crate) async fn frozen_output(origin: String, tag: u32, birthday: u64) -> std::process::Output {
    let binary = std::env::var("WALLET_PIR_FROZEN_CLIENT")
        .expect("build the isolated frozen client and set WALLET_PIR_FROZEN_CLIENT");
    let script = hex::encode(script(tag).as_slice());
    tokio::process::Command::new(binary)
        .args([origin, script, birthday.to_string()])
        .output()
        .await
        .unwrap()
}
pub(crate) async fn frozen(origin: String, tag: u32, birthday: u64) -> serde_json::Value {
    let output = frozen_output(origin, tag, birthday).await;
    assert!(
        output.status.success(),
        "frozen client: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).unwrap()
}
