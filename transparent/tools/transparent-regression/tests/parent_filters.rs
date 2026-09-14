//! Real HTTP parent traversal + PIR, with SQLite restarts and an independent oracle.
#[path = "../../../services/transparent-shard-server/tests/common/mod.rs"]
mod common;
use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::get,
    Router,
};
use common::*;
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{Arc, Mutex},
};
use transparent_filter::{
    experimental_parent::{self as p, Manifest, Parent},
    ShardMap,
};
use transparent_wallet::{
    http::{HttpFilterSource, HttpOptions, HttpShardTransport},
    Anchor, Completion, ScriptEntry, StaticChain, StaticScripts, WorkLimits,
};
use transparent_wallet_store::SqliteStore;

#[derive(Clone)]
struct Origin {
    files: Arc<BTreeMap<String, Vec<u8>>>,
    requests: Arc<Mutex<Vec<String>>>,
}
async fn get_file(State(s): State<Origin>, Path(path): Path<String>) -> (StatusCode, Vec<u8>) {
    s.requests.lock().unwrap().push(path.clone());
    match s.files.get(&path) {
        Some(b) => (StatusCode::OK, b.clone()),
        None => (StatusCode::NOT_FOUND, vec![]),
    }
}
async fn origin(files: BTreeMap<String, Vec<u8>>) -> (String, Arc<Mutex<Vec<String>>>) {
    let requests = Arc::new(Mutex::new(Vec::new()));
    let app = Router::new()
        .route("/*path", get(get_file))
        .with_state(Origin {
            files: Arc::new(files),
            requests: requests.clone(),
        });
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let a = l.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(l, app).await.unwrap();
    });
    (format!("http://{a}"), requests)
}
async fn recover(
    db: std::path::PathBuf,
    map: ShardMap,
    pir: String,
    origin: String,
    tags: Vec<u32>,
    enabled: bool,
) -> (
    transparent_regression::Snapshot,
    transparent_wallet::SyncReport,
) {
    let height = map.shards.last().unwrap().end_height;
    recover_at(db, map, pir, origin, tags, enabled, height).await
}
#[allow(clippy::too_many_arguments)]
async fn recover_at(
    db: std::path::PathBuf,
    map: ShardMap,
    pir: String,
    origin: String,
    tags: Vec<u32>,
    enabled: bool,
    height: u64,
) -> (
    transparent_regression::Snapshot,
    transparent_wallet::SyncReport,
) {
    tokio::task::spawn_blocking(move || {
        let mut store = SqliteStore::open(db).unwrap();
        let options = HttpOptions::default();
        let mut filters = HttpFilterSource::new(&origin, &options).unwrap();
        if enabled {
            filters = filters
                .with_parent_experiment(format!("{origin}/candidate.json"), "recent-8k".into());
        }
        let mut transport = HttpShardTransport::new(pir, &options).unwrap();
        let geometry = transport.geometry().unwrap();
        let mut chain = StaticChain::from_map(&map);
        for h in FIRST.saturating_sub(1)..=height {
            chain.hashes.insert(h, hash_at(h).to_display_hex());
        }
        let mut provider = StaticScripts(
            tags.into_iter()
                .map(|tag| ScriptEntry {
                    script: script(tag).as_slice().to_vec(),
                    required_from: FIRST,
                    origin: transparent_wallet::store::ScriptOrigin::Derived,
                })
                .collect(),
        );
        let anchor = Anchor {
            height,
            hash: hash_at(height).to_display_hex(),
        };
        let report = transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &chain,
            &mut provider,
            &mut filters,
            &mut transport,
            &WorkLimits::default(),
            &anchor,
        )
        .unwrap();
        assert_eq!(report.completion, Completion::Complete);
        use transparent_wallet::WalletStore;
        for s in store.scripts().unwrap() {
            assert!(store
                .coverage(&s.script)
                .unwrap()
                .iter()
                .all(|r| r.end_height <= height));
        }

        (transparent_regression::actual(&store).unwrap(), report)
    })
    .await
    .unwrap()
}
#[tokio::test(flavor = "multi_thread")]
async fn parent_negatives_restart_import_and_corrupt_fallback_recover_exactly() {
    let all = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &all);
    let pir = serve(dir.path()).await;
    let mut files = BTreeMap::new();
    let mut sizes = Vec::new();
    for c in &map.shards {
        let bytes = std::fs::read(dir.path().join(&c.manifest_digest).join("filter.bin")).unwrap();
        sizes.push((c.manifest_digest.clone(), bytes.len() as u64));
        files.insert(format!("v1/filters/shards/{}/filter", c.shard_id), bytes);
    }
    files.insert(
        "v1/filters/shards".into(),
        serde_json::to_vec(&map).unwrap(),
    );
    let mut parents = Vec::new();
    for group in map
        .shards
        .iter()
        .filter(|c| c.sealed)
        .cloned()
        .collect::<Vec<_>>()
        .chunks(2)
    {
        let union: BTreeSet<_> = group
            .iter()
            .flat_map(|c| {
                all[c.shard_id as usize]
                    .iter()
                    .map(|(s, _)| s.as_slice().to_vec())
            })
            .collect();
        let mut parent = Parent {
            genesis_hash: map.genesis_hash.clone(),
            profile: map.profile.clone(),
            m: 1000,
            p: p::optimal_p(1000),
            children: group.to_vec(),
            filter_hash: String::new(),
            bytes: 0,
            elements: 0,
        };
        let bytes = parent.build(&union).unwrap();
        files.insert(format!("artifacts/{}.bin", parent.filter_hash), bytes);
        parents.push(parent);
    }
    let manifest = Manifest {
        schema: p::SCHEMA.into(),
        map_sha256: p::sha(&serde_json::to_vec(&map).unwrap()),
        parents,
        child_bytes: sizes,
    };
    files.insert(
        "candidate.json".into(),
        serde_json::to_vec(&manifest).unwrap(),
    );
    let (base, requests) = origin(files.clone()).await;
    let partial = dir.path().join("partial.sqlite");
    recover_at(
        partial.clone(),
        map.clone(),
        pir.clone(),
        base.clone(),
        vec![9_000_000],
        true,
        FIRST + SPAN - 2,
    )
    .await;
    requests.lock().unwrap().clear();
    recover_at(
        partial.clone(),
        map.clone(),
        pir.clone(),
        base.clone(),
        vec![9_000_000],
        true,
        FIRST + SPAN - 1,
    )
    .await;
    assert!(
        requests.lock().unwrap().is_empty(),
        "advancing within a cached parent must not fetch metadata or children"
    );
    {
        use transparent_wallet::WalletStore;
        let mut store = SqliteStore::open(&partial).unwrap();
        store
            .rollback_above(
                &Anchor {
                    height: FIRST + 10,
                    hash: hash_at(FIRST + 10).to_display_hex(),
                },
                "test accepted ancestor",
            )
            .unwrap();
    }
    requests.lock().unwrap().clear();
    recover_at(
        partial,
        map.clone(),
        pir.clone(),
        base.clone(),
        vec![9_000_000],
        true,
        FIRST + SPAN - 1,
    )
    .await;
    assert!(
        requests.lock().unwrap().is_empty(),
        "rollback may reuse a parent only under the same child identity"
    );
    let db = dir.path().join("wallet.sqlite");
    let (empty, _) = recover(
        db.clone(),
        map.clone(),
        pir.clone(),
        base.clone(),
        vec![9_000_000],
        true,
    )
    .await;
    assert!(empty.events.is_empty());
    assert!(requests
        .lock()
        .unwrap()
        .iter()
        .any(|p| p.starts_with("artifacts/")));
    requests.lock().unwrap().clear();
    recover(
        db.clone(),
        map.clone(),
        pir.clone(),
        base.clone(),
        vec![9_000_000],
        true,
    )
    .await;
    assert!(
        requests.lock().unwrap().is_empty(),
        "covered restart must not discover parents"
    );
    let expected = transparent_regression::reference(
        &all.iter()
            .flatten()
            .filter(|(s, _)| (1..=3).any(|t| script(t) == *s))
            .map(|(s, e)| transparent_regression::EventRecord::new(s.as_slice(), e))
            .collect::<Vec<_>>(),
        map.shards.last().unwrap().end_height,
    )
    .unwrap();
    let (actual, _) = recover(db, map.clone(), pir.clone(), base, vec![1, 2, 3], true).await;
    let imported_requests = requests.lock().unwrap().clone();
    assert!(
        !imported_requests
            .iter()
            .any(|p| p.starts_with("artifacts/")),
        "script import should reuse validated parent bytes: {imported_requests:?}"
    );
    assert_eq!(actual, expected);
    for mode in ["corrupt", "missing", "stale", "wrong-schema", "overlap"] {
        let mut changed = files.clone();
        if matches!(mode, "stale" | "wrong-schema" | "overlap") {
            let mut m = manifest.clone();
            match mode {
                "stale" => m.parents[0].children[0].revision += 1,
                "wrong-schema" => m.schema = "unknown-schema".into(),
                "overlap" => m.parents.push(m.parents[0].clone()),
                _ => unreachable!(),
            }
            changed.insert("candidate.json".into(), serde_json::to_vec(&m).unwrap());
        } else {
            for parent in &manifest.parents {
                let key = format!("artifacts/{}.bin", parent.filter_hash);
                if mode == "missing" {
                    changed.remove(&key);
                } else {
                    changed.insert(key, vec![0]);
                }
            }
        }
        let (base, requests) = origin(changed).await;
        let (actual, _) = recover(
            dir.path().join(format!("{mode}.sqlite")),
            map.clone(),
            pir.clone(),
            base,
            vec![1, 2, 3],
            true,
        )
        .await;
        assert_eq!(actual, expected, "{mode}");
        if matches!(mode, "stale" | "wrong-schema" | "overlap") {
            assert!(
                !requests
                    .lock()
                    .unwrap()
                    .iter()
                    .any(|p| p.starts_with("artifacts/")),
                "a rejected manifest must not be traversed: {mode}"
            );
        }
    }
}
