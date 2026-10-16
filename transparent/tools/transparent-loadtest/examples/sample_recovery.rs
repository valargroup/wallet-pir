//! Recovers every wallet of a sample once, over HTTP, and checks its events.
//!
//! Each wallet syncs from its `required_from` into a fresh in-memory store
//! with the wallet crate's own HTTP adapters, at the sample's anchor. It is
//! exact when the events the store holds in `[required_from, anchor]` digest
//! to the sample's `expected_digest` and number its `journal_events`: the
//! sample's own definition of an exact recovery. The completion is reported
//! beside it, because a set that starts after genesis cannot resolve a spend
//! of an output received before its first block; that is coverage, not
//! retrieval, and does not change the event digest.
//!
//! Prints one JSON line per wallet with its outcome and private bill.
//! Usage: sample_recovery SHARD_URL SAMPLE [CONCURRENCY]
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use zakura_transparent_private_shared::filter::ShardMap;
use zakura_transparent_private_wallet::engine::http::{
    HttpFilterSource, HttpOptions, HttpShardTransport,
};
use zakura_transparent_private_wallet::engine::store::{ScriptEntry, ScriptOrigin, WalletStore};
use zakura_transparent_private_wallet::engine::transport::{FilterSource, ShardTransport};
use zakura_transparent_private_wallet::engine::{
    parse_init, sync_into, Anchor, MemoryStore, StaticChain, StaticScripts, TableCharges,
    WorkLimits,
};

#[derive(serde::Deserialize, Clone)]
struct Client {
    class: String,
    scripts: Vec<String>,
    required_from: u64,
    expected_digest: String,
    journal_events: u64,
}

#[derive(serde::Deserialize)]
struct Sample {
    anchor_height: u64,
    anchor_hash: String,
    clients: Vec<Client>,
}

fn digest(store: &MemoryStore, from: u64, through: u64) -> anyhow::Result<(String, u64)> {
    let mut events: Vec<_> = store
        .events()
        .expect("events")
        .into_iter()
        .map(|stored| stored.event)
        .filter(|event| (from..=through).contains(&u64::from(event.height())))
        .collect();
    events.sort_by_key(|event| event.sort_key());
    events.dedup();
    let mut hasher = Sha256::new();
    for event in &events {
        hasher.update(event.to_bytes()?);
    }
    Ok((hex::encode(hasher.finalize()), events.len() as u64))
}

fn table(charges: &TableCharges) -> serde_json::Value {
    serde_json::json!({
        "queries": charges.queries,
        "setup_bytes": charges.setup_bytes,
        "query_upload": charges.query_upload,
        "query_download": charges.query_download,
        "segments_opened": charges.segments_opened,
    })
}

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let url = args.get(1).expect("SHARD_URL").clone();
    let sample: Sample =
        serde_json::from_slice(&std::fs::read(args.get(2).expect("SAMPLE")).unwrap()).unwrap();
    let concurrency: usize = args.get(3).map_or(4, |n| n.parse().unwrap());
    let options = HttpOptions::default();
    let anchor = Anchor {
        height: sample.anchor_height,
        hash: sample.anchor_hash.clone(),
    };
    let next = Arc::new(AtomicUsize::new(0));
    let clients = Arc::new(sample.clients);
    let out = Arc::new(Mutex::new(std::io::stdout()));
    let workers: Vec<_> = (0..concurrency)
        .map(|_| {
            let (next, clients, out, url, options, anchor) = (
                next.clone(),
                clients.clone(),
                out.clone(),
                url.clone(),
                options.clone(),
                anchor.clone(),
            );
            std::thread::spawn(move || loop {
                let index = next.fetch_add(1, Ordering::Relaxed);
                let Some(client) = clients.get(index).cloned() else {
                    break;
                };
                let started = std::time::Instant::now();
                let mut filters = HttpFilterSource::new(&url, &options).unwrap();
                let mut transport = HttpShardTransport::new(&url, &options).unwrap();
                let (map_raw, map_bytes) = filters.shard_map().unwrap();
                let map: ShardMap = serde_json::from_slice(&map_raw).unwrap();
                let (init, _) = transport.init().unwrap();
                let geometry = parse_init(&init).unwrap();
                let mut chain = StaticChain::from_map(&map);
                chain.hashes.insert(anchor.height, anchor.hash.clone());
                let mut scripts = StaticScripts(
                    client
                        .scripts
                        .iter()
                        .map(|script| ScriptEntry {
                            script: hex::decode(script).unwrap(),
                            origin: ScriptOrigin::Derived,
                            required_from: client.required_from,
                        })
                        .collect(),
                );
                let mut store = MemoryStore::new();
                let result = sync_into(
                    &mut store,
                    &map,
                    map_bytes,
                    &geometry,
                    &chain,
                    &mut scripts,
                    &mut filters,
                    &mut transport,
                    &WorkLimits::UNLIMITED,
                    &anchor,
                ).map_err(anyhow::Error::from).and_then(|report| {
                    let digest = digest(&store, client.required_from, anchor.height)?;
                    Ok((report, digest))
                });
                let line = match result {
                    Ok((report, (actual, events))) => {
                        let charges = report.charges;
                        serde_json::json!({
                            "index": index,
                            "class": client.class,
                            "scripts": client.scripts.len(),
                            "required_from": client.required_from,
                            "exact": actual == client.expected_digest && events == client.journal_events,
                            "digest": actual,
                            "events": events,
                            "expected_events": client.journal_events,
                            "completion": format!("{:?}", report.completion),
                            "unresolved_spends": report.ledger.unresolved().len(),
                            "seconds": started.elapsed().as_secs_f64(),
                            "map_bytes": charges.map_bytes,
                            "filter_bytes": charges.filter_bytes,
                            "manifest_bytes": charges.manifest_bytes,
                            "directory": table(&charges.directory),
                            "pages": table(&charges.pages),
                        })
                    }
                    Err(error) => serde_json::json!({
                        "index": index,
                        "class": client.class,
                        "exact": false,
                        "error": error.to_string(),
                    }),
                };
                let mut out = out.lock().unwrap();
                use std::io::Write;
                writeln!(out, "{line}").unwrap();
            })
        })
        .collect();
    for worker in workers {
        worker.join().unwrap();
    }
}
