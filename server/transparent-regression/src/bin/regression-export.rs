//! Read-only journal export of explicit wallet cases and accepted checkpoint hashes.
//! No expected data comes from PIR responses or the wallet ledger reducer.
use clap::Parser;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    path::PathBuf,
};
use transparent_filter::ShardMap;
use transparent_regression::journal::EventStore;
use transparent_regression::{reference, validate, Case, Checkpoint, EventRecord, Fixture, SCHEMA};
use transparent_wallet::Anchor;

type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Parser)]
struct Args {
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long)]
    map: PathBuf,
    /// JSON list of case specifications; scripts are public synthetic groupings.
    #[arg(long)]
    cases: PathBuf,
    #[arg(long)]
    cutoff_height: u64,
    #[arg(long)]
    source_sha: String,
    #[arg(long)]
    out: PathBuf,
    /// Refuse unexpectedly large fixtures instead of exhausting publisher memory.
    #[arg(long, default_value_t = 1_000_000)]
    max_events: usize,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Spec {
    id: String,
    profile: String,
    scripts: Vec<String>,
    required_from: u64,
    heights: Vec<u64>,
}
fn main() -> Result<(), Error> {
    let a = Args::parse();
    if a.out.exists() {
        return Err("output must be new; frozen expectations are not overwritten".into());
    }
    let raw = std::fs::read(&a.map)?;
    let map: ShardMap = serde_json::from_slice(&raw)?;
    map.check_shape()?;
    let specs: Vec<Spec> = serde_json::from_slice(&std::fs::read(&a.cases)?)?;
    if specs.is_empty() {
        return Err("empty case specifications".into());
    }
    let end = map.shards.last().ok_or("empty map")?.end_height;
    let store = EventStore::open_existing(&a.data_dir)?;
    if store.start_height() != 0
        || store.genesis_hash() != map.genesis_hash
        || store.covered_through().is_none_or(|h| h < end)
    {
        return Err("journal must cover genesis through the publication on the same chain".into());
    }
    let scripts = specs
        .iter()
        .flat_map(|s| s.scripts.iter())
        .map(|s| Ok(hex::decode(s)?))
        .collect::<Result<BTreeSet<_>, Error>>()?;
    let mut events: BTreeMap<String, Vec<EventRecord>> =
        scripts.iter().map(|s| (hex::encode(s), vec![])).collect();
    let mut count = 0;
    for h in 0..=end {
        for (script, event) in store.events_at(h)?.unwrap_or_default() {
            if scripts.contains(script.as_slice()) {
                count += 1;
                if count > a.max_events {
                    return Err(
                        "fixture exceeds max-events; choose bounded representative wallets".into(),
                    );
                }
                events
                    .get_mut(&hex::encode(script.as_slice()))
                    .unwrap()
                    .push(EventRecord::new(script.as_slice(), &event));
            }
        }
        if h % 250_000 == 0 {
            eprintln!("journal height {h}/{end}, selected events {count}");
        }
    }
    let heights = map
        .shards
        .iter()
        .flat_map(|s| [s.start_height.saturating_sub(1), s.end_height])
        .chain(specs.iter().flat_map(|s| s.heights.iter().copied()))
        .collect::<BTreeSet<_>>();
    let mut accepted_headers = BTreeMap::new();
    for h in heights {
        let b = store
            .block_at(h)
            .ok_or("journal missing requested checkpoint block")?;
        accepted_headers.insert(h, b.block_hash.to_display_hex());
    }
    for entry in &map.shards {
        if accepted_headers.get(&entry.end_height) != Some(&entry.terminal_block_hash) {
            return Err("map disagrees with journal chain".into());
        }
    }
    let mut cases = vec![];
    for spec in specs {
        let all = spec
            .scripts
            .iter()
            .flat_map(|s| events.get(s).into_iter().flatten().cloned())
            .collect::<Vec<_>>();
        // A fixed-script birthday is valid only when it omits no earlier activity.
        if all.iter().any(|r| {
            r.decode()
                .is_ok_and(|e| transparent_regression::height(&e) < spec.required_from)
        }) {
            return Err(format!("{} birthday omits earlier activity", spec.id).into());
        }
        let mut checkpoints = vec![];
        for h in spec.heights {
            checkpoints.push(Checkpoint {
                anchor: Anchor {
                    height: h,
                    hash: accepted_headers
                        .get(&h)
                        .ok_or("missing accepted hash")?
                        .clone(),
                },
                expected: reference(&all, h).map_err(|e| -> Error { e.into() })?,
            });
        }
        cases.push(Case {
            id: spec.id,
            profile: spec.profile,
            scripts: spec.scripts,
            required_from: spec.required_from,
            checkpoints,
        });
    }
    let fixture = Fixture {
        schema: SCHEMA.into(),
        source_sha: a.source_sha,
        provenance: "Read-only full journal replay; public scripts grouped synthetically; shares ingest provenance; no real wallet population".into(),
        map_sha256: hex::encode(Sha256::digest(serde_json::to_vec(&map)?)),
        publication_file_sha256: hex::encode(Sha256::digest(raw)),
        map,
        cutoff_height: a.cutoff_height,
        accepted_headers,
        cases,
    };
    validate(&fixture).map_err(|e| -> Error { e.into() })?;
    let bytes = serde_json::to_vec_pretty(&fixture)?;
    std::fs::write(&a.out, &bytes)?;
    println!(
        "{}  {}",
        hex::encode(Sha256::digest(&bytes)),
        a.out.display()
    );
    Ok(())
}
