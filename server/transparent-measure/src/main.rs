//! The measurement the POC exists to produce.
//!
//! Serves the published shard set, syncs a wallet against it from a birthday
//! over real HTTP with real PIR, checks the recovered ledger against an
//! independent traversal of the event journal, and reports what it cost.
//!
//! Two things are being established, and they are separate:
//!
//! 1. **Correctness.** The reconstruction equals the traversal exactly — every
//!    UTXO and every spend, not the balance. Two errors can sum to the right
//!    number, so a balance check would pass a reconstruction that had lost a
//!    receive and a spend of equal value.
//! 2. **Cost.** What a wallet pays, split by stage, against what the same
//!    coverage costs through compact scanning.
//!
//! The comparison is only meaningful because both sides cover the same range.
//! The scanning baseline is an extrapolation from a measured day, and is
//! labelled as such in the output rather than presented as a measurement of
//! this range.

mod workload;

/// One block's indexed events, as the journal holds them.
type JournalBlock = (u64, Vec<(ScriptBytes, TransparentEvent)>);

use clap::Parser;
use std::collections::BTreeMap;
use std::path::PathBuf;
use transparent_events::TransparentEvent;
use transparent_filter::{ScriptBytes, ShardMap};
use transparent_filter_server::events::EventStore;
use transparent_shard::manifest::ShardManifest;
use transparent_shard_server::service::{router, ServiceState};
use transparent_shard_server::shardset::ShardSet;
use transparent_wallet::client::Table;
use transparent_wallet::ledger::Ledger;
use transparent_wallet::sync::{sync, ServiceGeometry};
use transparent_wallet::transport::{BoxError, FilterSource, ShardTransport};
use workload::{ScriptCensus, Workload};

/// Incremental transparent bytes a shielded-scanning wallet downloads, measured
/// over 1,152 blocks in the mainnet study.
///
/// Used only to extrapolate a baseline for a longer range. It is a measurement
/// of a different, shorter range, so the comparison it supports is a screening
/// estimate and not a like-for-like result.
const SCANNING_BYTES_PER_1152_BLOCKS: f64 = 2_888_097.0;

#[derive(Parser)]
#[command(
    name = "transparent-measure",
    about = "Measure transparent recovery against a published shard set"
)]
struct Cli {
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "./transparent-shards")]
    shard_dir: PathBuf,
    /// Birthday to sync from. Defaults to the start of coverage, which is the
    /// case the design is meant to make cheap.
    #[arg(long)]
    birthday: Option<u64>,
    /// Where to write the JSON results.
    #[arg(long)]
    json_out: Option<PathBuf>,
}

/// Filters read from the published shard directories.
///
/// A separate source from the private transport on purpose: a wallet must not
/// fetch public bytes from the same place it makes private requests.
struct PublishedFilters {
    filters: BTreeMap<u64, Vec<u8>>,
}

impl FilterSource for PublishedFilters {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        Err("the map is supplied directly".into())
    }

    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self.filters.get(&shard_id).ok_or("no such shard")?.clone();
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }
}

struct HttpShards {
    base: String,
    client: reqwest::blocking::Client,
}

impl ShardTransport for HttpShards {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self
            .client
            .get(format!("{}/v1/shards/init", self.base))
            .send()?
            .error_for_status()?
            .bytes()?
            .to_vec();
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn setup(&mut self, shard_id: u64, table: Table) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self
            .client
            .get(format!(
                "{}/v1/shards/{shard_id}/setup/{}",
                self.base,
                table.as_str()
            ))
            .send()?
            .error_for_status()?
            .bytes()?
            .to_vec();
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn query(&mut self, shard_id: u64, table: Table, body: &[u8]) -> Result<Vec<u8>, BoxError> {
        Ok(self
            .client
            .post(format!(
                "{}/v1/shards/{shard_id}/query/{}",
                self.base,
                table.as_str()
            ))
            .body(body.to_vec())
            .send()?
            .error_for_status()?
            .bytes()?
            .to_vec())
    }
}

/// Streams the journal once, feeding a census and every workload's traversal.
///
/// One pass, because the journal is hundreds of megabytes and reading it per
/// workload would dominate the run for no benefit.
fn scan_journal(
    store: &EventStore,
    from_height: u64,
) -> Result<(ScriptCensus, Vec<JournalBlock>), BoxError> {
    let first = store.start_height();
    let covered = store.covered_through().ok_or("the journal is empty")?;
    let mut census = ScriptCensus::new();
    let mut blocks = Vec::new();
    for height in first..=covered {
        let events = store
            .events_at(height)?
            .ok_or_else(|| format!("height {height} is missing"))?;
        if height >= from_height {
            for (script, _) in &events {
                census.observe(script.as_slice());
            }
        }
        blocks.push((height, events));
    }
    Ok((census, blocks))
}

/// The independent result: the journal replayed directly, with no retrieval.
fn traverse(
    blocks: &[JournalBlock],
    wallet: &[ScriptBytes],
    from_height: u64,
) -> Result<Ledger, BoxError> {
    let wanted: std::collections::BTreeSet<&[u8]> =
        wallet.iter().map(|script| script.as_slice()).collect();
    let mut events: Vec<(Vec<u8>, TransparentEvent)> = Vec::new();
    for (height, block) in blocks {
        if *height < from_height {
            continue;
        }
        for (script, event) in block {
            if wanted.contains(script.as_slice()) {
                events.push((script.as_slice().to_vec(), *event));
            }
        }
    }
    let mut ledger = Ledger::new();
    ledger.replay(&mut events)?;
    Ok(ledger)
}

/// Exact equality, not equal balances.
fn compare(recovered: &Ledger, expected: &Ledger) -> Result<(), String> {
    let mut mine: Vec<_> = recovered.utxos().cloned().collect();
    let mut theirs: Vec<_> = expected.utxos().cloned().collect();
    mine.sort_by_key(|u| (u.txid, u.output_index));
    theirs.sort_by_key(|u| (u.txid, u.output_index));
    if mine != theirs {
        return Err(format!(
            "UTXO sets differ: recovered {}, expected {}",
            mine.len(),
            theirs.len()
        ));
    }
    let mut my_spends = recovered.spends().to_vec();
    let mut their_spends = expected.spends().to_vec();
    my_spends.sort_by_key(|s| (s.spent_txid, s.spent_output_index));
    their_spends.sort_by_key(|s| (s.spent_txid, s.spent_output_index));
    if my_spends != their_spends {
        return Err(format!(
            "spend sets differ: recovered {}, expected {}",
            my_spends.len(),
            their_spends.len()
        ));
    }
    if recovered.confirmed_balance() != expected.confirmed_balance() {
        return Err("balances differ".into());
    }
    if recovered.history() != expected.history() {
        return Err("histories differ".into());
    }
    Ok(())
}

#[derive(serde::Serialize)]
struct Measurement {
    workload: String,
    scripts: usize,
    journal_events: u64,
    recovered_utxos: usize,
    recovered_spends: usize,
    unresolved: usize,
    confirmed_balance: u64,
    matched_shards: usize,
    unproductive_matches: u64,
    filters_checked: u64,
    shards_opened: u64,
    queries: u64,
    map_bytes: u64,
    filter_bytes: u64,
    setup_bytes: u64,
    query_upload: u64,
    query_download: u64,
    total_bytes: u64,
    public_floor_bytes: u64,
    elapsed_seconds: f64,
    equality: String,
}

async fn serve(set: ShardSet) -> Result<String, BoxError> {
    let state = ServiceState::build(set).map_err(|e| -> BoxError { e.into() })?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let addr = listener.local_addr()?;
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    });
    Ok(format!("http://{addr}"))
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();

    let store = EventStore::open_existing(&cli.data_dir)?;
    let covered = store.covered_through().ok_or("the journal is empty")?;
    let first = store.start_height();
    let birthday = cli.birthday.unwrap_or(first);

    // Load the published set and its filters before serving, so a corrupt set
    // fails here rather than mid-measurement.
    let set = ShardSet::open(&cli.shard_dir)?;
    let map: ShardMap = set.map.clone();
    let map_bytes = serde_json::to_vec(&map)?.len() as u64;
    let mut filters = BTreeMap::new();
    for entry in std::fs::read_dir(&cli.shard_dir)? {
        let path = entry?.path();
        if !path.is_dir() {
            continue;
        }
        let manifest: ShardManifest =
            serde_json::from_slice(&std::fs::read(path.join("manifest.json"))?)?;
        filters.insert(manifest.shard_id, std::fs::read(path.join("filter.bin"))?);
    }

    eprintln!(
        "journal {first}-{covered}, {} shards, birthday {birthday}",
        map.shards.len()
    );
    let (census, blocks) = scan_journal(&store, birthday)?;
    eprintln!(
        "census: {} indexable scripts, {} events on scripts outside coverage",
        census.counts.len(),
        census.excluded
    );

    let base = serve(set).await?;
    let workloads: Vec<Workload> = workload::workloads(&census);
    let mut results = Vec::new();

    for load in workloads {
        let started = std::time::Instant::now();
        let scripts = load.scripts.clone();
        let map = map.clone();
        let base_url = base.clone();
        let shard_filters = PublishedFilters {
            filters: filters.clone(),
        };

        let outcome = tokio::task::spawn_blocking(move || -> Result<_, String> {
            let client = reqwest::blocking::Client::new();
            let raw = client
                .get(format!("{base_url}/v1/shards/init"))
                .send()
                .map_err(|e| e.to_string())?
                .bytes()
                .map_err(|e| e.to_string())?;
            let init: serde_json::Value =
                serde_json::from_slice(&raw).map_err(|e| e.to_string())?;
            let geometry = ServiceGeometry {
                directory_scheme: serde_json::from_value(init["directory_scheme"].clone())
                    .map_err(|e| e.to_string())?,
                directory_setup_seed: init["directory_setup_seed"].as_u64().unwrap_or_default(),
                pages_scheme: serde_json::from_value(init["pages_scheme"].clone())
                    .map_err(|e| e.to_string())?,
                pages_setup_seed: init["pages_setup_seed"].as_u64().unwrap_or_default(),
            };
            let mut transport = HttpShards {
                base: base_url,
                client,
            };
            let mut filters = shard_filters;
            sync(
                &map,
                map_bytes,
                &geometry,
                &mut filters,
                &mut transport,
                &scripts,
                birthday,
            )
            .map_err(|error| error.to_string())
        })
        .await?;

        let elapsed = started.elapsed().as_secs_f64();
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                eprintln!("  {}: FAILED: {error}", load.name);
                continue;
            }
        };

        let expected = traverse(&blocks, &load.scripts, birthday)?;
        let equality = match compare(&outcome.ledger, &expected) {
            Ok(()) => "exact".to_string(),
            Err(error) => format!("MISMATCH: {error}"),
        };

        let charges = outcome.charges;
        let measurement = Measurement {
            workload: load.name.clone(),
            scripts: load.scripts.len(),
            journal_events: load.journal_events,
            recovered_utxos: outcome.ledger.utxos().count(),
            recovered_spends: outcome.ledger.spends().len(),
            unresolved: outcome.ledger.unresolved().len(),
            confirmed_balance: outcome.ledger.confirmed_balance(),
            matched_shards: outcome.matched_shards.len(),
            unproductive_matches: outcome.unproductive_matches,
            filters_checked: charges.filters_checked,
            shards_opened: charges.shards_opened,
            queries: charges.queries,
            map_bytes: charges.map_bytes,
            filter_bytes: charges.filter_bytes,
            setup_bytes: charges.setup_bytes,
            query_upload: charges.query_upload,
            query_download: charges.query_download,
            total_bytes: charges.total(),
            public_floor_bytes: charges.public_floor(),
            elapsed_seconds: elapsed,
            equality: equality.clone(),
        };

        eprintln!(
            "  {:<38} {:>3} scripts  {:>4} queries  {:>10.2} MB  {}  ({:.1}s)",
            measurement.workload,
            measurement.scripts,
            measurement.queries,
            measurement.total_bytes as f64 / 1e6,
            measurement.equality,
            elapsed
        );
        results.push(measurement);
    }

    let blocks_covered = covered.saturating_sub(birthday) + 1;
    let scanning_baseline = SCANNING_BYTES_PER_1152_BLOCKS * blocks_covered as f64 / 1_152.0;

    eprintln!();
    eprintln!(
        "coverage {birthday}-{covered} ({blocks_covered} blocks); \
         compact-scanning baseline ~{:.1} MB (extrapolated from a measured day)",
        scanning_baseline / 1e6
    );
    for result in &results {
        eprintln!(
            "  {:<38} {:>8.2} MB total, floor {:>6.2} MB, {:>6.1}x vs scanning",
            result.workload,
            result.total_bytes as f64 / 1e6,
            result.public_floor_bytes as f64 / 1e6,
            scanning_baseline / result.total_bytes.max(1) as f64,
        );
    }

    let report = serde_json::json!({
        "journal": { "start": first, "covered_through": covered },
        "birthday": birthday,
        "blocks_covered": blocks_covered,
        "shards": map.shards.len(),
        "indexable_scripts": census.counts.len(),
        "events_outside_coverage": census.excluded,
        "scanning_baseline_bytes": scanning_baseline,
        "scanning_baseline_note":
            "extrapolated from 2,888,097 B measured over 1,152 blocks; not a measurement of this range",
        "results": results,
    });
    if let Some(path) = &cli.json_out {
        std::fs::write(path, serde_json::to_vec_pretty(&report)?)?;
        eprintln!("wrote {}", path.display());
    }

    if results.iter().any(|r| r.equality != "exact") {
        return Err("at least one workload did not reconstruct exactly".into());
    }
    Ok(())
}
