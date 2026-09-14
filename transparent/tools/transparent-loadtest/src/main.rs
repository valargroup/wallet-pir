//! Drives real wallet syncs against a transparent shard service over HTTP,
//! at stepped concurrency, and reports what wallets would experience.
//!
//! Each simulated client is one `sync_into` over its own store, with the
//! scripts and starting height a workload class prescribes, through the
//! wallet crate's own HTTP adapters — the same code path a wallet runs, not a
//! query generator. A sync counts as correct only when the events its store
//! holds digest to what the sampler derived from the journal for that wallet.
//!
//! Concurrency steps up until a saturation rule trips; every failure and
//! every incomplete sync stays in the denominators. Bytes are split by stage
//! as the wallet charges them; TLS and connection overhead are not measured
//! and the report says so rather than estimating them.

mod preparation_cache;
mod scenario;
mod simulation_report;

use anyhow::{bail, Context};
use clap::Parser;
use hdrhistogram::Histogram;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use transparent_filter::ShardMap;
use transparent_wallet::client::Table;
use transparent_wallet::http::{HttpFilterSource, HttpOptions, HttpShardTransport};
use transparent_wallet::store::{ScriptEntry, ScriptOrigin, WalletStore};
use transparent_wallet::transport::{BoxError, FilterSource, ShardTransport};
use transparent_wallet::{
    sync_into, Completion, IncompleteReason, MemoryStore, ServiceGeometry, StaticChain,
    StaticScripts, WorkLimits,
};
use transparent_wallet_store::SqliteStore;

#[derive(Parser, Debug, Clone)]
#[command(
    name = "transparent-loadtest",
    about = "Stepped-concurrency wallet syncs against a shard service",
    after_help = "For saved mixed-wallet scenarios: transparent-loadtest --scenario FILE --out-dir NEW_DIRECTORY. Add --scenario FILE --help for scenario options."
)]
struct Args {
    /// The private retrieval origin, e.g. https://transparent-pir.example.
    #[arg(long)]
    shard_url: String,
    /// The public filter origin; defaults to the shard origin.
    #[arg(long)]
    filter_url: Option<String>,
    /// The sampler's output.
    #[arg(long)]
    sample: PathBuf,
    /// Concurrency steps, in order.
    #[arg(long, value_delimiter = ',', default_values_t = [8usize, 32, 128, 512])]
    steps: Vec<usize>,
    /// Wall time per step.
    #[arg(long, default_value = "10m")]
    step_duration: humantime::Duration,
    /// Completed syncs per ordinary class a step aims for before it may end early.
    #[arg(long, default_value_t = 100)]
    min_completed_per_class: u64,
    #[arg(long, default_value_t = 0.05)]
    max_error_rate: f64,
    #[arg(long, default_value_t = 0.10)]
    max_503_rate: f64,
    /// Stop stepping when any ordinary class's p99 sync time exceeds this.
    #[arg(long)]
    max_p99_sync_s: Option<f64>,
    /// Private queries one sync may spend before it stops as incomplete; the
    /// wallet's own work budget. Unset is unlimited, and one script with a
    /// million events then holds a step for hours.
    #[arg(long)]
    max_queries: Option<u64>,
    /// Only these classes, comma separated.
    #[arg(long, value_delimiter = ',')]
    classes: Option<Vec<String>>,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long, default_value = "60s")]
    timeout: humantime::Duration,
    /// Keep each client's store on disk under this directory instead of in
    /// memory; measures the SQLite cost a real wallet pays.
    #[arg(long)]
    store_dir: Option<PathBuf>,
    /// Scrape these origins' /metrics at each step boundary: the workers
    /// over the VPC, since the router routes no operator path. Repeatable.
    #[arg(long = "metrics-url")]
    metrics_urls: Vec<String>,
    #[arg(long)]
    json_out: Option<PathBuf>,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    source_sha: Option<String>,
    #[arg(long)]
    host_sku: Option<String>,
    #[arg(long)]
    host_region: Option<String>,
    #[arg(long)]
    client_label: Option<String>,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct SampleClient {
    class: String,
    scripts: Vec<String>,
    required_from: u64,
    expected_digest: String,
    journal_events: u64,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct Sample {
    genesis_hash: String,
    start_height: u64,
    cutoff_height: u64,
    anchor_height: u64,
    anchor_hash: Option<String>,
    clients: Vec<SampleClient>,
}

/// Bytes and time per stage, as one client saw them.
#[derive(Default, Clone, Copy)]
struct StageTotals {
    calls: u64,
    up: u64,
    down: u64,
    micros: u64,
    http_409: u64,
    http_503: u64,
    failures: u64,
}

/// The bytes a (payload, cost) reply carried, or its error.
fn sized(result: &Result<(Vec<u8>, u64), BoxError>) -> Result<u64, &BoxError> {
    result.as_ref().map(|(bytes, _)| bytes.len() as u64)
}

/// A transport wrapper that records every request's stage, size, time and
/// refusal, without changing what the wallet does.
struct Counting<T> {
    inner: T,
    stages: Arc<Mutex<BTreeMap<&'static str, StageTotals>>>,
}

impl<T> Counting<T> {
    fn record(
        &self,
        stage: &'static str,
        up: u64,
        result: Result<u64, &BoxError>,
        elapsed: Duration,
    ) {
        let mut stages = self.stages.lock().unwrap();
        let entry = stages.entry(stage).or_default();
        entry.calls += 1;
        entry.up += up;
        entry.micros += elapsed.as_micros() as u64;
        match result {
            Ok(down) => entry.down += down,
            Err(error) => {
                let text = error.to_string();
                if transparent_wallet::StaleRevision::found_in(error).is_some() {
                    entry.http_409 += 1;
                } else if transparent_wallet::Overloaded::found_in(error).is_some()
                    || text.contains("503")
                {
                    entry.http_503 += 1;
                } else {
                    entry.failures += 1;
                }
            }
        }
    }
}

impl<T: ShardTransport> ShardTransport for Counting<T> {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        let result = self.inner.init();
        self.record("init", 0, sized(&result), started.elapsed());
        result
    }
    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        let result = self.inner.manifest(shard_id, revision);
        self.record("manifest", 0, sized(&result), started.elapsed());
        result
    }
    fn setup(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        let result = self.inner.setup(shard_id, revision, table, segment);
        self.record(
            match table {
                Table::Directory => "setup_directory",
                Table::Pages => "setup_pages",
            },
            0,
            sized(&result),
            started.elapsed(),
        );
        result
    }
    fn query(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        body: &[u8],
    ) -> Result<Vec<u8>, BoxError> {
        let started = Instant::now();
        let result = self.inner.query(shard_id, revision, table, body);
        self.record(
            match table {
                Table::Directory => "query_directory",
                Table::Pages => "query_pages",
            },
            body.len() as u64,
            result.as_ref().map(|b| b.len() as u64),
            started.elapsed(),
        );
        result
    }
}

struct CountingFilters<F> {
    inner: F,
    stages: Arc<Mutex<BTreeMap<&'static str, StageTotals>>>,
}

impl<F: FilterSource> FilterSource for CountingFilters<F> {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        let result = self.inner.shard_map();
        let mut stages = self.stages.lock().unwrap();
        let entry = stages.entry("map").or_default();
        entry.calls += 1;
        entry.micros += started.elapsed().as_micros() as u64;
        match sized(&result) {
            Ok(down) => entry.down += down,
            Err(_) => entry.failures += 1,
        }
        result
    }
    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        let started = Instant::now();
        let result = self.inner.filter(shard_id);
        let mut stages = self.stages.lock().unwrap();
        let entry = stages.entry("filters").or_default();
        entry.calls += 1;
        entry.micros += started.elapsed().as_micros() as u64;
        match sized(&result) {
            Ok(down) => entry.down += down,
            Err(_) => entry.failures += 1,
        }
        result
    }
}

/// One client's outcome.
struct Outcome {
    class: String,
    total: Duration,
    completed: bool,
    /// Why a sync stopped short, in the facade's words; None when complete.
    incomplete: Option<String>,
    exact: bool,
    failed: Option<String>,
    stages: BTreeMap<&'static str, StageTotals>,
    events: u64,
}

/// Digest of the events a store holds, matching the sampler's rule.
fn store_digest<S: WalletStore>(
    store: &S,
    from: u64,
    through: u64,
) -> anyhow::Result<(String, u64)> {
    let mut events: Vec<transparent_events::TransparentEvent> = store
        .events()?
        .into_iter()
        .map(|stored| stored.event)
        .filter(|event| {
            let height = match event {
                transparent_events::TransparentEvent::Receive(r) => u64::from(r.height),
                transparent_events::TransparentEvent::Spend(s) => u64::from(s.height),
            };
            height >= from && height <= through
        })
        .collect();
    events.sort_by_key(|event| event.sort_key());
    events.dedup();
    let mut hasher = Sha256::new();
    for event in &events {
        hasher.update(event.to_bytes());
    }
    Ok((hex::encode(hasher.finalize()), events.len() as u64))
}

fn run_client(
    args: &Args,
    spec: &SampleClient,
    map: &ShardMap,
    map_bytes: u64,
    geometry: &ServiceGeometry,
    index: usize,
) -> Outcome {
    let started = Instant::now();
    let stages: Arc<Mutex<BTreeMap<&'static str, StageTotals>>> =
        Arc::new(Mutex::new(BTreeMap::new()));
    let options = HttpOptions {
        timeout: *args.timeout,
        user_agent: "transparent-loadtest".into(),
    };
    let scripts: Vec<ScriptEntry> = spec
        .scripts
        .iter()
        .filter_map(|hex| hex::decode(hex).ok())
        .map(|script| ScriptEntry {
            script,
            origin: ScriptOrigin::Derived,
            required_from: spec.required_from,
        })
        .collect();
    let chain = StaticChain::from_map(map);
    let filter_url = args
        .filter_url
        .clone()
        .unwrap_or_else(|| args.shard_url.clone());
    let limits = WorkLimits {
        max_queries: args.max_queries,
        max_private_bytes: None,
    };
    let attempt = || -> anyhow::Result<(Option<String>, String, u64)> {
        let mut transport = Counting {
            inner: HttpShardTransport::new(&args.shard_url, &options)
                .map_err(|e| anyhow::anyhow!(e))?,
            stages: stages.clone(),
        };
        let mut filters = CountingFilters {
            inner: HttpFilterSource::new(&filter_url, &options).map_err(|e| anyhow::anyhow!(e))?,
            stages: stages.clone(),
        };
        let mut provider = StaticScripts(scripts.clone());
        let anchor = map.shards.last().map(|e| e.end_height).unwrap_or(0);
        let report = match &args.store_dir {
            Some(dir) => {
                let path = dir.join(format!("client-{index}.sqlite"));
                let _ = std::fs::remove_file(&path);
                let mut store = SqliteStore::open(&path)?;
                let report = sync_into(
                    &mut store,
                    map,
                    map_bytes,
                    geometry,
                    &chain,
                    &mut provider,
                    &mut filters,
                    &mut transport,
                    &limits,
                    &transparent_wallet::Anchor {
                        height: map.shards.last().unwrap().end_height,
                        hash: map.shards.last().unwrap().terminal_block_hash.clone(),
                    },
                )?;
                let (digest, events) = store_digest(&store, spec.required_from, anchor)?;
                let _ = std::fs::remove_file(&path);
                (incomplete_reason(&report.completion), digest, events)
            }
            None => {
                let mut store = MemoryStore::new();
                let report = sync_into(
                    &mut store,
                    map,
                    map_bytes,
                    geometry,
                    &chain,
                    &mut provider,
                    &mut filters,
                    &mut transport,
                    &limits,
                    &transparent_wallet::Anchor {
                        height: map.shards.last().unwrap().end_height,
                        hash: map.shards.last().unwrap().terminal_block_hash.clone(),
                    },
                )?;
                let (digest, events) = store_digest(&store, spec.required_from, anchor)?;
                (incomplete_reason(&report.completion), digest, events)
            }
        };
        Ok(report)
    };
    let result = attempt();
    let snapshot = stages.lock().unwrap().clone();
    match result {
        Ok((incomplete, digest, events)) => {
            // A synthetic wallet starts from an empty store at a height inside
            // its history, so a spend of an older output is unresolved by
            // construction; the wallet rightly withholds its anchor, but the
            // range was covered and the events are what the journal says.
            // That counts as complete here, and is reported by reason.
            let covered =
                incomplete.is_none() || incomplete.as_deref() == Some("unresolved-spends");
            Outcome {
                class: spec.class.clone(),
                total: started.elapsed(),
                completed: covered,
                exact: covered && digest == spec.expected_digest && events == spec.journal_events,
                incomplete,
                failed: None,
                stages: snapshot,
                events,
            }
        }
        Err(error) => Outcome {
            class: spec.class.clone(),
            total: started.elapsed(),
            completed: false,
            incomplete: None,
            exact: false,
            failed: Some(error.to_string()),
            stages: snapshot,
            events: 0,
        },
    }
}

#[derive(Default)]
struct ClassStats {
    n: u64,
    completed: u64,
    exact: u64,
    failed: u64,
    hist: Option<Histogram<u64>>,
    stages: BTreeMap<&'static str, StageTotals>,
    events: u64,
    /// Distinct failure messages and how often each occurred, capped so a
    /// report stays readable; the count is exact, the message set is not.
    failures_by_message: BTreeMap<String, u64>,
    /// Syncs that stopped short, by the reason the wallet gave.
    incomplete_by_reason: BTreeMap<String, u64>,
}

impl ClassStats {
    fn record(&mut self, outcome: &Outcome) {
        self.n += 1;
        if outcome.completed {
            self.completed += 1;
        }
        if outcome.exact {
            self.exact += 1;
        }
        if let Some(reason) = &outcome.incomplete {
            *self.incomplete_by_reason.entry(reason.clone()).or_default() += 1;
        }
        if let Some(message) = &outcome.failed {
            self.failed += 1;
            if self.failures_by_message.len() < 16 || self.failures_by_message.contains_key(message)
            {
                *self.failures_by_message.entry(message.clone()).or_default() += 1;
            }
        }
        let hist = self
            .hist
            .get_or_insert_with(|| Histogram::new_with_bounds(1, 3_600_000, 3).unwrap());
        let _ = hist.record((outcome.total.as_millis() as u64).max(1));
        for (stage, totals) in &outcome.stages {
            let entry = self.stages.entry(stage).or_default();
            entry.calls += totals.calls;
            entry.up += totals.up;
            entry.down += totals.down;
            entry.micros += totals.micros;
            entry.http_409 += totals.http_409;
            entry.http_503 += totals.http_503;
            entry.failures += totals.failures;
        }
        self.events += outcome.events;
    }

    fn json(&self) -> serde_json::Value {
        let hist = self.hist.as_ref();
        let ms = |p: f64| {
            hist.map(|h| h.value_at_quantile(p / 100.0) as f64 / 1e3)
                .unwrap_or(0.0)
        };
        let requests: u64 = self.stages.values().map(|s| s.calls).sum();
        let refused_503: u64 = self.stages.values().map(|s| s.http_503).sum();
        let refused_409: u64 = self.stages.values().map(|s| s.http_409).sum();
        serde_json::json!({
            "n": self.n,
            "completed": self.completed,
            "exact": self.exact,
            "mismatched": self.completed - self.exact.min(self.completed),
            "failed": self.failed,
            "sync_seconds": {"p50": ms(50.0), "p95": ms(95.0), "p99": ms(99.0), "max": hist.map(|h| h.max() as f64 / 1e3).unwrap_or(0.0)},
            "requests": requests,
            "http_409": refused_409,
            "http_503": refused_503,
            "events_recovered": self.events,
            "failures_by_message": self.failures_by_message,
            "incomplete_by_reason": self.incomplete_by_reason,
            "stages": self.stages.iter().map(|(stage, t)| (stage.to_string(), serde_json::json!({
                "calls": t.calls, "bytes_up": t.up, "bytes_down": t.down,
                "seconds": t.micros as f64 / 1e6, "http_409": t.http_409, "http_503": t.http_503, "failures": t.failures,
            }))).collect::<serde_json::Map<String, serde_json::Value>>(),
        })
    }
}

/// Every origin's `/metrics` text, keyed by origin; an origin that does not
/// answer is recorded as null rather than failing the run.
fn scrape_all(urls: &[String]) -> serde_json::Value {
    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .ok();
    let mut out = serde_json::Map::new();
    for url in urls {
        let text = client.as_ref().and_then(|client| {
            client
                .get(format!("{}/metrics", url.trim_end_matches('/')))
                .send()
                .ok()?
                .text()
                .ok()
        });
        out.insert(
            url.clone(),
            text.map(serde_json::Value::String)
                .unwrap_or(serde_json::Value::Null),
        );
    }
    serde_json::Value::Object(out)
}

fn main() -> anyhow::Result<()> {
    // Scenario children do blocking wallet work. Do not create a machine-sized
    // Tokio thread pool in every simulated wallet process.
    if std::env::args().any(|arg| arg == "--scenario-worker") {
        return scenario::entry(Arc::new(std::sync::atomic::AtomicBool::new(false)));
    }
    if std::env::args().any(|arg| arg == "--scenario" || arg.starts_with("--scenario=")) {
        let interrupted = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let signal = interrupted.clone();
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        // Register before admitting work: a supervisor can otherwise receive
        // SIGTERM before the signal thread installs its handler and lose its
        // final report instead of recording cancellation.
        #[cfg(unix)]
        let (mut terminate, mut interrupt) = {
            let _context = runtime.enter();
            (
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?,
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?,
            )
        };
        std::thread::spawn(move || {
            runtime.block_on(async move {
                #[cfg(unix)]
                {
                    tokio::select! { _ = interrupt.recv() => {}, _ = terminate.recv() => {} }
                }
                #[cfg(not(unix))]
                {
                    let _ = tokio::signal::ctrl_c().await;
                }
                signal.store(true, Ordering::Relaxed);
            })
        });
        return scenario::entry(interrupted);
    }
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(legacy_main())
}

async fn legacy_main() -> anyhow::Result<()> {
    let args = Args::parse();
    let sample: Sample =
        serde_json::from_slice(&std::fs::read(&args.sample).context("reading the sample")?)?;
    let sample_sha256 = hex::encode(Sha256::digest(std::fs::read(&args.sample)?));
    let options = HttpOptions {
        timeout: *args.timeout,
        user_agent: "transparent-loadtest".into(),
    };
    let filter_url = args
        .filter_url
        .clone()
        .unwrap_or_else(|| args.shard_url.clone());
    let (map, map_bytes) = HttpFilterSource::new(&filter_url, &options)
        .map_err(|e| anyhow::anyhow!(e))?
        .map()
        .map_err(|e| anyhow::anyhow!(e))?;
    let geometry = HttpShardTransport::new(&args.shard_url, &options)
        .map_err(|e| anyhow::anyhow!(e))?
        .geometry()
        .map_err(|e| anyhow::anyhow!(e))?;
    let tip = map
        .shards
        .last()
        .context("the map names no shards")?
        .clone();
    // The sample's expectations are about one set: refuse any other.
    if map.genesis_hash != sample.genesis_hash
        || map.start_height != sample.start_height
        || tip.end_height != sample.anchor_height
        || sample
            .anchor_hash
            .as_deref()
            .is_some_and(|h| h != tip.terminal_block_hash)
    {
        bail!(
            "the served set (genesis {}, {}-{}, tip {}) is not the sample's (genesis {}, {}-{}, tip {:?})",
            map.genesis_hash, map.start_height, tip.end_height, tip.terminal_block_hash,
            sample.genesis_hash, sample.start_height, sample.anchor_height, sample.anchor_hash
        );
    }
    let clients: Vec<SampleClient> = sample
        .clients
        .iter()
        .filter(|c| {
            args.classes
                .as_ref()
                .is_none_or(|wanted| wanted.contains(&c.class))
        })
        .cloned()
        .collect();
    if clients.is_empty() {
        bail!("no clients selected");
    }
    let map = Arc::new(map);
    let geometry = Arc::new(geometry);
    let args = Arc::new(args);

    let mut steps_out = Vec::new();
    let mut previous_rate: Option<f64> = None;
    let mut stopped_at: Option<usize> = None;
    let mut stop_reason: Option<String> = None;
    let started_all = Instant::now();
    for &concurrency in &args.steps {
        eprintln!(
            "== step: {concurrency} concurrent clients for {}",
            args.step_duration
        );
        let metrics_before = scrape_all(&args.metrics_urls);
        let next = Arc::new(AtomicUsize::new(0));
        let deadline = Instant::now() + *args.step_duration;
        let stats: Arc<Mutex<BTreeMap<String, ClassStats>>> = Arc::new(Mutex::new(BTreeMap::new()));
        let completed_total = Arc::new(AtomicU64::new(0));
        let mut workers = Vec::new();
        for worker in 0..concurrency {
            let args = args.clone();
            let clients = clients.clone();
            let map = map.clone();
            let geometry = geometry.clone();
            let next = next.clone();
            let stats = stats.clone();
            let completed_total = completed_total.clone();
            workers.push(tokio::task::spawn_blocking(move || {
                // Spread the start so the step does not open every connection
                // in one instant.
                std::thread::sleep(Duration::from_millis((worker as u64 * 37) % 1_000));
                while Instant::now() < deadline {
                    let index = next.fetch_add(1, Ordering::Relaxed);
                    let spec = &clients
                        [(index.wrapping_mul(2_654_435_761) ^ args.seed as usize) % clients.len()];
                    let outcome = run_client(&args, spec, &map, map_bytes, &geometry, index);
                    completed_total.fetch_add(1, Ordering::Relaxed);
                    stats
                        .lock()
                        .unwrap()
                        .entry(outcome.class.clone())
                        .or_default()
                        .record(&outcome);
                    // Enough of every ordinary class ends the step early.
                    let enough = {
                        let stats = stats.lock().unwrap();
                        !stats.is_empty()
                            && stats
                                .values()
                                .all(|s| s.completed >= args.min_completed_per_class)
                            && stats.len()
                                >= clients
                                    .iter()
                                    .map(|c| c.class.as_str())
                                    .collect::<std::collections::BTreeSet<_>>()
                                    .len()
                    };
                    if enough {
                        break;
                    }
                }
            }));
        }
        let step_started = Instant::now();
        for worker in workers {
            let _ = worker.await;
        }
        let elapsed = step_started.elapsed();
        let metrics_after = scrape_all(&args.metrics_urls);
        let stats = stats.lock().unwrap();
        let total: u64 = stats.values().map(|s| s.n).sum();
        let completed: u64 = stats.values().map(|s| s.completed).sum();
        let exact: u64 = stats.values().map(|s| s.exact).sum();
        let failed: u64 = stats.values().map(|s| s.failed).sum();
        let requests: u64 = stats
            .values()
            .flat_map(|s| s.stages.values().map(|t| t.calls))
            .sum();
        let refused_503: u64 = stats
            .values()
            .flat_map(|s| s.stages.values().map(|t| t.http_503))
            .sum();
        let rate = completed as f64 / elapsed.as_secs_f64().max(0.001);
        let error_rate = if total == 0 {
            1.0
        } else {
            (total - exact) as f64 / total as f64
        };
        let rate_503 = if requests == 0 {
            0.0
        } else {
            refused_503 as f64 / requests as f64
        };
        let worst_p99 = stats
            .values()
            .filter_map(|s| {
                s.hist
                    .as_ref()
                    .map(|h| h.value_at_quantile(0.99) as f64 / 1e3)
            })
            .fold(0.0, f64::max);
        eprintln!(
            "   {total} syncs, {completed} completed, {exact} exact, {failed} failed; {rate:.2} completed/s; error rate {error_rate:.3}; 503 rate {rate_503:.3}; worst p99 {worst_p99:.1}s"
        );
        steps_out.push(serde_json::json!({
            "concurrency": concurrency,
            "duration_seconds": elapsed.as_secs_f64(),
            "syncs": total,
            "completed": completed,
            "exact": exact,
            "failed": failed,
            "completed_syncs_per_second": rate,
            "error_rate": error_rate,
            "http_503_rate": rate_503,
            "worst_p99_sync_seconds": worst_p99,
            "classes": stats.iter().map(|(class, s)| (class.clone(), s.json())).collect::<serde_json::Map<_, _>>(),
            "metrics_before": metrics_before,
            "metrics_after": metrics_after,
        }));
        // The report so far, so a run stopped early still leaves its steps.
        write_report(
            &args,
            &map,
            &tip,
            &sample,
            &sample_sha256,
            &filter_url,
            &geometry.schema,
            clients.len(),
            &steps_out,
            None,
            None,
            started_all,
            true,
        )?;
        let mut reason = None;
        if error_rate > args.max_error_rate {
            reason = Some(format!(
                "error rate {error_rate:.3} above {}",
                args.max_error_rate
            ));
        } else if rate_503 > args.max_503_rate {
            reason = Some(format!(
                "503 rate {rate_503:.3} above {}",
                args.max_503_rate
            ));
        } else if args.max_p99_sync_s.is_some_and(|max| worst_p99 > max) {
            reason = Some(format!("p99 {worst_p99:.1}s above the objective"));
        } else if previous_rate.is_some_and(|previous| rate < 0.7 * previous) {
            reason = Some(format!(
                "throughput fell to {rate:.2}/s from {:.2}/s",
                previous_rate.unwrap()
            ));
        }
        previous_rate = Some(rate);
        if let Some(reason) = reason {
            eprintln!("   saturation: {reason}; not stepping further");
            stopped_at = Some(concurrency);
            stop_reason = Some(reason);
            break;
        }
    }

    write_report(
        &args,
        &map,
        &tip,
        &sample,
        &sample_sha256,
        &filter_url,
        &geometry.schema,
        clients.len(),
        &steps_out,
        stopped_at,
        stop_reason,
        started_all,
        false,
    )?;
    Ok(())
}

/// Writes the report; called after every step so an interrupted run keeps
/// what it measured, and once more at the end with `in_progress` false.
#[allow(clippy::too_many_arguments)]
fn write_report(
    args: &Args,
    map: &ShardMap,
    tip: &transparent_filter::ShardMapEntry,
    sample: &Sample,
    sample_sha256: &str,
    filter_url: &str,
    schema: &str,
    clients: usize,
    steps: &[serde_json::Value],
    stopped_at: Option<usize>,
    stop_reason: Option<String>,
    started_all: Instant,
    in_progress: bool,
) -> anyhow::Result<()> {
    let report = serde_json::json!({
        "schema": "transparent-loadtest-v1",
        "run_id": args.run_id,
        "generated_at_unix": std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0),
        "source_sha": args.source_sha,
        "command": std::env::args().collect::<Vec<_>>(),
        "shard_url": args.shard_url,
        "filter_url": filter_url,
        "set": {
            "schema": schema,
            "genesis_hash": map.genesis_hash,
            "network": map.network,
            "profile": map.profile,
            "start_height": map.start_height,
            "anchor_height": tip.end_height,
            "anchor_hash": tip.terminal_block_hash,
            "cutoff_height": sample.cutoff_height,
            "max_queries_per_sync": args.max_queries,
            "shards": map.shards.len(),
            "geometries": map.seal.keys().collect::<Vec<_>>(),
        },
        "sample": {"path": args.sample, "sha256": sample_sha256, "clients": clients},
        "host": {"sku": args.host_sku, "region": args.host_region},
        "client": {"label": args.client_label, "store": if args.store_dir.is_some() { "sqlite" } else { "memory" }},
        "steps": steps,
        "stopped_at_concurrency": stopped_at,
        "in_progress": in_progress,
        "stop_reason": stop_reason,
        "total_seconds": started_all.elapsed().as_secs_f64(),
        "limitations": [
            "TLS handshake and connection bytes are not measured; byte figures are wallet-level payloads as the wallet charges them",
            "clients are synthetic groupings of public scripts, not a user population",
            "a step ends early once every class reached min_completed_per_class; rates are computed over the step's actual duration",
            "a sync that stopped only for unresolved spends counts as completed: synthetic wallets begin with an empty store inside their history, so older receives are absent by construction; the count is reported per class",
        ],
    });
    let text = serde_json::to_string_pretty(&report)?;
    match &args.json_out {
        Some(path) => std::fs::write(path, &text)?,
        None => println!("{text}"),
    }
    let text = serde_json::to_string_pretty(&report)?;
    match &args.json_out {
        Some(path) => std::fs::write(path, &text)?,
        None if !in_progress => println!("{text}"),
        None => {}
    }
    Ok(())
}

/// The facade's wording for an incomplete sync, or None when complete.
fn incomplete_reason(completion: &Completion) -> Option<String> {
    match completion {
        Completion::Complete => None,
        Completion::Incomplete { reason, .. } => Some(match reason {
            IncompleteReason::QueryBudget => "query-budget".into(),
            IncompleteReason::ByteBudget => "byte-budget".into(),
            IncompleteReason::PendingLimit => "pending-limit".into(),
            IncompleteReason::Overloaded { shard_id } => format!("overloaded:{shard_id}"),
            IncompleteReason::ChainUnknown { height } => format!("chain-unknown:{height}"),
            IncompleteReason::PublicationBehind { height } => {
                format!("publication-behind:{height}")
            }
            IncompleteReason::UnresolvedSpends => "unresolved-spends".into(),
            IncompleteReason::DiscoveryUnbounded => "discovery-unbounded".into(),
        }),
    }
}
