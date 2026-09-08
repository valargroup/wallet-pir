//! Reproducible mixed-wallet scenarios. The supervisor owns admission, evidence
//! and hard deadlines; blocking wallet work runs in killable child processes.
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{mpsc, Arc};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context, Result};
use clap::Parser;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use transparent_wallet::http::{HttpFilterSource, HttpObserver, HttpOptions, HttpShardTransport};
use transparent_wallet::store::{
    ScriptEntry, ScriptOrigin, SetIdentity, ShardCommit, StoredEvent, WalletStore,
};
use transparent_wallet::{
    sync_into, Anchor, Completion, MemoryStore, StaticChain, StaticScripts, WorkLimits,
};
use transparent_wallet_store::SqliteStore;

use crate::simulation_report::{self, Report};
use crate::{Sample, SampleClient};

#[derive(Parser)]
struct Cli {
    #[arg(long)]
    scenario: Option<PathBuf>,
    #[arg(long)]
    out_dir: Option<PathBuf>,
    #[arg(long)]
    run_id: Option<String>,
    #[arg(long)]
    shard_url: Option<String>,
    #[arg(long)]
    filter_url: Option<String>,
    /// Named full /metrics URL; repeat for every measured worker.
    #[arg(long, value_parser = parse_target)]
    metrics_target: Vec<(String, String)>,
    #[arg(long, hide = true)]
    scenario_worker: bool,
}

fn parse_target(value: &str) -> Result<(String, String), String> {
    let (name, url) = value
        .split_once('=')
        .ok_or("expected NAME=http(s)://host/metrics")?;
    if name.is_empty() {
        return Err("empty metrics target name".into());
    }
    Ok((name.to_owned(), url.to_owned()))
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    pub schema: String,
    pub name: String,
    pub mode: Mode,
    pub sample: PathBuf,
    pub shard_url: String,
    #[serde(default)]
    pub filter_url: Option<String>,
    pub profiles: BTreeMap<String, usize>,
    #[serde(default = "default_seed")]
    pub seed: u64,
    #[serde(default = "preparation_concurrency")]
    pub preparation_concurrency: usize,
    #[serde(default = "preparation_deadline")]
    pub preparation_deadline_seconds: u64,
    #[serde(default = "ten_minutes")]
    pub duration_seconds: u64,
    #[serde(default = "ten_minutes")]
    pub recovery_deadline_seconds: u64,
    #[serde(default = "request_timeout")]
    pub request_timeout_seconds: u64,
    #[serde(default)]
    pub max_queries: Option<u64>,
    #[serde(default)]
    pub store: Store,
    #[serde(default)]
    pub metrics_targets: BTreeMap<String, String>,
    #[serde(default)]
    pub max_p99_exact_seconds: Option<f64>,
    #[serde(default)]
    pub notes: String,
}
fn preparation_concurrency() -> usize {
    2
}

fn preparation_deadline() -> u64 {
    3600
}

fn default_seed() -> u64 {
    1
}
fn ten_minutes() -> u64 {
    600
}
fn request_timeout() -> u64 {
    60
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    Wave,
    Sustained,
}
#[derive(Clone, Copy, Debug, Default, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Store {
    #[default]
    Sqlite,
    Memory,
}

impl Config {
    fn validate(&self) -> Result<()> {
        if self.schema != "transparent-scenario-v1" {
            bail!("unsupported scenario schema");
        }
        if !(1..=512).contains(&self.preparation_concurrency) {
            bail!("preparation_concurrency must be between 1 and 512");
        }
        if self.profiles.is_empty()
            || self.profiles.values().any(|n| *n == 0)
            || self
                .profiles
                .values()
                .try_fold(0usize, |a, b| a.checked_add(*b))
                .is_none_or(|n| n > 512)
        {
            bail!("profiles must contain 1–512 total slots and no zero counts");
        }
        if self.preparation_deadline_seconds == 0
            || self.duration_seconds == 0
            || self.recovery_deadline_seconds == 0
            || self.request_timeout_seconds == 0
        {
            bail!("durations and timeouts must be positive");
        }
        if self
            .max_p99_exact_seconds
            .is_some_and(|n| !n.is_finite() || n <= 0.0)
        {
            bail!("invalid latency objective");
        }
        if self.metrics_targets.keys().any(|name| {
            name.is_empty()
                || name == "load-client"
                || !name
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
        }) {
            bail!("metrics target names must be plain tokens other than load-client");
        }
        for url in std::iter::once(&self.shard_url)
            .chain(self.filter_url.iter())
            .chain(self.metrics_targets.values())
        {
            let parsed = reqwest::Url::parse(url).context("invalid origin or metrics URL")?;
            if !matches!(parsed.scheme(), "http" | "https")
                || parsed.host_str().is_none()
                || !parsed.username().is_empty()
                || parsed.password().is_some()
                || parsed.query().is_some()
                || parsed.fragment().is_some()
            {
                bail!("URLs must be HTTP(S), without credentials, query strings or fragments");
            }
        }
        Ok(())
    }
    fn options(&self) -> HttpOptions {
        HttpOptions {
            timeout: Duration::from_secs(self.request_timeout_seconds),
            user_agent: "transparent-simulation".into(),
        }
    }
    fn filter_origin(&self) -> &str {
        self.filter_url.as_deref().unwrap_or(&self.shard_url)
    }
}

pub fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}
fn digest(bytes: &[u8]) -> String {
    hex::encode(Sha256::digest(bytes))
}
fn io_error(error: transparent_wallet::transport::BoxError) -> anyhow::Error {
    anyhow::anyhow!(error.to_string())
}

/// Independent deterministic shuffle per class, with active sample exclusion.
struct Selector {
    pools: BTreeMap<String, Vec<usize>>,
    cursors: BTreeMap<String, usize>,
}
impl Selector {
    fn new(config: &Config, clients: &[SampleClient]) -> Result<Self> {
        let mut pools = BTreeMap::new();
        let mut random = config.seed;
        for (class, slots) in &config.profiles {
            let mut seen = BTreeSet::new();
            let mut pool = Vec::new();
            for (index, client) in clients
                .iter()
                .enumerate()
                .filter(|(_, c)| &c.class == class)
            {
                let mut scripts = client.scripts.clone();
                scripts.sort();
                scripts.dedup();
                if seen.insert((scripts, client.required_from)) {
                    pool.push(index);
                }
            }
            if pool.len() < *slots {
                bail!(
                    "class {class} needs {slots} distinct wallets, sample has {}",
                    pool.len()
                );
            }
            for i in (1..pool.len()).rev() {
                random = random
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                pool.swap(i, (random % (i as u64 + 1)) as usize);
            }
            pools.insert(class.clone(), pool);
        }
        Ok(Self {
            pools,
            cursors: BTreeMap::new(),
        })
    }
    fn next(&mut self, class: &str, active: &BTreeSet<usize>) -> usize {
        let pool = &self.pools[class];
        let cursor = self.cursors.entry(class.into()).or_default();
        for _ in 0..pool.len() {
            let candidate = pool[*cursor % pool.len()];
            *cursor = (*cursor + 1) % pool.len();
            if !active.contains(&candidate) {
                return candidate;
            }
        }
        unreachable!("pool has at least as many wallets as slots")
    }
}

#[derive(Clone, Serialize, Deserialize)]
struct Job {
    id: usize,
    config: Config,
    wallet: SampleClient,
    map_digest: String,
    store_path: PathBuf,
    seed_path: PathBuf,
    preparing: bool,
}

#[derive(Serialize, Deserialize)]
struct Seed {
    map_sha256: String,
    events: Vec<SeedEvent>,
}

#[derive(Serialize, Deserialize)]
struct SeedEvent {
    script: String,
    event: String,
}

/// Import prior ledger events without claiming coverage for the measured window
/// or warming its filter/setup caches. Every seed comes from a complete recovery
/// against the same pinned publication in the separate preparation phase.
fn seed_store(
    store: &mut impl WalletStore,
    map: &transparent_filter::ShardMap,
    path: &Path,
    wallet: &SampleClient,
) -> Result<()> {
    let seed: Seed = serde_json::from_slice(
        &fs::read(path)
            .with_context(|| format!("missing prepared wallet history: {}", path.display()))?,
    )?;
    if seed.map_sha256 != digest(&serde_json::to_vec(map)?) {
        bail!("publication changed since wallet preparation");
    }
    store.bind_set(&SetIdentity::of(map))?;
    let mut by_shard: BTreeMap<usize, Vec<StoredEvent>> = BTreeMap::new();
    for entry in seed.events {
        let event = transparent_events::TransparentEvent::from_bytes(&hex::decode(entry.event)?)?;
        let height = u64::from(event.height());
        if height >= wallet.required_from || !wallet.scripts.contains(&entry.script) {
            bail!("seed contains an event outside the prior wallet history");
        }
        let index = map
            .shards
            .iter()
            .position(|s| s.start_height <= height && height <= s.end_height)
            .context("seed event is outside the publication")?;
        by_shard.entry(index).or_default().push(StoredEvent {
            script: hex::decode(entry.script)?,
            event,
            shard_id: map.shards[index].shard_id,
            revision_digest: map.shards[index].manifest_digest.clone(),
        });
    }
    for (index, events) in by_shard {
        let shard = &map.shards[index];
        store.commit_shard(ShardCommit {
            shard_id: shard.shard_id,
            revision_digest: shard.manifest_digest.clone(),
            sealed: shard.sealed,
            start_height: shard.start_height,
            end_height: shard.end_height,
            terminal_block_hash: shard.terminal_block_hash.clone(),
            events,
            ..Default::default()
        })?;
    }
    if !store.ledger()?.unresolved().is_empty() {
        bail!("prepared history contains unresolved spends");
    }
    Ok(())
}

fn emit(value: &Value) -> Result<()> {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    serde_json::to_writer(&mut out, value)?;
    writeln!(out)?;
    out.flush()?;
    Ok(())
}

/// Internal protocol: ready, then one JSON job per line and request/outcome events
/// on stdout. No telemetry contains wallet scripts or private request bodies.
fn worker() -> Result<()> {
    emit(&json!({"type":"ready"}))?;
    for line in std::io::stdin().lock().lines() {
        let job: Job = serde_json::from_str(&line?)?;
        let started = now();
        emit(&json!({"type":"started", "id":job.id, "at":started}))?;
        let result = recover(&job);
        let event = match result {
            Ok(mut result) => {
                result["type"] = json!("outcome");
                result["id"] = json!(job.id);
                result["at"] = json!(now());
                result
            }
            Err(error) => {
                json!({"type":"outcome", "id":job.id, "at":now(), "outcome":"failed", "error":error.to_string()})
            }
        };
        emit(&event)?;
    }
    Ok(())
}

fn recover(job: &Job) -> Result<Value> {
    let id = job.id;
    let observer: HttpObserver = Arc::new(move |o| {
        // A broken pipe stops telemetry and the process: continuing invisible
        // work after the supervisor is gone would violate the concurrency bound.
        if emit(
            &json!({"type":"request", "id":id, "at":now(), "stage":o.stage,
            "status":o.status, "seconds":o.elapsed.as_secs_f64(), "bytes_up":o.bytes_up,
            "bytes_down":o.bytes_down, "failed":o.failed}),
        )
        .is_err()
        {
            std::process::exit(2);
        }
    });
    let options = job.config.options();
    let mut filters = HttpFilterSource::new(job.config.filter_origin(), &options)
        .map_err(io_error)?
        .with_observer(observer.clone())
        .with_retry_attempts(if job.preparing { 3 } else { 1 });
    let mut transport = HttpShardTransport::new(&job.config.shard_url, &options)
        .map_err(io_error)?
        .with_observer(observer)
        .with_retry_attempts(if job.preparing { 3 } else { 1 });
    let (map, map_bytes) = filters.map().map_err(io_error)?;
    if digest(&serde_json::to_vec(&map)?) != job.map_digest {
        bail!("publication changed before recovery");
    }
    let geometry = transport.geometry().map_err(io_error)?;
    let tip = map.shards.last().context("empty map")?;
    let anchor = Anchor {
        height: tip.end_height,
        hash: tip.terminal_block_hash.clone(),
    };
    let scripts: Vec<ScriptEntry> = job
        .wallet
        .scripts
        .iter()
        .map(|s| {
            Ok(ScriptEntry {
                script: hex::decode(s)?,
                required_from: if job.preparing {
                    map.start_height
                } else {
                    job.wallet.required_from
                },
                origin: ScriptOrigin::Derived,
            })
        })
        .collect::<Result<_>>()?;
    let mut provider = StaticScripts(scripts);
    let chain = StaticChain::from_map(&map);
    let limits = WorkLimits {
        max_queries: job.config.max_queries,
        max_private_bytes: None,
    };
    let mut store: Box<dyn WalletStore> = match job.config.store {
        Store::Sqlite => Box::new(SqliteStore::open(&job.store_path)?),
        Store::Memory => Box::new(MemoryStore::new()),
    };
    if !job.preparing && job.wallet.required_from > map.start_height {
        seed_store(&mut store, &map, &job.seed_path, &job.wallet)?;
    }
    let seeded_events = store.events()?.len();
    let report = sync_into(
        &mut store,
        &map,
        map_bytes,
        &geometry,
        &chain,
        &mut provider,
        &mut filters,
        &mut transport,
        &limits,
        &anchor,
    )?;
    let (actual, events) = crate::store_digest(&store, job.wallet.required_from, anchor.height)?;
    let outcome = if report.completion != Completion::Complete {
        "incomplete"
    } else if actual == job.wallet.expected_digest && events == job.wallet.journal_events {
        "exact"
    } else {
        "mismatched"
    };
    let unresolved_spends = report.ledger.unresolved().len();
    if outcome == "exact" && job.preparing {
        let seed: Vec<SeedEvent> = store
            .events()?
            .into_iter()
            .filter(|e| u64::from(e.event.height()) < job.wallet.required_from)
            .map(|e| SeedEvent {
                script: hex::encode(e.script),
                event: hex::encode(e.event.to_bytes()),
            })
            .collect();
        fs::write(
            &job.seed_path,
            serde_json::to_vec(&Seed {
                map_sha256: job.map_digest.clone(),
                events: seed,
            })?,
        )?;
    }
    drop(store);
    if outcome == "exact" && job.config.store == Store::Sqlite {
        fs::remove_file(&job.store_path)?;
    }
    Ok(
        json!({"outcome":outcome, "events":events, "actual_digest":actual, "events_exact":actual == job.wallet.expected_digest && events == job.wallet.journal_events, "unresolved_spends":unresolved_spends, "seeded_events":seeded_events,
        "completion":format!("{:?}",report.completion), "map_refreshes":report.map_refreshes,
        "matched_shards":report.matched_shards.len(), "commits":report.commits}),
    )
}

struct Process {
    child: Child,
    input: ChildStdin,
}
impl Process {
    fn spawn(slot: usize, generation: u64, tx: mpsc::Sender<Message>, logs: &Path) -> Result<Self> {
        let mut child = Command::new(std::env::current_exe()?)
            .arg("--scenario-worker")
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(File::create(
                logs.join(format!("worker-{slot}-{generation}.log")),
            )?)
            .spawn()?;
        let input = child.stdin.take().context("worker stdin")?;
        let stdout = child.stdout.take().context("worker stdout")?;
        std::thread::spawn(move || {
            for line in BufReader::new(stdout).lines() {
                let event = line
                    .map_err(anyhow::Error::from)
                    .and_then(|line| Ok(serde_json::from_str::<Value>(&line)?));
                match event {
                    Ok(value) => {
                        if tx.send(Message::Worker(slot, generation, value)).is_err() {
                            return;
                        }
                    }
                    Err(error) => {
                        let _ = tx.send(Message::Gone(slot, generation, error.to_string()));
                        return;
                    }
                }
            }
            let _ = tx.send(Message::Gone(slot, generation, "worker exited".into()));
        });
        Ok(Self { child, input })
    }
    fn send(&mut self, job: &Job) -> Result<()> {
        serde_json::to_writer(&mut self.input, job)?;
        writeln!(self.input)?;
        self.input.flush()?;
        Ok(())
    }
}
impl Drop for Process {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
struct Slot {
    class: String,
    process: Option<Process>,
    generation: u64,
    ready: bool,
    spawned: Instant,
    active: Option<(usize, usize, Instant)>, // run id, sample index, deadline origin
    wave_started: bool,
}
enum Message {
    Worker(usize, u64, Value),
    Gone(usize, u64, String),
    Metrics(Value),
}

fn scrape_thread(
    targets: BTreeMap<String, String>,
    tx: mpsc::Sender<Message>,
    stop: Arc<AtomicBool>,
) -> std::thread::JoinHandle<()> {
    std::thread::spawn(move || {
        // Independent target threads prevent an unavailable worker delaying all others.
        let handles: Vec<_> = targets
            .into_iter()
            .map(|(name, url)| {
                let tx = tx.clone();
                let stop = stop.clone();
                std::thread::spawn(move || {
                    let client = reqwest::blocking::Client::builder()
                        .timeout(Duration::from_secs(1))
                        .build();
                    loop {
                        let tick = Instant::now();
                        let final_scrape = stop.load(Ordering::Relaxed);
                        let result = client.as_ref().map_err(|e| e.to_string()).and_then(|c| {
                            c.get(&url)
                                .send()
                                .and_then(|r| r.error_for_status())
                                .and_then(|r| r.text())
                                .map_err(|e| e.to_string())
                        });
                        let event = match result {
                            Ok(text) => json!({"at":now(), "target":name, "text":text}),
                            Err(error) => json!({"at":now(), "target":name, "error":error}),
                        };
                        if tx.send(Message::Metrics(event)).is_err() || final_scrape {
                            break;
                        }
                        while tick.elapsed() < Duration::from_secs(1)
                            && !stop.load(Ordering::Relaxed)
                        {
                            std::thread::sleep(Duration::from_millis(25));
                        }
                    }
                })
            })
            .collect();
        for handle in handles {
            let _ = handle.join();
        }
    })
}

fn write_line(file: &mut File, value: &Value) -> Result<()> {
    serde_json::to_writer(&mut *file, value)?;
    writeln!(file)?;
    file.flush()?;
    Ok(())
}

pub fn entry(interrupted: Arc<AtomicBool>) -> Result<()> {
    let cli = Cli::parse();
    if cli.scenario_worker {
        return worker();
    }
    let path = cli.scenario.context("--scenario is required")?;
    let mut config: Config = serde_json::from_slice(&fs::read(&path)?)?;
    if let Some(url) = cli.shard_url {
        config.shard_url = url;
    }
    if let Some(url) = cli.filter_url {
        config.filter_url = Some(url);
    }
    for (name, url) in cli.metrics_target {
        config.metrics_targets.insert(name, url);
    }
    config.validate()?;
    // Paths in saved scenarios are relative to the scenario, not the shell cwd.
    config.sample = fs::canonicalize(path.parent().unwrap_or(Path::new(".")).join(&config.sample))?;
    let sample_bytes = fs::read(&config.sample)?;
    let sample: Sample = serde_json::from_slice(&sample_bytes)?;
    if sample.anchor_hash.is_none() {
        bail!("scenario samples must pin an anchor hash");
    }
    for client in &sample.clients {
        if client.scripts.is_empty()
            || client.scripts.iter().any(|s| hex::decode(s).is_err())
            || client.required_from < sample.start_height
            || client.required_from > sample.anchor_height
        {
            bail!("invalid sample wallet");
        }
    }
    let selector = Selector::new(&config, &sample.clients)?;
    let out = cli.out_dir.context("--out-dir is required")?;
    fs::create_dir(&out).context("output directory must be new")?;
    fs::create_dir(out.join("stores"))?;
    fs::create_dir(out.join("logs"))?;
    let out = fs::canonicalize(out)?;
    let mut report = Report::new(&config, digest(&sample_bytes));
    if let Some(run_id) = cli.run_id {
        report.run_id = run_id;
    }
    fs::write(
        out.join("scenario.json"),
        serde_json::to_vec_pretty(&config)?,
    )?;
    let seeds = out.join("seeds");
    fs::create_dir(&seeds)?;
    simulation_report::write(&out, &report)?;
    let result = (|| {
        prepare(
            &config,
            &sample,
            &selector,
            &out,
            &seeds,
            &interrupted,
            &mut report,
        )?;
        report.phase = "measuring".into();
        simulation_report::write(&out, &report)?;
        run(
            &config,
            &sample,
            selector,
            &out,
            &interrupted,
            &mut report,
            &seeds,
            false,
        )
    })();
    if let Err(error) = &result {
        report.errors.push(error.to_string());
    }
    report.finished_at = Some(now());
    report.interrupted = interrupted.load(Ordering::Relaxed);
    report.finalize();
    report.phase = if report.success {
        "complete"
    } else if report.phase == "preparing" {
        "preparation_failed"
    } else {
        "measurement_failed"
    }
    .into();
    simulation_report::write(&out, &report)?;
    result?;
    if !report.success {
        bail!(
            "scenario unsuccessful; see {}",
            out.join("report.html").display()
        );
    }
    Ok(())
}

fn preflight(
    config: &Config,
    out: &Path,
    filename: &str,
) -> Result<(transparent_filter::ShardMap, String, String)> {
    let file = Arc::new(std::sync::Mutex::new(File::create(out.join(filename))?));
    let observer: HttpObserver = Arc::new(move |o| {
        let _ = write_line(
            &mut file.lock().unwrap(),
            &json!({"at":now(),"stage":o.stage,"status":o.status,"seconds":o.elapsed.as_secs_f64(),"bytes_up":o.bytes_up,"bytes_down":o.bytes_down,"failed":o.failed}),
        );
    });
    let (map, _) = HttpFilterSource::new(config.filter_origin(), &config.options())
        .map_err(io_error)?
        .with_observer(observer.clone())
        .with_retry_attempts(3)
        .map()
        .map_err(io_error)?;
    let geometry = HttpShardTransport::new(&config.shard_url, &config.options())
        .map_err(io_error)?
        .with_observer(observer)
        .with_retry_attempts(3)
        .geometry()
        .map_err(io_error)?;
    let hash = digest(&serde_json::to_vec(&map)?);
    Ok((map, hash, geometry.schema))
}

/// Prepare only the wave's selected wallets, or the entire replacement pool for
/// sustained runs. Batch sizes never exceed the configured per-profile slots.
#[allow(clippy::too_many_arguments)]
fn prepare(
    config: &Config,
    sample: &Sample,
    selector: &Selector,
    out: &Path,
    seeds: &Path,
    interrupted: &AtomicBool,
    parent: &mut Report,
) -> Result<()> {
    let mut remaining: BTreeMap<String, Vec<usize>> = selector
        .pools
        .iter()
        .map(|(class, pool)| {
            let count = if config.mode == Mode::Wave {
                config.profiles[class]
            } else {
                pool.len()
            };
            (
                class.clone(),
                pool[..count]
                    .iter()
                    .copied()
                    .filter(|i| sample.clients[*i].required_from > sample.start_height)
                    .collect(),
            )
        })
        .collect();
    let mut batch = 0;
    while remaining.values().any(|p| !p.is_empty()) {
        if interrupted.load(Ordering::Relaxed) {
            bail!("wallet preparation interrupted");
        }
        let mut pools = BTreeMap::new();
        let mut capacity = config.preparation_concurrency;
        for (class, pool) in &mut remaining {
            let n = pool.len().min(config.profiles[class]).min(capacity);
            capacity -= n;
            if n > 0 {
                pools.insert(class.clone(), pool.drain(..n).collect::<Vec<_>>());
            }
        }
        let mut prep = config.clone();
        prep.name = format!("{}: wallet preparation batch {batch}", config.name);
        prep.mode = Mode::Wave;
        prep.recovery_deadline_seconds = config.preparation_deadline_seconds;
        prep.profiles = pools.iter().map(|(c, p)| (c.clone(), p.len())).collect();
        prep.max_queries = None;
        prep.max_p99_exact_seconds = None;
        let directory = out.join("preparation").join(format!("batch-{batch}"));
        fs::create_dir_all(directory.join("stores"))?;
        fs::create_dir(directory.join("logs"))?;
        let mut report = Report::new(&prep, digest(&fs::read(&config.sample)?));
        let link = format!("preparation/batch-{batch}/report.html");
        parent.preparation.push(json!({"batch":batch,"status":"running","report":link,"wallets":prep.profiles.values().sum::<usize>()}));
        simulation_report::write(&directory, &report)?;
        simulation_report::write(out, parent)?;
        eprintln!("{} (excluded from measured load)", prep.name);
        let result = run(
            &prep,
            sample,
            Selector {
                pools,
                cursors: BTreeMap::new(),
            },
            &directory,
            interrupted,
            &mut report,
            seeds,
            true,
        );
        if let Err(error) = &result {
            report.errors.push(error.to_string());
        }
        report.finished_at = Some(now());
        report.interrupted = interrupted.load(Ordering::Relaxed);
        report.finalize();
        report.phase = if report.success {
            "complete"
        } else {
            "preparation_failed"
        }
        .into();
        simulation_report::write(&directory, &report)?;
        parent.preparation[batch] = json!({"batch":batch,"status":if report.success {"complete"} else {"failed"},"report":link,
            "summary":report.summary,"errors":report.errors,"users":report.users});
        simulation_report::write(out, parent)?;
        result?;
        if !report.success {
            bail!(
                "wallet preparation unsuccessful; see {}",
                directory.join("report.html").display()
            );
        }
        batch += 1;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run(
    config: &Config,
    sample: &Sample,
    mut selector: Selector,
    out: &Path,
    interrupted: &AtomicBool,
    report: &mut Report,
    seeds: &Path,
    preparing: bool,
) -> Result<()> {
    let (map, map_digest, schema) = preflight(config, out, "preflight.ndjson")?;
    let tip = map.shards.last().context("empty shard map")?;
    if map.genesis_hash != sample.genesis_hash
        || map.start_height != sample.start_height
        || tip.end_height != sample.anchor_height
        || sample.anchor_hash.as_deref() != Some(&tip.terminal_block_hash)
    {
        bail!("served publication does not match the workload sample");
    }
    report.provenance["phase"] = json!(if preparing {
        "wallet_preparation"
    } else {
        "measured_recovery"
    });
    report.provenance["wallet_initialization"] = json!("Prior ledger events only; independent stores with cold filter/setup caches. Preparation can warm server caches.");
    report.publication = json!({"map_sha256":map_digest, "schema":schema,"genesis_hash":map.genesis_hash,"start_height":map.start_height,"anchor_height":tip.end_height,"anchor_hash":tip.terminal_block_hash,"cutoff_height":sample.cutoff_height,"shards":map.shards.len(),"geometries":map.seal.keys().collect::<Vec<_>>()});
    fs::write(out.join("map.json"), serde_json::to_vec_pretty(&map)?)?;
    let (tx, rx) = mpsc::channel();
    let mut requests = File::create(out.join("requests.ndjson"))?;
    let mut events = File::create(out.join("wallets.ndjson"))?;
    let mut metrics_file = File::create(out.join("metrics.ndjson"))?;
    let stop = Arc::new(AtomicBool::new(false));
    let scraper = scrape_thread(config.metrics_targets.clone(), tx.clone(), stop.clone());
    // Always stop the scrape threads, including when evidence writes fail.
    let execution = (|| -> Result<()> {
        let mut slots = Vec::new();
        for (class, n) in &config.profiles {
            for _ in 0..*n {
                let slot = slots.len();
                slots.push(Slot {
                    class: class.clone(),
                    process: Some(Process::spawn(slot, 0, tx.clone(), &out.join("logs"))?),
                    generation: 0,
                    ready: false,
                    spawned: Instant::now(),
                    active: None,
                    wave_started: false,
                });
            }
        }
        let mut begun: Option<Instant> = None;
        let mut last_checkpoint = Instant::now();
        let mut last_host = Instant::now() - Duration::from_secs(2);
        let mut system = sysinfo::System::new();
        let mut last_progress = Instant::now();
        loop {
            match rx.recv_timeout(Duration::from_millis(20)) {
                Ok(Message::Metrics(value)) => {
                    write_line(&mut metrics_file, &value)?;
                    report.metrics.push(value);
                }
                Ok(Message::Worker(slot, generation, value))
                    if generation == slots[slot].generation =>
                {
                    match value["type"].as_str() {
                        Some("ready") => slots[slot].ready = true,
                        Some("started") | Some("request") | Some("outcome") => {
                            if let Some((id, _, _)) = slots[slot].active {
                                if value["id"].as_u64() == Some(id as u64) {
                                    if value["type"] == "request" {
                                        write_line(&mut requests, &value)?;
                                        report.request(&value);
                                    } else {
                                        write_line(&mut events, &value)?;
                                        report.event(&value);
                                    }
                                    if value["type"] == "outcome" {
                                        slots[slot].active = None;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }
                Ok(Message::Gone(slot, generation, error))
                    if generation == slots[slot].generation =>
                {
                    if let Some((id, _, _)) = slots[slot].active.take() {
                        let event = json!({"type":"outcome", "id":id, "at":now(), "outcome":"failed", "error":error});
                        write_line(&mut events, &event)?;
                        report.event(&event);
                    } else if begun.is_none() {
                        bail!("worker {slot} failed before barrier: {error}");
                    }
                    slots[slot].process = None;
                    slots[slot].ready = false;
                }
                _ => {}
            }
            let cancelled = interrupted.load(Ordering::Relaxed);
            if begun.is_none()
                && slots.iter().all(|s| s.ready)
                && config
                    .metrics_targets
                    .keys()
                    .all(|name| report.metrics.iter().any(|m| m["target"] == *name))
            {
                report.started_at = Some(now());
                begun = Some(Instant::now());
            }
            let admit = !cancelled
                && begun.is_some_and(|start| {
                    config.mode == Mode::Wave
                        || start.elapsed() < Duration::from_secs(config.duration_seconds)
                });
            for slot in &mut slots {
                if let Some((id, _, start)) = slot.active {
                    if cancelled
                        || start.elapsed() >= Duration::from_secs(config.recovery_deadline_seconds)
                    {
                        slot.process = None;
                        slot.ready = false;
                        slot.active = None;
                        let error = if cancelled {
                            "Recovery cancelled by operator".to_owned()
                        } else {
                            format!("{} exceeded its {} second deadline; the worker was stopped before completion. See request stages for completed work.",
                                if preparing { "Full-history wallet preparation" } else { "Measured recovery" },
                                config.recovery_deadline_seconds)
                        };
                        let value = json!({"type":"outcome","id":id,"at":now(),"outcome":if cancelled {"cancelled"} else {"timed_out"},"error":error});
                        write_line(&mut events, &value)?;
                        report.event(&value);
                    }
                }
                if !slot.ready
                    && slot.process.is_some()
                    && slot.spawned.elapsed() > Duration::from_secs(30)
                {
                    bail!("worker readiness deadline exceeded");
                }
            }
            let mut active: BTreeSet<usize> = slots
                .iter()
                .filter_map(|s| s.active.map(|(_, i, _)| i))
                .collect();
            for (index, slot) in slots.iter_mut().enumerate() {
                if !admit
                    || slot.active.is_some()
                    || (config.mode == Mode::Wave && slot.wave_started)
                {
                    continue;
                }
                if slot.process.is_none() {
                    slot.generation += 1;
                    slot.spawned = Instant::now();
                    slot.process = Some(Process::spawn(
                        index,
                        slot.generation,
                        tx.clone(),
                        &out.join("logs"),
                    )?);
                    continue;
                }
                if !slot.ready {
                    continue;
                }
                // Recheck the clock at each admission, not only at the top of a loop.
                if config.mode == Mode::Sustained
                    && begun.unwrap().elapsed() >= Duration::from_secs(config.duration_seconds)
                {
                    break;
                }
                let sample_index = selector.next(&slot.class, &active);
                active.insert(sample_index);
                let id = report.users.len();
                let job = Job {
                    id,
                    config: config.clone(),
                    wallet: sample.clients[sample_index].clone(),
                    map_digest: map_digest.clone(),
                    store_path: out.join("stores").join(format!("wallet-{id}.sqlite")),
                    seed_path: seeds.join(format!("sample-{sample_index}.json")),
                    preparing,
                };
                let value = json!({"type":"scheduled","id":id,"at":now(),"slot":index,"sample_index":sample_index,"profile":slot.class,"expected_events":job.wallet.journal_events,"expected_digest":job.wallet.expected_digest,"required_from":job.wallet.required_from,"script_count":job.wallet.scripts.len()});
                write_line(&mut events, &value)?;
                report.event(&value);
                slot.active = Some((id, sample_index, Instant::now()));
                slot.wave_started = true;
                if let Err(error) = slot.process.as_mut().unwrap().send(&job) {
                    slot.process = None;
                    slot.ready = false;
                    slot.active = None;
                    let value = json!({"type":"outcome","id":id,"at":now(),"outcome":"failed","error":error.to_string()});
                    write_line(&mut events, &value)?;
                    report.event(&value);
                }
            }
            if last_host.elapsed() >= Duration::from_secs(1) {
                let mut pids = vec![sysinfo::get_current_pid().map_err(|e| anyhow::anyhow!(e))?];
                pids.extend(slots.iter().filter_map(|s| {
                    s.process
                        .as_ref()
                        .map(|p| sysinfo::Pid::from_u32(p.child.id()))
                }));
                system.refresh_processes(sysinfo::ProcessesToUpdate::Some(&pids), true);
                let processes: Vec<Value> = pids.iter().filter_map(|pid| system.process(*pid).map(|p| json!({"pid":pid.as_u32(),"rss_bytes":p.memory(),"cpu_seconds":p.accumulated_cpu_time() as f64 / 1000.0,"start_time":p.start_time()}))).collect();
                let value = json!({"at":now(),"target":"load-client","processes":processes,"active_users":slots.iter().filter(|s| s.active.is_some()).count()});
                write_line(&mut metrics_file, &value)?;
                report.metrics.push(value);
                last_host = Instant::now();
            }
            if last_checkpoint.elapsed() >= Duration::from_secs(5) {
                simulation_report::write(out, report)?;
                last_checkpoint = Instant::now();
            }
            if last_progress.elapsed() >= Duration::from_secs(10) {
                eprintln!(
                    "{}: {} started, {} active, {} exact",
                    config.name,
                    report.users.len(),
                    slots.iter().filter(|s| s.active.is_some()).count(),
                    report
                        .users
                        .iter()
                        .filter(|u| u["outcome"] == "exact")
                        .count()
                );
                last_progress = Instant::now();
            }
            let all_idle = slots.iter().all(|s| s.active.is_none());
            if (cancelled
                || (begun.is_some() && !admit)
                || (config.mode == Mode::Wave && slots.iter().all(|s| s.wave_started)))
                && all_idle
            {
                break;
            }
        }
        report.execution_finished_at = Some(now());
        // All Process values are killed and reaped when slots leaves scope.
        Ok(())
    })();
    report.execution_finished_at.get_or_insert_with(now);
    stop.store(true, Ordering::Relaxed);
    let _ = scraper.join();
    for message in rx.try_iter() {
        if let Message::Metrics(value) = message {
            write_line(&mut metrics_file, &value)?;
            report.metrics.push(value);
        }
    }
    // A supervisor failure still gives every started recovery a terminal state.
    let unfinished: Vec<_> = report
        .users
        .iter()
        .filter(|u| u["outcome"] == "running")
        .map(|u| u["id"].clone())
        .collect();
    for id in unfinished {
        let value = json!({"type":"outcome","id":id,"at":now(),"outcome":"cancelled","error":"supervisor stopped"});
        let _ = write_line(&mut events, &value);
        report.event(&value);
    }
    execution?;
    if !interrupted.load(Ordering::Relaxed) {
        match preflight(config, out, "postflight.ndjson") {
            Ok((_, final_digest, final_schema)) => {
                report.publication_stable =
                    Some(final_digest == map_digest && final_schema == schema);
            }
            Err(error) => {
                report
                    .errors
                    .push(format!("publication postflight: {error}"));
                report.publication_stable = Some(false);
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    fn config() -> Config {
        serde_json::from_value(json!({"schema":"transparent-scenario-v1","name":"test","mode":"wave","sample":"sample.json","shard_url":"http://localhost:1","profiles":{"small-active":2}})).unwrap()
    }
    #[test]
    fn selection_is_reproducible_and_excludes_active_wallets() {
        let config = config();
        let clients: Vec<_> = (0..5)
            .map(|i| SampleClient {
                class: "small-active".into(),
                scripts: vec![format!("{i:02x}")],
                required_from: 0,
                expected_digest: String::new(),
                journal_events: 0,
            })
            .collect();
        let mut a = Selector::new(&config, &clients).unwrap();
        let mut b = Selector::new(&config, &clients).unwrap();
        let mut active = BTreeSet::new();
        for _ in 0..5 {
            let i = a.next("small-active", &active);
            assert_eq!(i, b.next("small-active", &active));
            assert!(active.insert(i));
        }
        active.remove(&2);
        assert_eq!(a.next("small-active", &active), 2);
        assert!(Selector::new(&config, &clients[..1]).is_err());
        assert!(Selector::new(&config, &[clients[0].clone(), clients[0].clone()]).is_err());
    }
    #[test]
    fn invalid_config_is_rejected_before_load() {
        let mut c = config();
        assert!(c.validate().is_ok());
        c.profiles.insert("small-active".into(), 0);
        assert!(c.validate().is_err());
        let mut c = config();
        c.shard_url = "http://user:password@localhost".into();
        assert!(c.validate().is_err());
        let mut c = config();
        c.preparation_deadline_seconds = 0;
        assert!(c.validate().is_err());
        let mut c = config();
        c.recovery_deadline_seconds = 0;
        assert!(c.validate().is_err());
    }
}
