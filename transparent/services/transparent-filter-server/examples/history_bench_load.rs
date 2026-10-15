//! Open-loop query traffic for the prepared history runtimes benchmark.
//!
//! Queries are scheduled at a fixed rate from a seeded schedule, whatever the
//! worker's answers take, so a slow worker meets the same offered load as a
//! fast one; a query that cannot be started within a second of its slot is
//! counted as unstarted instead of being sent late. The schedule cycles
//! through four classes: a sealed shard's directory, a sealed shard's page
//! shard, the open tail's directory and the tail's newest page shard, all in
//! the recent tier the worker serves.
//!
//! Every answer is decoded with the client's secret and compared with the
//! row read straight from the published segment file, from the publisher's
//! directories: the oracle never touches a runtime. A revision the worker no
//! longer holds (409) is retried once against a fresh map, as a wallet does.
//!
//! One JSON object per query goes to `--out`; a summary is printed at the end.

use clap::Parser;
use std::collections::HashMap;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, RwLock};
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use transparent_filter::ShardMap;
use transparent_shard::manifest::ShardManifest;
use transparent_shard_server::runtime::SharedParams;
use transparent_shard_server::shardset::{Table, TableRef};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
struct Cli {
    /// The worker's base URL.
    #[arg(long)]
    worker: String,
    /// Directories whose subdirectories hold published revisions by digest:
    /// the publisher's root and initial publication.
    #[arg(long, required = true)]
    oracle: Vec<PathBuf>,
    /// Geometry of the tier queried.
    #[arg(long, default_value = "decoupled-2k-8k")]
    geometry: String,
    #[arg(long, default_value_t = 20)]
    rate: u32,
    #[arg(long)]
    duration_seconds: u64,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    #[arg(long)]
    out: PathBuf,
}

const CLASSES: [&str; 4] = [
    "sealed-directory",
    "sealed-page",
    "recent-directory",
    "recent-page",
];

fn splitmix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e37_79b9_7f4a_7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    x ^ (x >> 31)
}

fn unix() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs_f64()
}

/// The current map and the manifests of its shards, by digest.
struct View {
    map: ShardMap,
    manifests: HashMap<String, Arc<ShardManifest>>,
}

/// A segment's setup: revision digest, table name and segment.
type SetupKey = (String, String, u32);

struct Load {
    cli: Cli,
    http: reqwest::Client,
    view: RwLock<Option<Arc<View>>>,
    manifests: Mutex<HashMap<String, Arc<ShardManifest>>>,
    setups: Mutex<HashMap<SetupKey, Arc<Vec<u8>>>>,
    params: HashMap<Table, Arc<SharedParams>>,
    oracle_dirs: Mutex<HashMap<String, PathBuf>>,
}

/// One query's target.
struct Target {
    shard_id: u64,
    digest: String,
    table: TableRef,
    segments: u32,
    rows: usize,
}

impl Load {
    async fn manifest(&self, shard_id: u64, digest: &str) -> Result<Arc<ShardManifest>, BoxError> {
        if let Some(manifest) = self.manifests.lock().unwrap().get(digest) {
            return Ok(manifest.clone());
        }
        let raw = self
            .http
            .get(format!(
                "{}/v2/shards/{shard_id}/revisions/{digest}/manifest",
                self.cli.worker
            ))
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let manifest: Arc<ShardManifest> = Arc::new(serde_json::from_slice(&raw)?);
        if manifest.digest() != digest {
            return Err("manifest does not digest to the map".into());
        }
        self.manifests
            .lock()
            .unwrap()
            .insert(digest.to_string(), manifest.clone());
        Ok(manifest)
    }

    async fn refresh(&self) -> Result<Arc<View>, BoxError> {
        let raw = self
            .http
            .get(format!("{}/v2/shards", self.cli.worker))
            .send()
            .await?
            .error_for_status()?
            .bytes()
            .await?;
        let map: ShardMap = serde_json::from_slice(&raw)?;
        let mut manifests = HashMap::new();
        for entry in map
            .shards
            .iter()
            .filter(|e| e.geometry == self.cli.geometry)
        {
            manifests.insert(
                entry.manifest_digest.clone(),
                self.manifest(entry.shard_id, &entry.manifest_digest)
                    .await?,
            );
        }
        let view = Arc::new(View { map, manifests });
        *self.view.write().unwrap() = Some(view.clone());
        Ok(view)
    }

    fn target(&self, view: &View, class: usize, selector: u64) -> Option<Target> {
        let recent: Vec<_> = view
            .map
            .shards
            .iter()
            .filter(|e| e.geometry == self.cli.geometry)
            .collect();
        let tail = recent.last().filter(|e| !e.sealed)?;
        let sealed: Vec<_> = recent.iter().filter(|e| e.sealed).collect();
        let entry = match class {
            0 | 1 => *sealed.get((selector % sealed.len().max(1) as u64) as usize)?,
            _ => *tail,
        };
        let manifest = view.manifests.get(&entry.manifest_digest)?;
        let directory_rows = self.params[&Table::Directory].profile.rows;
        let (table, segments, rows) = match class {
            0 | 2 => (
                TableRef::Directory,
                manifest.directory_segments.len() as u32,
                directory_rows,
            ),
            _ => {
                let children: Vec<_> = manifest
                    .page_children
                    .iter()
                    .filter(|c| !c.page_segments.is_empty())
                    .collect();
                // The tail's newest page shard is the one that changes; a
                // sealed shard's are chosen among all.
                let child = if class == 3 {
                    *children.last()?
                } else {
                    *children.get(((selector >> 32) % children.len().max(1) as u64) as usize)?
                };
                (
                    TableRef::Pages(child.index),
                    child.page_segments.len() as u32,
                    child.query_rows() as usize,
                )
            }
        };
        Some(Target {
            shard_id: entry.shard_id,
            digest: entry.manifest_digest.clone(),
            table,
            segments,
            rows,
        })
    }

    fn oracle_row(
        &self,
        target: &Target,
        segment: u32,
        row: usize,
        row_bytes: usize,
    ) -> Result<Vec<u8>, BoxError> {
        let dir = {
            let mut cached = self.oracle_dirs.lock().unwrap();
            match cached.get(&target.digest) {
                Some(dir) => dir.clone(),
                None => {
                    let mut found = None;
                    'search: for root in &self.cli.oracle {
                        if root.join(&target.digest).is_dir() {
                            found = Some(root.join(&target.digest));
                            break;
                        }
                        for entry in std::fs::read_dir(root)?.filter_map(Result::ok) {
                            let candidate = entry.path().join(&target.digest);
                            if candidate.join("manifest.json").is_file() {
                                found = Some(candidate);
                                break 'search;
                            }
                        }
                    }
                    let dir = found.ok_or("no oracle holds the revision")?;
                    cached.insert(target.digest.clone(), dir.clone());
                    dir
                }
            }
        };
        let mut file =
            std::fs::File::open(dir.join(format!("{}.{segment}.bin", target.table.name())))?;
        file.seek(SeekFrom::Start((row * row_bytes) as u64))?;
        let mut bytes = vec![0u8; row_bytes];
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }

    async fn setup(&self, target: &Target, segment: u32) -> Result<Arc<Vec<u8>>, BoxError> {
        let key = (target.digest.clone(), target.table.name(), segment);
        if let Some(public) = self.setups.lock().unwrap().get(&key) {
            return Ok(public.clone());
        }
        let setup: serde_json::Value = self
            .http
            .get(format!(
                "{}/v2/shards/{}/revisions/{}/setup/{}/{segment}",
                self.cli.worker,
                target.shard_id,
                target.digest,
                target.table.name()
            ))
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        let public = Arc::new(base64(
            setup["public_params"]
                .as_str()
                .ok_or("setup without masks")?,
        )?);
        self.setups.lock().unwrap().insert(key, public.clone());
        Ok(public)
    }

    /// Sends one query and checks its answer; the record's `outcome` says
    /// how it ended.
    async fn query(
        &self,
        class: usize,
        selector: u64,
        record: &mut serde_json::Value,
    ) -> Result<(), BoxError> {
        for attempt in 0..2 {
            let cached = self.view.read().unwrap().clone();
            let view = match (attempt, cached) {
                (0, Some(view)) => view,
                _ => self.refresh().await?,
            };
            let target = self
                .target(&view, class, selector)
                .ok_or("the map has no target of this class")?;
            let shared = self.params[&target.table.kind()].clone();
            let profile = &shared.profile;
            let row = ((selector >> 8) % target.rows as u64) as usize;
            let mut publics = Vec::new();
            for segment in 0..target.segments {
                publics.push(self.setup(&target, segment).await?);
            }
            let (secret, upload) = profile.prepare(row)?;
            let mut body =
                transparent_shard::manifest::query_binding(&target.digest, &target.table.name())
                    .to_vec();
            body.extend(upload);
            record["shard_id"] = target.shard_id.into();
            record["digest"] = target.digest.clone().into();
            record["table"] = target.table.name().into();
            record["row"] = row.into();
            record["segments"] = target.segments.into();
            let sent = unix();
            record["sent_unix"] = sent.into();
            let response = self
                .http
                .post(format!(
                    "{}/v2/shards/{}/revisions/{}/query/{}",
                    self.cli.worker,
                    target.shard_id,
                    target.digest,
                    target.table.name()
                ))
                .body(body)
                .send()
                .await?;
            let status = response.status();
            let answer = response.bytes().await?;
            let finished = unix();
            record["latency_ms"] = ((finished - sent) * 1e3).into();
            record["status"] = status.as_u16().into();
            if status.as_u16() == 409 && attempt == 0 {
                record["stale_retries"] = 1.into();
                continue;
            }
            if !status.is_success() {
                record["outcome"] = "refused".into();
                return Ok(());
            }
            let each = shared.response_bytes();
            if answer.len() != each * target.segments as usize {
                record["outcome"] = "malformed".into();
                return Ok(());
            }
            let mut exact = true;
            let mut populated = false;
            for (segment, chunk) in answer.chunks(each).enumerate() {
                let decoded = profile.decode(&secret, &publics[segment], &chunk[16..])?;
                let expected = self.oracle_row(&target, segment as u32, row, profile.row_bytes)?;
                populated |= expected.iter().any(|b| *b != 0);
                exact &= decoded == expected;
            }
            record["populated"] = populated.into();
            record["outcome"] = if exact { "exact" } else { "inexact" }.into();
            return Ok(());
        }
        record["outcome"] = "stale".into();
        Ok(())
    }
}

/// Standard padded base64, as the setup route encodes the masks.
fn base64(text: &str) -> Result<Vec<u8>, BoxError> {
    let value = |c: u8| -> Result<u32, BoxError> {
        Ok(match c {
            b'A'..=b'Z' => c - b'A',
            b'a'..=b'z' => c - b'a' + 26,
            b'0'..=b'9' => c - b'0' + 52,
            b'+' => 62,
            b'/' => 63,
            _ => return Err("invalid base64".into()),
        } as u32)
    };
    let bytes = text.trim_end_matches('=').as_bytes();
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        let mut word = 0u32;
        for (i, c) in chunk.iter().enumerate() {
            word |= value(*c)? << (18 - 6 * i);
        }
        out.extend_from_slice(&word.to_be_bytes()[1..chunk.len()]);
    }
    Ok(out)
}

fn quantile(sorted: &[f64], q: f64) -> Option<f64> {
    if sorted.is_empty() {
        return None;
    }
    let rank = ((q * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
    Some(sorted[rank - 1])
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let geometry = transparent_shard::layout::by_name(&cli.geometry).ok_or("unknown geometry")?;
    let mut params = HashMap::new();
    for table in Table::HISTORY {
        params.insert(table, Arc::new(SharedParams::build(geometry, table)?));
    }
    let total = u64::from(cli.rate) * cli.duration_seconds;
    let interval = Duration::from_secs_f64(1.0 / f64::from(cli.rate));
    let out = Arc::new(Mutex::new(std::io::BufWriter::new(std::fs::File::create(
        &cli.out,
    )?)));
    let seed = cli.seed;
    let load = Arc::new(Load {
        cli,
        http: reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .pool_max_idle_per_host(64)
            .build()?,
        view: RwLock::new(None),
        manifests: Mutex::new(HashMap::new()),
        setups: Mutex::new(HashMap::new()),
        params,
        oracle_dirs: Mutex::new(HashMap::new()),
    });
    load.refresh().await?;
    // The map is refreshed four times a second, as a wallet polling the
    // public map would see each publication soon after it activates.
    let refresher = {
        let load = load.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_millis(250)).await;
                if let Err(error) = load.refresh().await {
                    eprintln!("map refresh failed: {error}");
                }
            }
        })
    };
    let started_unix = unix();
    let start = tokio::time::Instant::now();
    let mut tasks = tokio::task::JoinSet::new();
    let mut unstarted = 0u64;
    for i in 0..total {
        let scheduled = start + interval * i as u32;
        tokio::time::sleep_until(scheduled).await;
        let lag = tokio::time::Instant::now() - scheduled;
        let class = (i % 4) as usize;
        let selector = splitmix(seed ^ splitmix(i));
        let mut record = serde_json::json!({
            "index": i, "class": CLASSES[class],
            "scheduled_unix": started_unix + (interval * i as u32).as_secs_f64(),
            "start_lag_ms": lag.as_secs_f64() * 1e3,
        });
        if lag > Duration::from_secs(1) {
            unstarted += 1;
            record["outcome"] = "unstarted".into();
            writeln!(out.lock().unwrap(), "{record}")?;
            continue;
        }
        let (load, out) = (load.clone(), out.clone());
        tasks.spawn(async move {
            if let Err(error) = load.query(class, selector, &mut record).await {
                record["outcome"] = "error".into();
                record["error"] = error.to_string().into();
            }
            let _ = writeln!(out.lock().unwrap(), "{record}");
            record
        });
        // Reap finished queries without waiting for any.
        while let Some(Some(_)) = tasks.try_join_next().map(|r| r.ok()) {}
    }
    while tasks.join_next().await.is_some() {}
    refresher.abort();
    out.lock().unwrap().flush()?;
    drop(out);
    // The summary, from what was written.
    let raw = std::fs::read_to_string(&load.cli.out)?;
    let records: Vec<serde_json::Value> = raw
        .lines()
        .map(serde_json::from_str)
        .collect::<Result<_, _>>()?;
    let mut outcomes: HashMap<String, u64> = HashMap::new();
    let mut by_class: HashMap<String, Vec<f64>> = HashMap::new();
    let mut all = Vec::new();
    for record in &records {
        let outcome = record["outcome"].as_str().unwrap_or("missing").to_string();
        *outcomes.entry(outcome.clone()).or_default() += 1;
        if outcome == "exact" {
            let latency = record["latency_ms"].as_f64().unwrap_or(f64::NAN);
            by_class
                .entry(record["class"].as_str().unwrap_or("").into())
                .or_default()
                .push(latency);
            all.push(latency);
        }
    }
    let summarize = |latencies: &mut Vec<f64>| {
        latencies.sort_by(|a, b| a.total_cmp(b));
        serde_json::json!({"n": latencies.len(), "p50_ms": quantile(latencies, 0.5),
            "p95_ms": quantile(latencies, 0.95), "p99_ms": quantile(latencies, 0.99),
            "max_ms": latencies.last()})
    };
    let summary = serde_json::json!({
        "scheduled": total,
        "unstarted": unstarted,
        "outcomes": outcomes,
        "latency": summarize(&mut all),
        "by_class": by_class.iter_mut().map(|(k, v)| (k.clone(), summarize(v))).collect::<serde_json::Map<_, _>>(),
    });
    println!("{summary}");
    Ok(())
}
