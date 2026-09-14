//! Sequential deployed correctness checks. Each case runs in a deadline-bounded child.
mod counting;
mod http_evidence;
use anyhow::{bail, Context, Result};
use clap::Parser;
use counting::{addressed_shards, Counting, CountingFilters};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    path::{Path, PathBuf},
    process::{Command, Stdio},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use transparent_regression::{actual, validate, Case, Fixture};
use transparent_wallet::{
    http::{HttpFilterSource, HttpOptions, HttpShardTransport},
    store::{ScriptEntry, ScriptOrigin},
    sync_into,
    transport::{FilterSource, ShardTransport},
    Completion, SetIdentity, StaticChain, StaticScripts, WalletStore, WorkLimits,
};
use transparent_wallet_store::SqliteStore;

#[derive(Parser)]
struct Args {
    #[arg(long)]
    fixture: PathBuf,
    #[arg(long)]
    shard_url: String,
    #[arg(long)]
    filter_url: String,
    #[arg(long)]
    out_dir: PathBuf,
    #[arg(long, default_value_t = 60)]
    request_timeout_secs: u64,
    #[arg(long, default_value_t = 1800)]
    case_timeout_secs: u64,
    /// Attempts share request_timeout_secs; every attempt is recorded.
    #[arg(long, default_value_t = 3, value_parser = clap::value_parser!(u8).range(1..=3))]
    http_attempts: u8,
    #[arg(long)]
    source_sha: String,
    #[arg(long, hide = true)]
    worker_case: Option<String>,
}
fn options(a: &Args) -> HttpOptions {
    HttpOptions {
        timeout: Duration::from_secs(a.request_timeout_secs),
        user_agent: "transparent-regression".into(),
    }
}
/// What a fixture can pin in a served map, and what the tail may have done.
///
/// A live publication always ends in one unsealed shard republished on a block
/// cadence, so a whole-map digest pins a publication that stopped existing a
/// block after the export. What a fixture *can* pin is the set identity and
/// every sealed entry byte for byte: sealed contents are immutable, so those
/// carry the guarantee. The tail may only have grown, keeping its shard id,
/// geometry, start height and parent hash; a shrunk, re-parented or renumbered
/// tail is still drift and still fails.
fn agrees(
    pinned: &transparent_filter::ShardMap,
    served: &transparent_filter::ShardMap,
) -> Result<()> {
    served
        .check_shape()
        .map_err(anyhow::Error::msg)
        .context("served map is malformed")?;
    if SetIdentity::of(pinned) != SetIdentity::of(served) {
        bail!("served map is a different set lineage than the fixture pins");
    }
    for (index, want) in pinned.shards.iter().enumerate() {
        let Some(got) = served.shards.get(index) else {
            bail!(
                "served map holds {} shards; the fixture pins {}",
                served.shards.len(),
                pinned.shards.len()
            );
        };
        if want.sealed {
            if got != want {
                bail!(
                    "sealed shard {} changed: pinned revision {} {}, served revision {} {}",
                    want.shard_id,
                    want.revision,
                    want.manifest_digest,
                    got.revision,
                    got.manifest_digest
                );
            }
            continue;
        }
        if got.shard_id != want.shard_id
            || got.geometry != want.geometry
            || got.start_height != want.start_height
            || got.parent_block_hash != want.parent_block_hash
        {
            bail!(
                "shard {} is not a continuation of the pinned tail",
                want.shard_id
            );
        }
        if got.end_height < want.end_height {
            bail!(
                "tail shard {} ends at {}, below the pinned {}",
                want.shard_id,
                got.end_height,
                want.end_height
            );
        }
    }
    Ok(())
}

/// The tail as one run has been watching it.
///
/// Relaxing the pin to the sealed prefix hands the run itself the part the
/// digest used to hold: across a run the tail stays on the same parent and
/// never goes backwards, and a seal may only append the next shard. A reorg
/// that rewrote the tail has to move one of these, contradict the fixture's
/// accepted headers, or produce a different ledger.
#[derive(Clone, PartialEq, Eq)]
struct TailWatch {
    shard_id: u64,
    start_height: u64,
    parent_block_hash: String,
    end_height: u64,
}

impl TailWatch {
    fn of(map: &transparent_filter::ShardMap) -> Result<Self> {
        let tail = map.shards.last().context("served map has no shards")?;
        Ok(Self {
            shard_id: tail.shard_id,
            start_height: tail.start_height,
            parent_block_hash: tail.parent_block_hash.clone(),
            end_height: tail.end_height,
        })
    }
    /// Accepts a later observation of the same growing tail, or the next shard
    /// after it sealed. Refuses anything else.
    fn advance(&mut self, next: &Self) -> Result<()> {
        if next.shard_id < self.shard_id || next.end_height < self.end_height {
            bail!(
                "the tail went backwards: shard {} through {} after shard {} through {}",
                next.shard_id,
                next.end_height,
                self.shard_id,
                self.end_height
            );
        }
        if next.shard_id == self.shard_id
            && (next.start_height != self.start_height
                || next.parent_block_hash != self.parent_block_hash)
        {
            bail!(
                "tail shard {} was republished on another parent",
                self.shard_id
            );
        }
        if next.shard_id > self.shard_id && next.start_height <= self.start_height {
            bail!(
                "shard {} replaced shard {} without starting after it",
                next.shard_id,
                self.shard_id
            );
        }
        *self = next.clone();
        Ok(())
    }
}

/// One origin's answer to the map route.
struct Origin {
    url: String,
    map: transparent_filter::ShardMap,
    bytes: u64,
    digest: String,
}

/// SHA-256 over the part of a fixture's map the service must still serve
/// unchanged: the set identity and every sealed entry.
///
/// Recorded as evidence rather than checked as a whole, because the check has
/// to name *which* entry moved when one does.
fn sealed_prefix_digest(map: &transparent_filter::ShardMap) -> (usize, String) {
    let sealed: Vec<_> = map.shards.iter().filter(|s| s.sealed).collect();
    let body = json!({"identity": SetIdentity::of(map), "sealed": sealed});
    (
        sealed.len(),
        hex::encode(Sha256::digest(
            serde_json::to_vec(&body).expect("a sealed prefix serializes"),
        )),
    )
}

/// Checks both public origins still serve the publication the fixture pins, and
/// returns the map to sync against.
///
/// The wallet is handed the *served* map, not the fixture's: the fixture's tail
/// revision was withdrawn thousands of republications ago and only the sealed
/// entries it pins are still fetchable. The fixture's job is to say what must
/// not have changed and what the ledger must come out as.
fn pinned(
    a: &Args,
    f: &Fixture,
    watch: &mut Option<TailWatch>,
) -> Result<(transparent_filter::ShardMap, u64, Value)> {
    let mut seen: Vec<Origin> = vec![];
    for url in [&a.filter_url, &a.shard_url] {
        let mut origin = HttpFilterSource::new(url, &options(a))
            .map_err(anyhow::Error::msg)?
            .with_retry_attempts(usize::from(a.http_attempts))
            .with_observer(http_evidence::observer(a)?);
        let (raw, bytes) = origin.shard_map().map_err(anyhow::Error::msg)?;
        let map: transparent_filter::ShardMap = serde_json::from_slice(&raw)?;
        agrees(&f.map, &map).with_context(|| format!("publication drift at {url}"))?;
        seen.push(Origin {
            url: url.clone(),
            digest: hex::encode(Sha256::digest(&raw)),
            map,
            bytes,
        });
    }
    let (filters, shards) = (&seen[0], &seen[1]);
    let (front, back) = (TailWatch::of(&filters.map)?, TailWatch::of(&shards.map)?);
    // Both origins satisfied the same pin, so their sealed prefixes are already
    // identical. Their tails may be one block apart, because each fetch is its
    // own moment; which tail they are serving may not differ.
    if front.shard_id != back.shard_id
        || front.start_height != back.start_height
        || front.parent_block_hash != back.parent_block_hash
    {
        bail!(
            "{} and {} serve different tail shards",
            filters.url,
            shards.url
        );
    }
    // Same tail revision and still different maps would be a disagreement the
    // block cadence cannot explain.
    if filters.map.shards.last() == shards.map.shards.last() && filters.map != shards.map {
        bail!(
            "{} and {} serve the same tail revision but different maps",
            filters.url,
            shards.url
        );
    }
    match watch {
        Some(held) => held.advance(&front)?,
        empty => *empty = Some(front.clone()),
    }
    let tail = filters.map.shards.last().expect("check_shape found shards");
    let sample = json!({
        "filter_url": filters.url,
        "shard_url": shards.url,
        "filter_map_sha256": filters.digest,
        "shard_map_sha256": shards.digest,
        "tail": {
            "shard_id": tail.shard_id,
            "revision": tail.revision,
            "sealed": tail.sealed,
            "end_height": tail.end_height,
            "manifest_digest": tail.manifest_digest,
        },
        "origin_tail_skew_blocks": back.end_height as i64 - front.end_height as i64,
        "shards": filters.map.shards.len(),
    });
    Ok((filters.map.clone(), filters.bytes, sample))
}
fn run_case(a: &Args, f: &Fixture, case: &Case, stages: &mut Vec<Value>) -> Result<()> {
    let mut watch = None;
    let chain = StaticChain {
        hashes: f.accepted_headers.clone(),
    };
    let entries = case
        .scripts
        .iter()
        .map(|script| {
            Ok(ScriptEntry {
                script: hex::decode(script)?,
                origin: ScriptOrigin::Derived,
                required_from: case.required_from,
            })
        })
        .collect::<Result<Vec<_>>>()?;
    let folder = a.out_dir.join(&case.id);
    std::fs::create_dir_all(&folder)?;
    // A worker directory is created once; never overwrite an earlier wallet.
    let db = folder.join("wallet.sqlite");
    if db.exists() {
        bail!("refusing to reuse a previous run's store");
    }
    for (phase, cp) in case
        .checkpoints
        .iter()
        .map(|c| ("continue", c))
        .chain(std::iter::once((
            "repeat",
            case.checkpoints.last().unwrap(),
        )))
        .chain(std::iter::once((
            "fresh-final",
            case.checkpoints.last().unwrap(),
        )))
    {
        eprintln!("{}: {phase} through {}", case.id, cp.anchor.height);
        let path = if phase == "fresh-final" {
            folder.join("fresh.sqlite")
        } else {
            db.clone()
        };
        // Pinned once per sync rather than once per case: every sync must start
        // from a map the fixture still vouches for, and the tail it started
        // from is what decides whether a repeat may re-derive.
        let (served, map_bytes, publication) = pinned(a, f, &mut watch)?;
        let settled = served
            .shard_for_height(cp.anchor.height)
            .is_some_and(|shard| shard.sealed);
        let started = Instant::now();
        let counts = Arc::new(Mutex::new(BTreeMap::new()));
        let mut transport = Counting {
            inner: HttpShardTransport::new(&a.shard_url, &options(a))
                .map_err(anyhow::Error::msg)?
                .with_retry_attempts(usize::from(a.http_attempts))
                .with_observer(http_evidence::observer(a)?),
            stages: counts.clone(),
        };
        let mut filters = CountingFilters {
            inner: HttpFilterSource::new(&a.filter_url, &options(a))
                .map_err(anyhow::Error::msg)?
                .with_retry_attempts(usize::from(a.http_attempts))
                .with_observer(http_evidence::observer(a)?),
            stages: counts.clone(),
        };
        let (init, _) = transport.init().map_err(anyhow::Error::msg)?;
        let geometry = transparent_wallet::parse_init(&init).map_err(anyhow::Error::msg)?;
        let mut store = SqliteStore::open(&path)?;
        let mut scripts = StaticScripts(entries.clone());
        let result = sync_into(
            &mut store,
            &served,
            map_bytes,
            &geometry,
            &chain,
            &mut scripts,
            &mut filters,
            &mut transport,
            &WorkLimits::UNLIMITED,
            &cp.anchor,
        );
        let report = result.as_ref().ok();
        stages.push(json!({"phase": phase, "anchor": cp.anchor, "seconds": started.elapsed().as_secs_f64(), "requests": &*counts.lock().unwrap(), "completion": report.map(|r| format!("{:?}",r.completion)), "error": result.as_ref().err().map(ToString::to_string), "publication": publication, "anchor_coverage": if settled { "settled" } else { "provisional" }}));
        let report = result?;
        if report.completion != Completion::Complete {
            bail!(
                "incomplete at {}: {:?}",
                cp.anchor.height,
                report.completion
            );
        }
        if phase == "repeat" {
            if settled {
                if report.charges.queries() != 0 {
                    bail!("unchanged complete checkpoint issued private queries");
                }
            } else {
                // The anchor lies in the unsealed tail, whose revision the
                // service replaces on a block cadence, and the contract has the
                // wallet re-derive provisional coverage rather than keep it.
                // Requiring no query here would be requiring the tail to stand
                // still. What must hold is that re-derivation reaches the same
                // ledger and reads nothing but the tail: settled coverage below
                // it is immutable and must not be paid for twice.
                let tail = served.shards.last().expect("check_shape found shards");
                let strayed: Vec<u64> = addressed_shards(&counts.lock().unwrap())
                    .into_iter()
                    .filter(|id| *id != tail.shard_id)
                    .collect();
                if !strayed.is_empty() {
                    bail!("repeat at a provisional anchor re-read settled shards {strayed:?}");
                }
            }
        }
        drop(store);
        let reopened = SqliteStore::open(&path)?;
        let snapshot = actual(&reopened)?;
        if snapshot != cp.expected {
            let diff = folder.join(format!("{phase}-{}-difference.json", cp.anchor.height));
            std::fs::write(
                &diff,
                serde_json::to_vec_pretty(&json!({"expected":cp.expected,"actual":snapshot}))?,
            )?;
            bail!(
                "ledger mismatch at {} (see {})",
                cp.anchor.height,
                diff.display()
            );
        }
        if reopened.anchor()?.as_ref() != Some(&cp.anchor)
            || !reopened.pending()?.is_empty()
            || report.covered_through != cp.anchor.height
        {
            bail!("incorrect anchor or incomplete coverage");
        }
        for script in &entries {
            let ranges = reopened.coverage(&script.script)?;
            if !transparent_wallet::store::uncovered(
                &ranges,
                script.required_from,
                cp.anchor.height,
            )
            .is_empty()
                || ranges.iter().any(|r| r.end_height > cp.anchor.height)
            {
                bail!("per-script coverage mismatch");
            }
            if !ranges.iter().any(|r| {
                r.end_height == cp.anchor.height && r.terminal_block_hash == cp.anchor.hash
            }) {
                bail!("coverage not tied to requested hash");
            }
        }
    }
    pinned(a, f, &mut watch)?;
    Ok(())
}
fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&apos;")
}
fn main() -> Result<()> {
    let a = Args::parse();
    let raw = std::fs::read(&a.fixture)?;
    let f: Fixture = serde_json::from_slice(&raw)?;
    validate(&f)?;
    if a.request_timeout_secs == 0 || a.case_timeout_secs == 0 {
        bail!("deadlines must be positive");
    }
    if let Some(id) = &a.worker_case {
        let case = f
            .cases
            .iter()
            .find(|c| &c.id == id)
            .context("unknown worker case")?;
        let mut stages = vec![];
        let result = run_case(&a, &f, case, &mut stages);
        std::fs::write(
            a.out_dir.join(format!("{id}.json")),
            serde_json::to_vec_pretty(
                &json!({"id":id,"profile":case.profile,"passed":result.is_ok(),"error":result.as_ref().err().map(ToString::to_string),"checkpoints":stages,"http":http_evidence::summary(&a)?}),
            )?,
        )?;
        return result;
    }
    if a.out_dir.exists() {
        bail!("out-dir must be new; preserve earlier evidence");
    }
    std::fs::create_dir_all(&a.out_dir)?;
    let mut results = vec![];
    let mut watch = None;
    let preflight = pinned(&a, &f, &mut watch);
    for case in &f.cases {
        eprintln!("checking {} ({})", case.id, case.profile);
        let result = if let Err(e) = &preflight {
            json!({"id":case.id,"passed":false,"error":format!("preflight unavailable or drift: {e}")})
        } else {
            child(&a, &case.id).unwrap_or_else(|e| json!({"id":case.id,"passed":false,"error":format!("worker could not execute: {e}")}))
        };
        results.push(result);
    }
    let passed = results.iter().all(|r| r["passed"] == true);
    let runner_binary_sha256 =
        hex::encode(Sha256::digest(std::fs::read(std::env::current_exe()?)?));
    let (sealed_prefix_shards, sealed_prefix_sha256) = sealed_prefix_digest(&f.map);
    let report = json!({"runner_binary_sha256":runner_binary_sha256,"passed":passed,"source_sha":a.source_sha,"fixture_sha256":hex::encode(Sha256::digest(&raw)),"map_sha256":f.map_sha256,"sealed_prefix_shards":sealed_prefix_shards,"sealed_prefix_sha256":sealed_prefix_sha256,"preflight":preflight.as_ref().ok().map(|(_,_,sample)| sample.clone()),"preflight_error":preflight.as_ref().err().map(ToString::to_string),"cases":results,"http":http_evidence::summary(&a)?,"http_policy":{"maximum_attempts":a.http_attempts,"request_deadline_seconds":a.request_timeout_secs,"case_deadline_seconds":a.case_timeout_secs,"same_buffered_request":true}});
    std::fs::write(
        a.out_dir.join("report.json"),
        serde_json::to_vec_pretty(&report)?,
    )?;
    let failures = results.iter().filter(|r| r["passed"] != true).count();
    let mut junit = format!(
        "<testsuite name=\"transparent-regression\" tests=\"{}\" failures=\"{failures}\">",
        results.len()
    );
    for r in &results {
        junit.push_str(&format!(
            "<testcase name=\"{}\">",
            escape(r["id"].as_str().unwrap())
        ));
        if r["passed"] != true {
            junit.push_str(&format!(
                "<failure message=\"{}\"/>",
                escape(r["error"].as_str().unwrap_or("case failed"))
            ));
        }
        junit.push_str("</testcase>");
    }
    junit.push_str("</testsuite>\n");
    std::fs::write(a.out_dir.join("junit.xml"), junit)?;
    if !passed {
        bail!(
            "{failures} regression cases failed; see {}",
            a.out_dir.display()
        );
    }
    Ok(())
}
fn child(a: &Args, id: &str) -> Result<Value> {
    let mut process = Command::new(std::env::current_exe()?)
        .arg("--fixture")
        .arg(&a.fixture)
        .arg("--shard-url")
        .arg(&a.shard_url)
        .arg("--filter-url")
        .arg(&a.filter_url)
        .arg("--out-dir")
        .arg(&a.out_dir)
        .arg("--source-sha")
        .arg(&a.source_sha)
        .arg("--worker-case")
        .arg(id)
        .arg("--http-attempts")
        .arg(a.http_attempts.to_string())
        .arg("--request-timeout-secs")
        .arg(a.request_timeout_secs.to_string())
        .stdout(Stdio::from(std::fs::File::create(
            a.out_dir.join(format!("{id}.stdout")),
        )?))
        .stderr(Stdio::from(std::fs::File::create(
            a.out_dir.join(format!("{id}.stderr")),
        )?))
        .spawn()?;
    let started = Instant::now();
    loop {
        if let Some(status) = process.try_wait()? {
            let path = a.out_dir.join(format!("{id}.json"));
            let mut result: Value = if Path::new(&path).exists() {
                serde_json::from_slice(&std::fs::read(path)?)?
            } else {
                json!({"id":id,"passed":false,"error":format!("worker exited {status} without a report")})
            };
            if !status.success() {
                result["passed"] = json!(false);
            }
            return Ok(result);
        }
        if started.elapsed() >= Duration::from_secs(a.case_timeout_secs) {
            process.kill()?;
            process.wait()?;
            return Ok(json!({"id":id,"passed":false,"error":"case deadline exceeded"}));
        }
        std::thread::sleep(Duration::from_millis(100));
    }
}
