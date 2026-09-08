//! Sequential deployed correctness checks. Each case runs in a deadline-bounded child.
mod counting;
use anyhow::{bail, Context, Result};
use clap::Parser;
use counting::{Counting, CountingFilters};
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
    Completion, StaticChain, StaticScripts, WalletStore, WorkLimits,
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
fn pinned(a: &Args, f: &Fixture) -> Result<u64> {
    let mut bytes = 0;
    for url in [&a.filter_url, &a.shard_url] {
        let mut origin = HttpFilterSource::new(url, &options(a)).map_err(anyhow::Error::msg)?;
        let (raw, n) = origin.shard_map().map_err(anyhow::Error::msg)?;
        let observed_digest = hex::encode(Sha256::digest(&raw));
        let observed: transparent_filter::ShardMap = serde_json::from_slice(&raw)?;
        if observed_digest != f.map_sha256 || observed != f.map {
            bail!("publication drift at {url}: expected {} ({} shards), observed {} ({} shards, through {:?})", f.map_sha256, f.map.shards.len(), observed_digest, observed.shards.len(), observed.shards.last().map(|s|s.end_height));
        }
        bytes = n;
    }
    Ok(bytes)
}
fn run_case(a: &Args, f: &Fixture, case: &Case, stages: &mut Vec<Value>) -> Result<()> {
    let map_bytes = pinned(a, f)?;
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
        let started = Instant::now();
        let counts = Arc::new(Mutex::new(BTreeMap::new()));
        let mut transport = Counting {
            inner: HttpShardTransport::new(&a.shard_url, &options(a))
                .map_err(anyhow::Error::msg)?,
            stages: counts.clone(),
        };
        let mut filters = CountingFilters {
            inner: HttpFilterSource::new(&a.filter_url, &options(a)).map_err(anyhow::Error::msg)?,
            stages: counts.clone(),
        };
        let (init, _) = transport.init().map_err(anyhow::Error::msg)?;
        let geometry = transparent_wallet::parse_init(&init).map_err(anyhow::Error::msg)?;
        let mut store = SqliteStore::open(&path)?;
        let mut scripts = StaticScripts(entries.clone());
        let result = sync_into(
            &mut store,
            &f.map,
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
        stages.push(json!({"phase": phase, "anchor": cp.anchor, "seconds": started.elapsed().as_secs_f64(), "requests": &*counts.lock().unwrap(), "completion": report.map(|r| format!("{:?}",r.completion)), "error": result.as_ref().err().map(ToString::to_string)}));
        let report = result?;
        if report.completion != Completion::Complete {
            bail!(
                "incomplete at {}: {:?}",
                cp.anchor.height,
                report.completion
            );
        }
        if phase == "repeat" && report.charges.queries() != 0 {
            bail!("unchanged complete checkpoint issued private queries");
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
    pinned(a, f)?;
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
                &json!({"id":id,"profile":case.profile,"passed":result.is_ok(),"error":result.as_ref().err().map(ToString::to_string),"checkpoints":stages}),
            )?,
        )?;
        return result;
    }
    if a.out_dir.exists() {
        bail!("out-dir must be new; preserve earlier evidence");
    }
    std::fs::create_dir_all(&a.out_dir)?;
    let mut results = vec![];
    let preflight = pinned(&a, &f);
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
    let report = json!({"runner_binary_sha256":runner_binary_sha256,"passed":passed,"source_sha":a.source_sha,"fixture_sha256":hex::encode(Sha256::digest(&raw)),"map_sha256":f.map_sha256,"cases":results});
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
