//! Open-loop txid display load with an exactness oracle.
//!
//! Units are dispatched at fixed due times, `start + i / rate`, whatever the
//! service does: a slot with no free worker is reported as `missed_slot`, and
//! a dispatcher that falls behind skips the slots it missed instead of
//! bursting to catch up. Two units:
//!
//! - `lookup`: the client's whole transcript (map, manifest, setup, two
//!   directory queries, the record's page queries), checked against the
//!   fixture's `record_sha256`, or `Absent` for controls.
//! - `query`: one directory query, like for like with the history reference
//!   load, which sends single-row queries.
//!
//! The tier of each sample is decided at dispatch from the latest polled map,
//! so a recent sample that has since sealed is counted as archive traffic.
//! Cold samples use a fresh client (no cached map, manifest or setup, new
//! connections) and are tagged. Output is JSONL on stdout.
#[path = "support/txdisplay.rs"]
mod txdisplay;

use clap::{Parser, ValueEnum};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use transparent_shard::display::DisplayMap;
use txdisplay::{DisplayClient, LookupReport, LookupResult};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Clone, Copy, Debug, PartialEq, Eq, ValueEnum)]
enum Unit {
    Lookup,
    Query,
}

#[derive(Parser)]
struct Args {
    #[arg(long)]
    url: String,
    /// A `txid-inventory fixture` document.
    #[arg(long)]
    fixture: PathBuf,
    /// Units per second.
    #[arg(long, default_value_t = 20.0)]
    rate: f64,
    /// Units that may be in flight at once.
    #[arg(long, default_value_t = 64)]
    workers: usize,
    /// Zero runs until stopped. A supervisor must refresh the permit file.
    #[arg(long, default_value_t = 0)]
    seconds: u64,
    #[arg(long)]
    permit: PathBuf,
    #[arg(long, value_enum, default_value_t = Unit::Lookup)]
    unit: Unit,
    /// Tier shares, e.g. `recent=0.8,archive=0.2`.
    #[arg(long, default_value = "recent=0.8,archive=0.2")]
    mix: String,
    /// Every Nth lookup uses a fresh client; zero never does.
    #[arg(long, default_value_t = 0)]
    cold_every: u64,
    /// Every Nth lookup asks an absent control; zero never does.
    #[arg(long, default_value_t = 20)]
    absent_every: u64,
    #[arg(long, default_value_t = 1_000)]
    map_poll_ms: u64,
}

#[derive(Clone, Deserialize)]
struct Sample {
    txid: String,
    height: u64,
    #[serde(default)]
    class: String,
    #[serde(default)]
    record_sha256: Option<String>,
    #[serde(default)]
    kind: Option<String>,
}

#[derive(Deserialize)]
struct Fixture {
    schema: String,
    samples: Vec<Sample>,
    #[serde(default)]
    absent: Vec<Sample>,
}

fn now() -> f64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs_f64()
}

fn emit(value: serde_json::Value) {
    println!("{value}");
}

fn allowed(path: &Path) -> bool {
    std::fs::metadata(path)
        .and_then(|m| m.modified())
        .ok()
        .and_then(|t| t.elapsed().ok())
        .is_some_and(|age| age < Duration::from_secs(45))
        && std::fs::read_to_string(path).is_ok_and(|s| s.trim() == "allow")
}

/// Parses `recent=0.8,archive=0.2` into the recent share.
fn recent_share(mix: &str) -> Result<f64, Error> {
    let mut recent = None;
    let mut archive = None;
    for part in mix.split(',') {
        let (name, value) = part.split_once('=').ok_or("mix entries are name=share")?;
        let value: f64 = value.trim().parse()?;
        match name.trim() {
            "recent" => recent = Some(value),
            "archive" => archive = Some(value),
            other => return Err(format!("unknown tier {other}").into()),
        }
    }
    let (recent, archive) = (recent.unwrap_or(0.0), archive.unwrap_or(0.0));
    if recent < 0.0 || archive < 0.0 || (recent + archive - 1.0).abs() > 1e-9 {
        return Err("tier shares must be non-negative and sum to one".into());
    }
    Ok(recent)
}

/// Whether unit `sequence` is a recent one: an exact share over every run of
/// slots, spread evenly rather than drawn at random.
fn is_recent(sequence: u64, share: f64) -> bool {
    let before = (sequence as f64 * share).floor();
    let after = ((sequence + 1) as f64 * share).floor();
    after > before
}

/// The next sample of `tier` from `pool`, round robin, judged against the
/// current map: samples beyond its coverage are skipped.
fn pick<'a>(
    pool: &'a [Sample],
    map: &DisplayMap,
    archive: bool,
    cursor: &mut usize,
) -> Option<&'a Sample> {
    for _ in 0..pool.len() {
        let sample = &pool[*cursor % pool.len()];
        *cursor += 1;
        if map
            .shard_for_height(sample.height)
            .is_some_and(|shard| shard.sealed == archive)
        {
            return Some(sample);
        }
    }
    None
}

fn http_phases(report: &LookupReport) -> f64 {
    let phase = |pages: bool| {
        report
            .queries
            .iter()
            .filter(|q| (q.table == "pages") == pages)
            .map(|q| q.http_s)
            .fold(0.0, f64::max)
    };
    phase(false) + phase(true)
}

fn emit_queries(sequence: u64, report: &LookupReport, cold: bool) {
    for query in &report.queries {
        emit(serde_json::json!({
            "event": "query", "unix": now(), "sequence": sequence,
            "shard": report.shard_id, "sealed": report.sealed, "bucket": report.bucket,
            "table": query.table, "row": query.row, "status": query.status,
            "http_seconds": query.http_s, "prepare_seconds": query.prepare_s,
            "decode_seconds": query.decode_s,
            "up_body": query.up_body, "down_body": query.down_body,
            "up_headers": query.up_headers, "down_headers": query.down_headers,
            "cold": cold,
        }));
    }
}

async fn run_unit(
    client: DisplayClient,
    unit: Unit,
    sample: Sample,
    sequence: u64,
    due: Instant,
    cold: bool,
) {
    let started = Instant::now();
    let unix = now();
    let lag = started.saturating_duration_since(due).as_secs_f64();
    let Some(txid) = txdisplay::parse_txid(&sample.txid) else {
        emit(
            serde_json::json!({"event":"error","unix":unix,"sequence":sequence,"kind":"fixture","error":"unparseable txid"}),
        );
        return;
    };
    let expected = if sample.record_sha256.is_some() {
        "found"
    } else {
        "absent"
    };
    match unit {
        Unit::Lookup => match client.lookup(txid, Some(sample.height)).await {
            Ok(report) => {
                let exact = match (&report.result, &sample.record_sha256) {
                    (LookupResult::Found(record), Some(sha)) => {
                        txdisplay::record_sha256(record) == *sha
                    }
                    (LookupResult::Absent, None) => true,
                    _ => false,
                };
                emit_queries(sequence, &report, cold);
                emit(serde_json::json!({
                    "event": "lookup", "unix": unix, "sequence": sequence,
                    "schedule_lag_seconds": lag, "total_seconds": report.total_s,
                    "prepare_seconds": report.queries.iter().map(|q| q.prepare_s).sum::<f64>(),
                    "decode_seconds": report.queries.iter().map(|q| q.decode_s).sum::<f64>(),
                    "http_seconds": http_phases(&report),
                    "class": report.class, "fixture_class": sample.class,
                    "control": sample.kind,
                    "shard_id": report.shard_id, "sealed": report.sealed,
                    "bucket": report.bucket, "digest": report.digest,
                    "map_sha256": report.map_sha256,
                    "queries": report.queries.len(),
                    "up": report.bytes.up(), "down": report.bytes.down(),
                    "bytes": report.bytes, "cold": cold, "fetched": report.cold,
                    "exact": exact, "outcome": report.outcome, "expected": expected,
                    "stale_retries": report.stale_retries,
                }));
            }
            Err(error) => emit(serde_json::json!({
                "event": "error", "unix": unix, "sequence": sequence, "kind": "lookup",
                "cold": cold, "status": error.status(), "error": error.to_string(),
                "seconds": started.elapsed().as_secs_f64(),
            })),
        },
        Unit::Query => {
            let choice = (sequence % 2) as usize;
            match client.directory_query(txid, sample.height, choice).await {
                Ok((report, contains)) => {
                    emit_queries(sequence, &report, cold);
                    emit(serde_json::json!({
                        "event": "lookup", "unit": "query", "unix": unix, "sequence": sequence,
                        "schedule_lag_seconds": lag, "total_seconds": report.total_s,
                        "http_seconds": http_phases(&report),
                        "shard_id": report.shard_id, "sealed": report.sealed,
                        "bucket": report.bucket, "choice": choice,
                        // A single row has no record to compare; exactness is
                        // the binding and epoch checks and a well-formed row.
                        "outcome": report.outcome, "contains": contains,
                        "expected": expected,
                        "exact": report.outcome == "queried",
                        "up": report.bytes.up(), "down": report.bytes.down(),
                        "cold": cold, "stale_retries": report.stale_retries,
                    }));
                }
                Err(error) => emit(serde_json::json!({
                    "event": "error", "unix": unix, "sequence": sequence, "kind": "query",
                    "cold": cold, "status": error.status(), "error": error.to_string(),
                })),
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let args = Args::parse();
    if !(args.rate > 0.0 && args.rate <= 100.0) || args.workers == 0 || args.workers > 256 {
        return Err("invalid rate or worker count".into());
    }
    let share = recent_share(&args.mix)?;
    let raw = std::fs::read(&args.fixture)?;
    let fixture: Fixture = serde_json::from_slice(&raw)?;
    if fixture.schema != "transparent-txid-display-fixture-v1" || fixture.samples.is_empty() {
        return Err("not a txid display fixture with samples".into());
    }
    let client = DisplayClient::new(&args.url, None, None)?;
    let (map, map_sha256, _) = client.refresh_map().await?;
    let map = Arc::new(std::sync::RwLock::new((map, map_sha256)));
    {
        let (client, map) = (client.clone(), map.clone());
        let interval = Duration::from_millis(args.map_poll_ms.max(50));
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                match client.refresh_map().await {
                    Ok((next, sha256, bytes)) => {
                        let changed = map.read().unwrap().1 != sha256;
                        if changed {
                            emit(serde_json::json!({
                                "event": "map", "unix": now(), "map_sha256": sha256,
                                "covered_through": next.covered_through(),
                                "first_shard_id": next.first_shard_id,
                                "recent_id": next.shards.last().map(|s| s.shard_id),
                                "sealed_count": next.shards.iter().filter(|s| s.sealed).count(),
                                "bytes": bytes,
                                "shards": next.shards.iter().map(|s| serde_json::json!({
                                    "id": s.shard_id, "digest": s.manifest_digest, "sealed": s.sealed,
                                    "start": s.start_height, "end": s.end_height,
                                })).collect::<Vec<_>>(),
                            }));
                            *map.write().unwrap() = (next, sha256);
                        }
                    }
                    Err(error) => emit(serde_json::json!({
                        "event": "error", "unix": now(), "kind": "map_poll", "error": error.to_string(),
                    })),
                }
            }
        });
    }
    emit(serde_json::json!({
        "event": "ready", "unix": now(), "unit": format!("{:?}", args.unit).to_lowercase(),
        "rate": args.rate, "workers": args.workers, "mix": args.mix,
        "cold_every": args.cold_every, "absent_every": args.absent_every,
        "samples": fixture.samples.len(), "absent": fixture.absent.len(),
        "fixture_sha256": hex::encode(Sha256::digest(&raw)),
        "map_sha256": map.read().unwrap().1,
    }));
    let permits = Arc::new(tokio::sync::Semaphore::new(args.workers));
    let interval = Duration::from_secs_f64(1.0 / args.rate);
    let started = Instant::now();
    let mut schedule = Instant::now() + Duration::from_millis(100);
    let mut slot = 0u64;
    let mut sequence = 0u64;
    let (mut recent_cursor, mut archive_cursor, mut absent_cursor) = (0usize, 0usize, 0usize);
    while args.seconds == 0 || started.elapsed() < Duration::from_secs(args.seconds) {
        if !allowed(&args.permit) {
            emit(
                serde_json::json!({"event":"paused","unix":now(),"reason":"permit missing, stale or denied"}),
            );
            tokio::time::sleep(Duration::from_secs(5)).await;
            schedule = Instant::now() + interval;
            slot = 0;
            continue;
        }
        let due = schedule + interval.mul_f64(slot as f64);
        if let Some(wait) = due.checked_duration_since(Instant::now()) {
            tokio::time::sleep(wait).await;
        } else if Instant::now().duration_since(due) > interval {
            // Never burst to catch up: every slot already gone is missed.
            let behind =
                (Instant::now().duration_since(due).as_secs_f64() / interval.as_secs_f64()) as u64;
            emit(
                serde_json::json!({"event":"missed_slot","unix":now(),"slots":behind,"reason":"dispatcher late"}),
            );
            slot += behind;
            continue;
        }
        slot += 1;
        let Ok(permit) = permits.clone().try_acquire_owned() else {
            emit(
                serde_json::json!({"event":"missed_slot","unix":now(),"sequence":sequence,"reason":"every worker busy"}),
            );
            sequence += 1;
            continue;
        };
        let current = map.read().unwrap().0.clone();
        let archive = !is_recent(sequence, share);
        let sample = if args.unit == Unit::Lookup
            && args.absent_every > 0
            && sequence % args.absent_every == args.absent_every - 1
        {
            pick(&fixture.absent, &current, archive, &mut absent_cursor).cloned()
        } else if archive {
            pick(&fixture.samples, &current, true, &mut archive_cursor).cloned()
        } else {
            pick(&fixture.samples, &current, false, &mut recent_cursor).cloned()
        };
        let Some(sample) = sample else {
            emit(
                serde_json::json!({"event":"missed_slot","unix":now(),"sequence":sequence,"reason":"no fixture sample in this tier"}),
            );
            sequence += 1;
            continue;
        };
        let cold = args.cold_every > 0 && sequence.is_multiple_of(args.cold_every);
        let unit_client = if cold {
            client.fresh(None)?
        } else {
            client.clone()
        };
        let unit = args.unit;
        tokio::spawn(async move {
            run_unit(unit_client, unit, sample, sequence, due, cold).await;
            drop(permit);
        });
        sequence += 1;
    }
    // Let in-flight units finish.
    let _ = permits.acquire_many(args.workers as u32).await;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_mix_is_exact_over_every_run_of_slots() {
        let share = recent_share("recent=0.8,archive=0.2").unwrap();
        let recent = (0..1_000).filter(|s| is_recent(*s, share)).count();
        assert_eq!(recent, 800);
        for window in (0..1_000u64).collect::<Vec<_>>().chunks(10) {
            assert_eq!(window.iter().filter(|s| is_recent(**s, share)).count(), 8);
        }
        assert!(recent_share("recent=0.5,archive=0.4").is_err());
        assert!(recent_share("hot=1").is_err());
        assert_eq!(recent_share("archive=1").unwrap(), 0.0);
    }

    #[test]
    fn missing_or_denied_permit_stops_admission() {
        let d = tempfile::tempdir().unwrap();
        let p = d.path().join("permit");
        assert!(!allowed(&p));
        std::fs::write(&p, "deny").unwrap();
        assert!(!allowed(&p));
        std::fs::write(&p, "allow").unwrap();
        assert!(allowed(&p));
    }
}
