//! Bytes per txid display lookup: computed from the native lengths, and
//! measured on the wire.
//!
//! - `formula`: body bytes of the fixed transcript, exactly two directory
//!   queries whether the entry is found or not, per geometry, warm and cold,
//!   for the display geometries and, for comparison, display tables at
//!   history geometries' directory sizes. Header and metadata sizes are
//!   estimates given on the command line; bodies are exact.
//! - `measure`: lookups through a TCP byte meter, cold (a fresh client and
//!   connections), warm, and recent-stale (a warm client after the recent
//!   revision moved on), with the client's computed bytes beside the
//!   measured ones. Over plain HTTP the two must be equal; over HTTPS the
//!   difference is TLS and connection setup.
#[path = "support/bytemeter.rs"]
mod bytemeter;
#[path = "support/txdisplay.rs"]
mod txdisplay;

use clap::{Parser, Subcommand};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};
use transparent_shard::layout::Geometry;
use txdisplay::{DisplayClient, LookupReport, LookupResult};

type Error = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Formula {
        #[arg(long, default_value_t = 1)]
        directory_segments: u64,
        /// Canonical manifest bytes (measure one; about 1.5 KB at N=1).
        #[arg(long, default_value_t = 1_500)]
        manifest_bytes: u64,
        /// Map bytes per listed shard.
        #[arg(long, default_value_t = 600)]
        map_entry_bytes: u64,
        #[arg(long, default_value_t = 25)]
        map_entries: u64,
        /// HTTP/1.1 header bytes per request, both directions.
        #[arg(long, default_value_t = 400)]
        header_bytes: u64,
        /// JSON fields around a setup's base64 masks.
        #[arg(long, default_value_t = 300)]
        setup_json_bytes: u64,
        #[arg(long)]
        out: Option<PathBuf>,
    },
    Measure {
        #[arg(long)]
        url: String,
        #[arg(long)]
        fixture: PathBuf,
        #[arg(long, default_value_t = 3)]
        per_class: usize,
        /// How long to wait for the recent revision to move before measuring
        /// a stale client; zero skips the stale state.
        #[arg(long, default_value_t = 30)]
        stale_wait_seconds: u64,
        /// Measure only this tier (`archive` or `recent`), e.g. against one
        /// worker directly rather than through the edge.
        #[arg(long)]
        tier: Option<String>,
        #[arg(long)]
        out: Option<PathBuf>,
    },
}

/// Body bytes of one 44-bit dithered query and of its answer, for a table of
/// `rows` rows of 4,096 bytes answered by `segments` segments.
fn query_bytes(rows: u64, segments: u64) -> (u64, u64) {
    let cols = transparent_shard::txid::ROW_BYTES / 2;
    (
        8 + transparent_native::dithered_request_len(rows as usize) as u64,
        segments * (16 + transparent_native::response_len(cols) as u64),
    )
}

/// One segment's setup document: base64 masks plus JSON fields.
fn setup_bytes(json: u64) -> u64 {
    let public = transparent_native::public_len(transparent_shard::txid::ROW_BYTES / 2) as u64;
    4 * public.div_ceil(3) + json
}

fn formula(
    directory_segments: u64,
    manifest_bytes: u64,
    map_bytes: u64,
    header_bytes: u64,
    setup_json_bytes: u64,
) -> Vec<serde_json::Value> {
    let geometries: [(&Geometry, &str); 4] = [
        (&transparent_shard::display::TXID_2K, "display"),
        (&transparent_shard::display::TXID_4K, "display"),
        (&transparent_shard::layout::RECENT_4K_8K, "history-attached"),
        (&transparent_shard::layout::ARCHIVE_WIDE, "history-attached"),
    ];
    let setup = setup_bytes(setup_json_bytes);
    let mut rows = Vec::new();
    for (geometry, attachment) in geometries {
        let (dir_up, dir_down) = query_bytes(geometry.directory_rows, directory_segments);
        let queries = 2;
        let (up, down) = (queries * dir_up, queries * dir_down);
        let warm = up + down;
        let setups = directory_segments;
        for (state, metadata, requests) in [
            ("warm", 0, queries),
            (
                "cold",
                setups * setup + manifest_bytes + map_bytes,
                queries + setups + 2,
            ),
        ] {
            let headers = requests * header_bytes;
            let total = warm + metadata + headers;
            rows.push(serde_json::json!({
                "geometry": geometry.name,
                "attachment": attachment,
                "directory_rows": geometry.directory_rows,
                "state": state,
                "queries": queries,
                "query_up": up,
                "query_down": down,
                "query_bytes": warm,
                "metadata": metadata,
                "headers_estimate": headers,
                "total": total,
                "under_300k": total <= 300_000,
            }));
        }
    }
    rows
}

#[derive(Clone, Deserialize)]
struct Sample {
    txid: String,
    height: u64,
    class: String,
    entry_sha256: Option<String>,
}

/// The fixture schema `txid-inventory fixture` writes, with `entry_sha256`.
const FIXTURE_SCHEMA: &str = "transparent-txid-display-fixture-v2";

#[derive(Deserialize)]
struct Fixture {
    schema: String,
    samples: Vec<Sample>,
}

fn write(path: Option<&Path>, value: &serde_json::Value) -> Result<(), Error> {
    let text = serde_json::to_string_pretty(value)? + "\n";
    match path {
        Some(path) => std::fs::write(path, text)?,
        None => print!("{text}"),
    }
    Ok(())
}

/// A client connecting through the meter. Plain HTTP to an explicit port or
/// address is redirected wholesale; otherwise the host resolves to the meter,
/// which keeps the Host header and TLS server name.
fn metered_client(
    url: &reqwest::Url,
    meter: &bytemeter::ByteMeter,
    profiles: txdisplay::ProfileCache,
) -> Result<DisplayClient, Error> {
    let literal = url.host_str().is_some_and(|host| {
        host.trim_matches(['[', ']'])
            .parse::<std::net::IpAddr>()
            .is_ok()
    });
    if url.scheme() == "http" && (url.port().is_some() || literal) {
        return Ok(DisplayClient::new(
            &format!("http://{}", meter.addr),
            None,
            Some(profiles),
        )?);
    }
    if url.port().is_some() {
        return Err("an HTTPS URL measured through the meter must use the default port".into());
    }
    Ok(DisplayClient::new(
        url.as_str(),
        Some(meter.addr),
        Some(profiles),
    )?)
}

struct Measured {
    report: LookupReport,
    up: u64,
    down: u64,
    connections: u64,
}

async fn measured(
    meter: &bytemeter::ByteMeter,
    client: &DisplayClient,
    sample: &Sample,
) -> Result<Measured, Error> {
    let txid = txdisplay::parse_txid(&sample.txid).ok_or("fixture txid")?;
    let (up, down) = meter.snapshot();
    let connections = meter.connections();
    let report = client.lookup(txid, Some(sample.height)).await?;
    let exact = match (&report.result, &sample.entry_sha256) {
        (LookupResult::Found(entry), Some(sha)) => txdisplay::entry_sha256(entry) == *sha,
        (LookupResult::Absent, None) => true,
        _ => false,
    };
    if !exact {
        return Err(format!(
            "{} was not answered exactly: {}",
            sample.txid, report.outcome
        )
        .into());
    }
    // The relay counts a request before forwarding it and a response before
    // the client can read it, so these deltas are complete.
    let (after_up, after_down) = meter.snapshot();
    Ok(Measured {
        up: after_up - up,
        down: after_down - down,
        connections: meter.connections() - connections,
        report,
    })
}

fn row(sample: &Sample, tier: &str, state: &str, plain: bool, m: &Measured) -> serde_json::Value {
    let computed = (m.report.bytes.up(), m.report.bytes.down());
    serde_json::json!({
        "tier": tier,
        "class": sample.class,
        "state": state,
        "txid": sample.txid,
        "shard_id": m.report.shard_id,
        "digest": m.report.digest,
        "queries": m.report.queries.len(),
        "computed_up": computed.0,
        "computed_down": computed.1,
        "computed_total": computed.0 + computed.1,
        "measured_up": m.up,
        "measured_down": m.down,
        "measured_total": m.up + m.down,
        "new_connections": m.connections,
        "equal": computed == (m.up, m.down),
        "plain_http": plain,
        "bytes": m.report.bytes,
        "fetched": m.report.cold,
        "stale_retries": m.report.stale_retries,
        "under_300k": m.up + m.down <= 300_000,
    })
}

async fn measure(
    url: &str,
    fixture: &Path,
    per_class: usize,
    stale_wait: Duration,
    only: Option<&str>,
) -> Result<serde_json::Value, Error> {
    let fixture: Fixture = serde_json::from_slice(&std::fs::read(fixture)?)?;
    if fixture.schema != FIXTURE_SCHEMA {
        return Err(format!("not a {FIXTURE_SCHEMA} document: {}", fixture.schema).into());
    }
    let url = reqwest::Url::parse(url)?;
    let upstream = tokio::net::lookup_host((
        url.host_str().ok_or("URL host")?.trim_matches(['[', ']']),
        url.port_or_known_default().ok_or("URL port")?,
    ))
    .await?
    .next()
    .ok_or("URL host does not resolve")?;
    let meter = bytemeter::ByteMeter::start(upstream).await?;
    let plain = url.scheme() == "http";
    let observer = DisplayClient::new(url.as_str(), None, None)?;
    let (map, _, _) = observer.refresh_map().await?;
    let mut groups: BTreeMap<(String, String), Vec<Sample>> = BTreeMap::new();
    for sample in fixture.samples {
        let Some(shard) = map.shard_for_height(sample.height) else {
            continue;
        };
        if only.is_some_and(|tier| tier != shard.tier()) {
            continue;
        }
        groups
            .entry((shard.tier().to_string(), sample.class.clone()))
            .or_default()
            .push(sample);
    }
    let profiles = observer.profiles();
    let mut rows = Vec::new();
    let mut stale_clients = Vec::new();
    for ((tier, _), samples) in &groups {
        for sample in samples.iter().take(per_class) {
            let client = metered_client(&url, &meter, profiles.clone())?;
            let cold = measured(&meter, &client, sample).await?;
            rows.push(row(sample, tier, "cold", plain, &cold));
            let warm = measured(&meter, &client, sample).await?;
            rows.push(row(sample, tier, "warm", plain, &warm));
            if tier == "recent" {
                stale_clients.push((client, sample.clone()));
            }
        }
    }
    // A warm client after the recent revision moved on: its map, manifest
    // and setup are stale, which a 409 and one refresh resolve.
    if !stale_wait.is_zero() && !stale_clients.is_empty() {
        let (_, before, _) = observer.refresh_map().await?;
        let deadline = Instant::now() + stale_wait;
        let mut moved = false;
        while Instant::now() < deadline {
            tokio::time::sleep(Duration::from_secs(1)).await;
            if observer.refresh_map().await?.1 != before {
                moved = true;
                break;
            }
        }
        if moved {
            for (client, sample) in &stale_clients {
                let stale = measured(&meter, client, sample).await?;
                rows.push(row(sample, "recent", "recent-stale", plain, &stale));
            }
        } else {
            eprintln!("the recent revision did not move within the wait; no stale samples");
        }
    }
    let mut summary: BTreeMap<String, serde_json::Value> = BTreeMap::new();
    for row in &rows {
        let key = format!(
            "{}/{}/{}",
            row["tier"].as_str().unwrap_or_default(),
            row["class"].as_str().unwrap_or_default(),
            row["state"].as_str().unwrap_or_default()
        );
        let total = row["measured_total"].as_u64().unwrap_or_default();
        let entry = summary.entry(key).or_insert_with(
            || serde_json::json!({"samples": 0, "max_measured": 0, "sum_measured": 0}),
        );
        entry["samples"] = (entry["samples"].as_u64().unwrap_or(0) + 1).into();
        entry["sum_measured"] = (entry["sum_measured"].as_u64().unwrap_or(0) + total).into();
        entry["max_measured"] = entry["max_measured"]
            .as_u64()
            .unwrap_or(0)
            .max(total)
            .into();
    }
    let unequal = rows.iter().filter(|r| r["equal"] != true).count();
    Ok(serde_json::json!({
        "url": url.as_str(),
        "plain_http": plain,
        "rows": rows,
        "summary": summary,
        "computed_differs_from_measured": unequal,
    }))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    match Args::parse().command {
        Command::Formula {
            directory_segments,
            manifest_bytes,
            map_entry_bytes,
            map_entries,
            header_bytes,
            setup_json_bytes,
            out,
        } => {
            let rows = formula(
                directory_segments,
                manifest_bytes,
                map_entry_bytes * map_entries,
                header_bytes,
                setup_json_bytes,
            );
            for row in &rows {
                eprintln!(
                    "{:<14} {:<5} {:>9} B (queries {:>9} B)",
                    row["geometry"].as_str().unwrap_or_default(),
                    row["state"].as_str().unwrap_or_default(),
                    row["total"],
                    row["query_bytes"],
                );
            }
            write(out.as_deref(), &serde_json::json!({"rows": rows}))
        }
        Command::Measure {
            url,
            fixture,
            per_class,
            stale_wait_seconds,
            tier,
            out,
        } => {
            let report = measure(
                &url,
                &fixture,
                per_class,
                Duration::from_secs(stale_wait_seconds),
                tier.as_deref(),
            )
            .await?;
            let unequal = report["computed_differs_from_measured"]
                .as_u64()
                .unwrap_or(0);
            write(out.as_deref(), &report)?;
            if report["plain_http"] == true && unequal > 0 {
                return Err(
                    format!("{unequal} lookups differ from the meter over plain HTTP").into(),
                );
            }
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The figures the design was argued from, reproduced from the native
    /// lengths: 38,920 B per dithered query at 2,048 rows (40,200 B at the 49
    /// bits every server also accepts), and the warm transcripts, which are
    /// two directory queries for every lookup.
    #[test]
    fn warm_transcripts_match_the_native_lengths() {
        assert_eq!(query_bytes(2_048, 1), (38_920, 5_648));
        assert_eq!(query_bytes(4_096, 1).0, 50_184);
        assert_eq!(query_bytes(32_768, 1).0, 207_880);
        // The 49-bit lengths the design was first argued from.
        let legacy = |rows: usize| 8 + transparent_native::request_len(rows) as u64;
        assert_eq!(
            (legacy(2_048), legacy(4_096), legacy(32_768)),
            (40_200, 52_744, 228_360)
        );
        let rows = formula(1, 0, 0, 0, 0);
        let total = |geometry: &str, state: &str| {
            rows.iter()
                .find(|r| r["geometry"] == geometry && r["state"] == state)
                .unwrap()["total"]
                .as_u64()
                .unwrap()
        };
        // Two dithered queries: 2,560, 5,120 and 40,960 B below the 49-bit
        // totals of 91,696, 116,784 and 468,016.
        assert_eq!(total("txid-2k", "warm"), 89_136);
        assert_eq!(total("txid-4k", "warm"), 111_664);
        assert_eq!(total("archive-wide", "warm"), 427_056);
        assert_eq!(setup_bytes(0), 19_800);
        // Cold adds one setup per directory segment, plus map and manifest.
        assert_eq!(total("txid-2k", "cold"), 89_136 + 19_800);
        assert!(rows.iter().all(|r| r["queries"] == 2));
    }
}
