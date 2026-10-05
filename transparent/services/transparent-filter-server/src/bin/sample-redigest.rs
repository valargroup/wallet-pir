//! Re-derives a workload sample's expected digests under the current event codec.
//!
//! A sample's `expected_digest` is a SHA-256 over the wallet's events in their
//! canonical encoding, so it is tied to the codec that wrote it: the 87-byte
//! v2 codec digests the same history differently from the 96-byte v1 codec.
//! This keeps every client, script and range, recomputes each digest from the
//! journal with the sampler's rule, and refuses unless every client's event
//! count equals the count the sample recorded. Equal counts over the same
//! scripts and ranges are what show that only the encoding moved.

use clap::Parser;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use transparent_events::TransparentEvent;
use transparent_filter_server::events::EventStore;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "sample-redigest",
    about = "Re-derive a workload sample's digests under the current codec"
)]
struct Cli {
    #[arg(long)]
    sample: PathBuf,
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    #[arg(long)]
    out: PathBuf,
    #[arg(long)]
    source_sha: Option<String>,
}

fn height_of(event: &TransparentEvent) -> u64 {
    match event {
        TransparentEvent::Receive(r) => u64::from(r.height),
        TransparentEvent::Spend(s) => u64::from(s.height),
    }
}

fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    if cli.out.exists() {
        return Err(format!("{} already exists", cli.out.display()).into());
    }
    let raw = std::fs::read(&cli.sample)?;
    let mut sample: serde_json::Value = serde_json::from_slice(&raw)?;
    let store = EventStore::open_existing(&cli.data_dir)?;
    if sample["genesis_hash"].as_str() != Some(store.genesis_hash()) {
        return Err("the sample and the journal name different chains".into());
    }
    let anchor = sample["anchor_height"]
        .as_u64()
        .ok_or("sample has no anchor_height")?;
    let start = sample["start_height"].as_u64().unwrap_or(0);
    if store.covered_through().is_none_or(|tip| tip < anchor) {
        return Err("the journal does not cover the sample's anchor".into());
    }

    let clients = sample["clients"]
        .as_array()
        .ok_or("sample has no clients")?
        .clone();
    let mut wanted: HashSet<Vec<u8>> = HashSet::new();
    for client in &clients {
        for script in client["scripts"]
            .as_array()
            .ok_or("client has no scripts")?
        {
            wanted.insert(hex::decode(script.as_str().ok_or("script is not hex")?)?);
        }
    }

    let mut history: HashMap<Vec<u8>, Vec<TransparentEvent>> = HashMap::new();
    let started = std::time::Instant::now();
    for height in start..=anchor {
        for (script, event) in store.events_at(height)?.unwrap_or_default() {
            if wanted.contains(script.as_slice()) {
                history
                    .entry(script.as_slice().to_vec())
                    .or_default()
                    .push(event);
            }
        }
        if height % 250_000 == 0 {
            eprintln!("  {height} ({:.0}s)", started.elapsed().as_secs_f64());
        }
    }

    let mut rewritten = Vec::with_capacity(clients.len());
    let mut changed = 0usize;
    for (index, mut client) in clients.into_iter().enumerate() {
        let range = client["expected_range"]
            .as_array()
            .ok_or("client has no expected_range")?;
        let (from, through) = (
            range[0].as_u64().ok_or("bad range")?,
            range[1].as_u64().ok_or("bad range")?,
        );
        let mut events: Vec<TransparentEvent> = Vec::new();
        for script in client["scripts"].as_array().expect("checked") {
            let bytes = hex::decode(script.as_str().expect("checked"))?;
            for event in history.get(&bytes).into_iter().flatten() {
                let height = height_of(event);
                if height >= from && height <= through {
                    events.push(*event);
                }
            }
        }
        events.sort_by_key(|event| event.sort_key());
        events.dedup();
        let recorded = client["journal_events"]
            .as_u64()
            .ok_or("no journal_events")?;
        if events.len() as u64 != recorded {
            return Err(format!(
                "client {index} ({}) has {} events, the sample recorded {recorded}",
                client["class"],
                events.len()
            )
            .into());
        }
        let mut hasher = Sha256::new();
        for event in &events {
            hasher.update(event.to_bytes());
        }
        let digest = hex::encode(hasher.finalize());
        if client["expected_digest"].as_str() != Some(digest.as_str()) {
            changed += 1;
        }
        client["expected_digest"] = serde_json::Value::String(digest);
        rewritten.push(client);
    }

    sample["clients"] = serde_json::Value::Array(rewritten);
    sample["redigest"] = serde_json::json!({
        "from_sample_sha256": hex::encode(Sha256::digest(&raw)),
        "journal_version": store.version(),
        "event_encoding": "self-contained canonical event bytes, including available transaction metadata",
        "event_bytes_max": transparent_events::MAX_EVENT_BYTES,
        "journal": cli.data_dir,
        "tool_sha": cli.source_sha,
        "clients_changed": changed,
        "rule": "same clients, scripts and ranges; sha256 over the canonical records in the current codec, canonical order, deduplicated; every client's event count equals the recorded journal_events",
    });
    std::fs::write(&cli.out, serde_json::to_vec_pretty(&sample)?)?;
    eprintln!(
        "{} clients, {changed} digests changed, all event counts equal",
        sample["clients"].as_array().map_or(0, Vec::len)
    );
    Ok(())
}
