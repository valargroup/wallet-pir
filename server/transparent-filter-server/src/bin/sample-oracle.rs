//! Recompute selected wallets' full-history answers independently from the event journal.
use clap::Parser;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    path::PathBuf,
};
use transparent_filter_server::events::EventStore;
#[derive(Parser)]
struct Cli {
    #[arg(long)]
    data_dir: PathBuf,
    #[arg(long)]
    sample: PathBuf,
    #[arg(long)]
    out: PathBuf,
}
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let cli = Cli::parse();
    let store = EventStore::open_existing(&cli.data_dir)?;
    let mut sample: Value = serde_json::from_slice(&std::fs::read(&cli.sample)?)?;
    let anchor = sample["anchor_height"].as_u64().ok_or("missing target")?;
    if store.start_height() != 0
        || sample["genesis_hash"] != store.genesis_hash()
        || store.covered_through().is_none_or(|h| h < anchor)
    {
        return Err("journal cannot supply full accepted history".into());
    }
    if sample["anchor_hash"]
        != store
            .block_at(anchor)
            .ok_or("missing anchor")?
            .block_hash
            .to_display_hex()
    {
        return Err("journal anchor mismatch".into());
    }
    let scripts: HashSet<Vec<u8>> = sample["clients"]
        .as_array()
        .ok_or("clients missing")?
        .iter()
        .flat_map(|c| c["scripts"].as_array().unwrap())
        .map(|s| hex::decode(s.as_str().unwrap()))
        .collect::<Result<_, _>>()?;
    let mut events: HashMap<Vec<u8>, Vec<transparent_events::TransparentEvent>> = HashMap::new();
    for height in 0..=anchor {
        for (script, event) in store.events_at(height)?.ok_or("journal gap")? {
            if scripts.contains(script.as_slice()) {
                events
                    .entry(script.as_slice().to_vec())
                    .or_default()
                    .push(event);
            }
        }
        if height % 100_000 == 0 {
            eprintln!("full-history oracle: {height}/{anchor}");
        }
    }
    for client in sample["clients"].as_array_mut().unwrap() {
        let mut owned = Vec::new();
        for script in client["scripts"].as_array().unwrap() {
            if let Some(e) = events.get(&hex::decode(script.as_str().unwrap())?) {
                owned.extend_from_slice(e);
            }
        }
        owned.sort_by_key(|e| e.sort_key());
        owned.dedup();
        let mut hash = Sha256::new();
        for event in &owned {
            hash.update(event.to_bytes());
        }
        client["required_from"] = json!(0);
        client["journal_events"] = json!(owned.len());
        client["expected_digest"] = json!(hex::encode(hash.finalize()));
    }
    sample["start_height"] = json!(0);
    sample["oracle"] =
        json!({"source":"independent event journal","scope":"full history for known scripts"});
    std::fs::write(cli.out, serde_json::to_vec_pretty(&sample)?)?;
    Ok(())
}
