//! Offline analysis only. Compile the existing canonical extractor without its
//! service/RocksDB stack. No fee implementation is copied into this tool.
// The service re-exports adapter items this tool does not use.
#[allow(dead_code, unused_imports)]
#[path = "../../../../../src/transparent/transparent-filter-server/src/extract.rs"]
mod extract;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, error::Error, fs, path::Path};
use transparent_events::FeeState;
use zakura_server_chain::{
    hash::display,
    transparent::{read_transaction, FetchedBlock, Output},
};
type AnyError = Box<dyn Error + Send + Sync>;
/// An outpoint: a txid in protocol byte order and an output index.
type OutPoint = ([u8; 32], u32);
struct Previous(HashMap<OutPoint, Output>);
impl extract::PreviousOutputs for Previous {
    fn previous_output(&mut self, txid: [u8; 32], index: u32) -> Result<Option<Output>, AnyError> {
        Ok(self.0.get(&(txid, index)).cloned())
    }
}

/// The single block in `raw`, refusing trailing bytes.
fn read_block(raw: &[u8]) -> Result<FetchedBlock, AnyError> {
    Ok(FetchedBlock::from_exact_bytes(raw)?)
}
mod census;
#[allow(dead_code)]
#[path = "../../../../../src/transparent/transparent-filter-server/src/display_journal.rs"]
mod display_journal;
#[allow(dead_code)]
#[path = "../../../../../src/transparent/transparent-filter-server/src/events.rs"]
mod events;
#[allow(dead_code)]
mod historical_v1;
mod journal_census;
mod shape;
mod source_info;
mod stream;
fn main() -> Result<(), AnyError> {
    let args: Vec<_> = std::env::args().collect();
    if args.get(1).map(String::as_str) == Some("--source-info") {
        if args.len() != 2 {
            return Err("usage: txid-sizing-export --source-info".into());
        }
        println!("{}", source_info::Executing::capture()?.into_identity());
        return Ok(());
    }
    if args.get(1).map(String::as_str) == Some("--shape-stream") {
        return shape::run();
    }
    if args.get(1).map(String::as_str) == Some("--journal-census") {
        return journal_census::run(&args[2..]);
    }
    if args.get(1).map(String::as_str) == Some("--census") {
        return census::run(args.get(2).ok_or("census database path")?);
    }
    if args.get(1).map(String::as_str) == Some("--pack-stream") {
        return stream::packing();
    }
    if args.get(1).map(String::as_str) == Some("--stream") {
        return stream::run();
    }
    if args.len() != 3 {
        return Err("usage: txid-sizing-export VECTOR_DIR OUTPUT.json".into());
    }
    let mut paths: Vec<_> = fs::read_dir(&args[1])?
        .map(|x| x.map(|x| x.path()))
        .collect::<Result<_, _>>()?;
    paths.retain(|p| {
        let name = p.file_name().unwrap().to_string_lossy();
        name.starts_with("block-main-") && name.ends_with(".txt") && !name.contains("bad")
    });
    paths.sort();
    let mut blocks = Vec::new();
    let mut previous = Previous(HashMap::new());
    for path in &paths {
        let text = fs::read(path)?;
        let raw = hex::decode(std::str::from_utf8(&text)?.trim())?;
        let block = read_block(&raw)?;
        let stem = path.file_stem().unwrap().to_str().unwrap();
        let height: u32 = stem
            .trim_start_matches("block-main-")
            .replace("-", "")
            .parse()?;
        // Retain full outputs only from the chosen blocks. Missing external
        // parents cause explicit exclusions, never fabricated unknown fees.
        for tx in block.transactions() {
            for (index, output) in tx.outputs.into_iter().enumerate() {
                previous.0.insert((tx.txid, index as u32), output);
            }
        }
        blocks.push((height, block, json!({"height":height,"file":path.file_name().unwrap().to_str(),
            "text_sha256":hex::encode(Sha256::digest(&text)),"raw_sha256":hex::encode(Sha256::digest(&raw)),"raw_bytes":raw.len()})));
    }
    let mut records = Vec::<Value>::new();
    let mut canonical_records = Vec::new();
    let mut excluded = Vec::<Value>::new();
    let mut inventory = Vec::new();
    let mut ineligible = 0;
    for (height, block, mut pin) in blocks {
        let transactions = block.transactions();
        pin["block_hash_display"] = json!(display(&block.hash()));
        pin["transactions"] = json!(transactions.len());
        let mut eligible = 0;
        let mut extracted = 0;
        for (index, tx) in transactions.iter().enumerate() {
            if !tx.coinbase && tx.spends.is_empty() && tx.outputs.is_empty() {
                ineligible += 1;
                continue;
            }
            eligible += 1;
            match block
                .transparent_transaction(index, height, &mut previous)
                .and_then(|read| extract::index(&[read], height, true))
            {
                Ok(result) => {
                    // Historical v1 analysis: the v1 record the extracted
                    // v2x source record carries, never its v2x encoding.
                    let record = result
                        .display
                        .into_iter()
                        .next()
                        .ok_or("missing eligible display record")?
                        .record;
                    let payload = record.encode()?;
                    let fee = match record.metadata.fee {
                        FeeState::Exact(v) => json!(v),
                        FeeState::Unknown => json!("unknown"),
                        FeeState::NotApplicable => json!("not_applicable"),
                    };
                    records.push(json!({"txid_internal":hex::encode(record.txid.0),"height":height,
                        "block_hash_display":display(&block.hash()),"transaction_index":index,
                        "coinbase":record.coinbase,"input_count":record.metadata.transparent_input_count,
                        "shielded_components":record.metadata.has_shielded_components,"fee":fee,
                        "outputs":record.outputs.iter().map(|o| json!({"value":o.value,"script":hex::encode(&o.script)})).collect::<Vec<_>>(),
                        "display_v1_hex":hex::encode(payload)}));
                    canonical_records.push(record);
                    extracted += 1;
                }
                Err(extract::ExtractError::MissingPreviousOutput(_)) => excluded
                    .push(json!({"height":height,
                    "txid_display":display(&tx.txid),"reason":"external_prevout_unavailable"})),
                Err(e) => return Err(e.into()),
            }
        }
        pin["eligible"] = json!(eligible);
        pin["extracted"] = json!(extracted);
        pin["complete_display_block"] = json!(eligible == extracted);
        inventory.push(pin);
    }
    let built =
        crate::historical_v1::build(0, &transparent_shard::DECOUPLED_2K_8K, &canonical_records)?;
    crate::historical_v1::verify(
        0,
        &transparent_shard::DECOUPLED_2K_8K,
        &built.directory,
        &built.pages,
        built.records,
    )?;
    let occupied = |segments: &[Vec<u8>]| -> (usize, usize) {
        let mut rows = 0;
        let mut bytes = 0;
        for segment in segments {
            for row in segment.chunks_exact(4096) {
                let count = u32::from_le_bytes(row[..4].try_into().unwrap());
                if count == 0 {
                    continue;
                }
                rows += 1;
                let mut at = 4;
                for _ in 0..count {
                    let n = u16::from_le_bytes(row[at..at + 2].try_into().unwrap()) as usize;
                    at += 2 + n;
                }
                bytes += at;
            }
        }
        (rows, bytes)
    };
    let directory_occupied = occupied(&built.directory);
    let pages_occupied = occupied(&built.pages);
    let output = json!({"schema":"txid-sizing-canonical-extract-v1","qualification":"UNQUALIFIED",
        "selection":"all valid block-main-*.txt at the pinned upstream revision; availability-selected convenience sample",
        "parent_selection":"outputs in selected blocks only; no network or demo parent fallback",
        "implemented_packing": {"shard_id":0,"geometry":"decoupled-2k-8k","threshold":128,
            "payload_bytes":built.payload_bytes,"directory_segments":built.directory.len(),"page_segments":built.pages.len(),
            "occupied_directory_rows":directory_occupied.0,"occupied_page_rows":pages_occupied.0,
            "occupied_bytes":directory_occupied.1+pages_occupied.1,"manifest":built.manifest(&transparent_shard::DECOUPLED_2K_8K)},
        "blocks":inventory,"records":records,"excluded":excluded,"shielded_only_transactions":ineligible});
    let dest = Path::new(&args[2]);
    fs::write(dest, serde_json::to_vec(&output)?)?;
    println!(
        "blocks={} records={} excluded={} shielded_only={}",
        output["blocks"].as_array().unwrap().len(),
        output["records"].as_array().unwrap().len(),
        output["excluded"].as_array().unwrap().len(),
        ineligible
    );
    Ok(())
}
