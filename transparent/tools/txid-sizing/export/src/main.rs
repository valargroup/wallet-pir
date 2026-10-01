//! Offline analysis only. Compile the existing canonical extractor without its
//! service/RocksDB stack. No fee implementation is copied into this tool.
#[allow(dead_code)]
#[path = "../../../../services/transparent-filter-server/src/extract.rs"]
mod extract;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::HashMap, error::Error, fs, path::Path, sync::Arc};
use transparent_events::FeeState;
use zakura_chain::{
    block::Block,
    serialization::ZcashDeserialize,
    transparent::{OutPoint, Output},
};
type AnyError = Box<dyn Error + Send + Sync>;
struct Previous(HashMap<OutPoint, Output>);
impl extract::PreviousOutputs for Previous {
    fn previous_output(&mut self, point: &OutPoint) -> Result<Option<Output>, AnyError> {
        Ok(self.0.get(point).cloned())
    }
}
mod census;
mod stream;
fn main() -> Result<(), AnyError> {
    let args: Vec<_> = std::env::args().collect();
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
        let mut cursor = raw.as_slice();
        let block = Arc::new(Block::zcash_deserialize(&mut cursor)?);
        if !cursor.is_empty() {
            return Err("trailing block bytes".into());
        }
        let stem = path.file_stem().unwrap().to_str().unwrap();
        let height: u32 = stem
            .trim_start_matches("block-main-")
            .replace("-", "")
            .parse()?;
        // Retain full outputs only from the chosen blocks. Missing external
        // parents cause explicit exclusions, never fabricated unknown fees.
        for tx in &block.transactions {
            for (index, output) in tx.outputs().iter().enumerate() {
                previous.0.insert(
                    OutPoint {
                        hash: tx.hash(),
                        index: index as u32,
                    },
                    output.clone(),
                );
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
        pin["block_hash_display"] = json!(block.hash().to_string());
        pin["transactions"] = json!(block.transactions.len());
        let mut eligible = 0;
        let mut extracted = 0;
        for (index, tx) in block.transactions.iter().enumerate() {
            if tx.inputs().is_empty() && tx.outputs().is_empty() {
                ineligible += 1;
                continue;
            }
            eligible += 1;
            match extract::extract_block(std::slice::from_ref(tx), &mut previous, height) {
                Ok(result) => {
                    let record = result
                        .display
                        .into_iter()
                        .next()
                        .ok_or("missing eligible display record")?;
                    let payload = record.encode()?;
                    let fee = match record.metadata.fee {
                        FeeState::Exact(v) => json!(v),
                        FeeState::Unknown => json!("unknown"),
                        FeeState::NotApplicable => json!("not_applicable"),
                    };
                    records.push(json!({"txid_internal":hex::encode(record.txid.0),"height":height,
                        "block_hash_display":block.hash().to_string(),"transaction_index":index,
                        "coinbase":record.coinbase,"input_count":record.metadata.transparent_input_count,
                        "shielded_components":record.metadata.has_shielded_components,"fee":fee,
                        "outputs":record.outputs.iter().map(|o| json!({"value":o.value,"script":hex::encode(&o.script)})).collect::<Vec<_>>(),
                        "display_v1_hex":hex::encode(payload)}));
                    canonical_records.push(record);
                    extracted += 1;
                }
                Err(extract::ExtractError::MissingPreviousOutput(_)) => excluded
                    .push(json!({"height":height,
                    "txid_display":tx.hash().to_string(),"reason":"external_prevout_unavailable"})),
                Err(e) => return Err(e.into()),
            }
        }
        pin["eligible"] = json!(eligible);
        pin["extracted"] = json!(extracted);
        pin["complete_display_block"] = json!(eligible == extracted);
        inventory.push(pin);
    }
    let built =
        transparent_shard::txid::build(0, &transparent_shard::RECENT_4K, &canonical_records)?;
    transparent_shard::txid::verify(
        0,
        &transparent_shard::RECENT_4K,
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
        "implemented_packing": {"shard_id":0,"geometry":"recent-4k","threshold":128,
            "payload_bytes":built.payload_bytes,"directory_segments":built.directory.len(),"page_segments":built.pages.len(),
            "occupied_directory_rows":directory_occupied.0,"occupied_page_rows":pages_occupied.0,
            "occupied_bytes":directory_occupied.1+pages_occupied.1,"manifest":built.manifest(&transparent_shard::RECENT_4K)},
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
