//! Block-bounded canonical extraction; no chain-sized in-memory JSON or UTXO map.
use zakura_pir_protocol::transparent::events as transparent_events;
use zakura_pir_publisher::transparent as transparent_shard;

use super::*;
use std::io::{self, BufRead, Read, Write};
const MAX_LINE: u64 = 256 * 1024 * 1024;

pub fn run() -> Result<(), AnyError> {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let stdout = io::stdout();
    let mut output = stdout.lock();
    loop {
        let mut line = Vec::new();
        let n = input
            .by_ref()
            .take(MAX_LINE + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if n as u64 > MAX_LINE {
            return Err("stream block frame exceeds bound".into());
        }
        let frame: Value = serde_json::from_slice(&line)?;
        let height = u32::try_from(frame["height"].as_u64().ok_or("height")?)?;
        let block = read_block(&hex::decode(
            frame["raw_block"].as_str().ok_or("raw block")?,
        )?)?;
        if display(&block.hash()) != frame["hash"].as_str().ok_or("block hash")? {
            return Err("raw block hash differs from RPC pin".into());
        }
        let mut previous = Previous(HashMap::new());
        for parent in frame["parents"].as_array().ok_or("parents")? {
            let tx = read_transaction(&hex::decode(parent["hex"].as_str().ok_or("parent hex")?)?)?;
            if display(&tx.txid) != parent["txid"].as_str().ok_or("parent txid")? {
                return Err("raw parent hash differs from requested txid".into());
            }
            for (index, out) in tx.outputs.into_iter().enumerate() {
                previous.0.insert((tx.txid, index as u32), out);
            }
        }
        let extracted = extract::extract_block(&block, &mut previous, height)?;
        let mut records = Vec::new();
        let transactions = block.transactions();
        let indexes: HashMap<_, _> = transactions
            .iter()
            .enumerate()
            .map(|(i, t)| (t.txid, i))
            .collect();
        for record in extracted.display.into_iter().map(|source| source.record) {
            let fee = match record.metadata.fee {
                FeeState::Exact(v) => json!(v),
                FeeState::Unknown => json!("unknown"),
                FeeState::NotApplicable => json!("not_applicable"),
            };
            records.push(json!({"txid_internal":hex::encode(record.txid.0),"height":height,
                "transaction_index":indexes[&record.txid.0],"coinbase":record.coinbase,
                "input_count":record.metadata.transparent_input_count,
                "shielded_components":record.metadata.has_shielded_components,"fee":fee,
                "outputs":record.outputs.iter().map(|o| json!({"value":o.value,"script":hex::encode(&o.script)})).collect::<Vec<_>>(),
                "display_v1_hex":hex::encode(record.encode()?)}));
        }
        let eligible = transactions
            .iter()
            .filter(|t| t.coinbase || !t.spends.is_empty() || !t.outputs.is_empty())
            .count();
        if eligible != records.len() {
            return Err("eligible/display inventory mismatch".into());
        }
        serde_json::to_writer(
            &mut output,
            &json!({"height":height,"hash":display(&block.hash()),
            "previousblockhash":display(&block.parent()),
            "transactions":transactions.len(),"eligible":eligible,
            "shielded_only":transactions.len()-eligible,"records":records}),
        )?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}

/// Bounded probability-sample packing oracle; never a full-census memory path.
pub fn packing() -> Result<(), AnyError> {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut records = Vec::new();
    let mut bytes = 0usize;
    let mut seen = std::collections::HashSet::new();
    loop {
        let mut line = Vec::new();
        let n = input
            .by_ref()
            .take(16 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if n > 16 * 1024 * 1024 {
            return Err("sample record frame bound".into());
        }
        let value: Value = serde_json::from_slice(&line)?;
        let txid: [u8; 32] = hex::decode(value["txid_internal"].as_str().ok_or("txid")?)?
            .try_into()
            .map_err(|_| "txid length")?;
        if !seen.insert(txid) {
            return Err("duplicate packing txid".into());
        }
        let payload = hex::decode(value["display_v1_hex"].as_str().ok_or("payload")?)?;
        bytes = bytes
            .checked_add(payload.len())
            .ok_or("sample payload total")?;
        if bytes > 128 * 1024 * 1024 || records.len() >= 100_000 {
            return Err("sample packing memory bound".into());
        }
        records.push(
            transparent_shard::txid_v1::TransparentDisplayRecord::decode(
                transparent_events::Txid(txid),
                &payload,
            )?,
        );
    }
    let built = crate::historical_v1::build(0, &transparent_shard::DECOUPLED_2K_8K, &records)?;
    crate::historical_v1::verify(
        0,
        &transparent_shard::DECOUPLED_2K_8K,
        &built.directory,
        &built.pages,
        built.records,
    )?;
    let occupied = |segments: &[Vec<u8>]| -> Result<(usize, usize), AnyError> {
        let mut rows = 0;
        let mut used = 0;
        for segment in segments {
            for row in segment.chunks_exact(4096) {
                let count = u32::from_le_bytes(row[..4].try_into()?);
                if count == 0 {
                    continue;
                }
                rows += 1;
                let mut at = 4;
                for _ in 0..count {
                    let len = u16::from_le_bytes(row[at..at + 2].try_into()?) as usize;
                    at += 2 + len;
                }
                used += at;
            }
        }
        Ok((rows, used))
    };
    let directory = occupied(&built.directory)?;
    let pages = occupied(&built.pages)?;
    serde_json::to_writer(
        io::stdout().lock(),
        &json!({"transactions":records.len(),
        "payload_bytes":built.payload_bytes,"directory_segments":built.directory.len(),"page_segments":built.pages.len(),
        "occupied_directory_rows":directory.0,"occupied_page_rows":pages.0,"occupied_bytes":directory.1+pages.1,
        "directory_sha256":built.directory.iter().map(|s|hex::encode(Sha256::digest(s))).collect::<Vec<_>>(),
        "pages_sha256":built.pages.iter().map(|s|hex::encode(Sha256::digest(s))).collect::<Vec<_>>(),
        "verification":"implemented display-v1/128 builder and complete table verifier"}),
    )?;
    Ok(())
}
