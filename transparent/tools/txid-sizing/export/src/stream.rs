//! Stateless canonical block worker. State/atomic checkpoints belong to census.py.
use crate::{extract, AnyError, Previous};
use serde_json::{json, Value};
use std::{collections::{HashMap, HashSet}, io::{self, BufRead, Write}, sync::Arc};
use transparent_events::{FeeState, TransactionMetadata, Txid};
use transparent_shard::txid::{DisplayOutput, TransparentDisplayRecord};
use zakura_chain::{block::{merkle, Block}, serialization::{ZcashDeserialize, ZcashSerialize}, transparent::{Input, Output}};

fn process(v: Value) -> Result<Value, AnyError> {
    let raw = hex::decode(v["raw_hex"].as_str().ok_or("raw_hex")?)?;
    let mut cursor = raw.as_slice();
    let block = Block::zcash_deserialize(&mut cursor)?;
    if !cursor.is_empty() { return Err("trailing block bytes".into()); }
    let root: merkle::Root = block.transactions.iter().map(|tx| tx.hash()).collect();
    if root != block.header.merkle_root { return Err("transaction merkle root mismatch".into()); }
    let height = u32::try_from(v["height"].as_u64().ok_or("height")?)?;
    if block.coinbase_height().map(|h| h.0) != Some(height) {
        return Err("canonical coinbase height mismatch".into());
    }
    let mut seen = HashSet::new();
    let mut transactions = Vec::new();
    let mut previous = Previous(HashMap::new());
    let mut spent = HashSet::new();
    for tx in &block.transactions {
        let txid = hex::encode(tx.hash().0);
        if !seen.insert(txid.clone()) { return Err("duplicate transaction identity".into()); }
        let mut inputs = Vec::new();
        for input in tx.inputs() {
            if let Input::PrevOut { outpoint, .. } = input {
                let key = extract::outpoint_label(outpoint);
                if !spent.insert(key.clone()) { return Err("duplicate spent outpoint".into()); }
                inputs.push(key.clone());
                if let Some(encoded) = v["prevouts"][&key].as_str() {
                    let bytes = hex::decode(encoded)?;
                    let mut c = bytes.as_slice();
                    let output = Output::zcash_deserialize(&mut c)?;
                    if !c.is_empty() { return Err("trailing prevout bytes".into()); }
                    previous.0.insert(*outpoint, output);
                }
            }
        }
        let outputs = tx.outputs().iter().enumerate().map(|(index, output)| {
            let mut bytes = Vec::new(); output.zcash_serialize(&mut bytes)?;
            Ok(json!({"key":format!("{}:{}", tx.hash(), index), "canonical_hex":hex::encode(bytes)}))
        }).collect::<Result<Vec<_>, AnyError>>()?;
        let eligible = !tx.inputs().is_empty() || !tx.outputs().is_empty();
        let mut entry = json!({"txid_internal":txid,"inputs":inputs,"utxos":outputs,"eligible":eligible});
        if v["op"] == "extract" && eligible {
            // Extract one transaction against sequentially created outputs, so
            // future transactions cannot supply a missing previous output.
            let coinbase = tx.inputs().iter().any(|i| matches!(i, Input::Coinbase { .. }));
            let missing = tx.inputs().iter().filter_map(|i| match i {
                Input::PrevOut { outpoint, .. } if !previous.0.contains_key(outpoint) => Some(extract::outpoint_label(outpoint)),
                _ => None,
            }).collect::<Vec<_>>();
            let record = if missing.is_empty() {
                extract::extract_block(std::slice::from_ref(tx), &mut previous, height)?
                    .display.into_iter().next().ok_or("missing eligible display")?
            } else {
                // This is analysis-only: never relax the production filter's
                // refusal to build a partial filter. Shared encoder validates
                // the supported unknown-fee metadata state.
                TransparentDisplayRecord {
                    txid: Txid(tx.hash().0), coinbase,
                    metadata: TransactionMetadata { fee: FeeState::Unknown,
                        transparent_input_count: u32::try_from(inputs.len())?,
                        has_shielded_components: tx.has_shielded_data() },
                    outputs: tx.outputs().iter().map(|o| DisplayOutput {
                        value: u64::from(o.value), script: o.lock_script.as_raw_bytes().to_vec()
                    }).collect(),
                }
            };
            let fee = match record.metadata.fee {
                FeeState::Exact(n) => json!(n), FeeState::Unknown => json!("unknown"),
                FeeState::NotApplicable => json!("not_applicable"),
            };
            entry["record"] = json!({"txid_internal":txid,"height":height,"coinbase":coinbase,
                "input_count":record.metadata.transparent_input_count,"shielded_components":record.metadata.has_shielded_components,
                "fee":fee,"missing_prevouts":missing,
                "outputs":record.outputs.iter().map(|o| json!({"value":o.value,"script":hex::encode(&o.script)})).collect::<Vec<_>>(),
                "display_v1_hex":hex::encode(record.encode()?)});
        }
        // Remove consumed outputs and add new ones only after this tx.
        for input in tx.inputs() {
            if let Input::PrevOut { outpoint, .. } = input { previous.0.remove(outpoint); }
        }
        for (index, output) in tx.outputs().iter().enumerate() {
            previous.0.insert(zakura_chain::transparent::OutPoint { hash:tx.hash(), index:index as u32 }, output.clone());
        }
        transactions.push(entry);
    }
    Ok(json!({"height":height,"hash":block.hash().to_string(),
        "parent":block.header.previous_block_hash.to_string(),"transactions":transactions}))
}

pub fn run() -> Result<(), AnyError> {
    let stdin = io::stdin(); let mut out = io::BufWriter::new(io::stdout().lock());
    for line in stdin.lock().lines() {
        let result = process(serde_json::from_str(&line?)?);
        let response = match result { Ok(v) => v, Err(e) => json!({"error":e.to_string()}) };
        serde_json::to_writer(&mut out, &response)?; writeln!(out)?; out.flush()?;
    }
    Ok(())
}
