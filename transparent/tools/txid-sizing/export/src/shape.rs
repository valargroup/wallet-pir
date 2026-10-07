//! Parent-free inventory using canonical transactions, shared flags and codec.
//! Unknown fees remain unknown; Python bounds their optional encoded length.
use super::*;
use std::io::{self, BufRead, Read, Write};
use transparent_events::TransactionMetadata;
use transparent_shard::txid::{DisplayOutput, TransparentDisplayRecord};
use zakura_chain::transparent::Input;

pub fn run() -> Result<(), AnyError> {
    let stdin = io::stdin();
    let mut input = stdin.lock();
    let mut output = io::stdout().lock();
    loop {
        let mut line = Vec::new();
        let n = input
            .by_ref()
            .take(256 * 1024 * 1024 + 1)
            .read_until(b'\n', &mut line)?;
        if n == 0 {
            break;
        }
        if n > 256 * 1024 * 1024 {
            return Err("shape frame exceeds bound".into());
        }
        let frame: Value = serde_json::from_slice(&line)?;
        let height = u32::try_from(frame["height"].as_u64().ok_or("height")?)?;
        let raw = hex::decode(frame["raw_block"].as_str().ok_or("raw block")?)?;
        let mut cursor = raw.as_slice();
        let block = Block::zcash_deserialize(&mut cursor)?;
        if !cursor.is_empty() {
            return Err("trailing canonical block bytes".into());
        }
        if block.hash().to_string() != frame["hash"].as_str().ok_or("hash")?
            || block.coinbase_height().map(|h| h.0) != Some(height)
        {
            return Err("canonical block identity/height mismatch".into());
        }
        let mut records = Vec::new();
        let mut all_txids = Vec::new();
        for (index, tx) in block.transactions.iter().enumerate() {
            all_txids.push(hex::encode(tx.hash().0));
            let coinbase = tx
                .inputs()
                .iter()
                .any(|i| matches!(i, Input::Coinbase { .. }));
            if coinbase != (index == 0) {
                return Err("coinbase position mismatch".into());
            }
            if tx.inputs().is_empty() && tx.outputs().is_empty() {
                continue;
            }
            let points: std::collections::HashSet<_> = tx
                .inputs()
                .iter()
                .filter_map(|i| {
                    if let Input::PrevOut { outpoint, .. } = i {
                        Some(*outpoint)
                    } else {
                        None
                    }
                })
                .collect();
            let ni = tx
                .inputs()
                .iter()
                .filter(|i| matches!(i, Input::PrevOut { .. }))
                .count();
            if points.len() != ni {
                return Err("duplicate transparent input".into());
            }
            // No-input transactions need no parent facts. Reuse the production
            // value-balance extraction for their exact fee (and for coinbase).
            let record = if ni == 0 {
                extract::extract_block(
                    std::slice::from_ref(tx),
                    &mut Previous(HashMap::new()),
                    height,
                )?
                .display
                .into_iter()
                .next()
                .ok_or("eligible display missing")?
            } else {
                TransparentDisplayRecord {
                    txid: transparent_events::Txid(tx.hash().0),
                    coinbase,
                    metadata: TransactionMetadata {
                        fee: FeeState::Unknown,
                        transparent_input_count: u32::try_from(ni)?,
                        has_shielded_components: tx.has_shielded_data(),
                    },
                    outputs: tx
                        .outputs()
                        .iter()
                        .map(|o| DisplayOutput {
                            value: u64::from(o.value),
                            script: o.lock_script.as_raw_bytes().to_vec(),
                        })
                        .collect(),
                }
            };
            let fee = match record.metadata.fee {
                FeeState::Exact(v) => json!(v),
                FeeState::Unknown => json!("unknown"),
                FeeState::NotApplicable => json!("not_applicable"),
            };
            records.push(json!({"txid_internal":hex::encode(record.txid.0),"height":height,
                "transaction_index":index,"coinbase":coinbase,"input_count":ni,
                "shielded_components":record.metadata.has_shielded_components,"fee":fee,
                "outputs":record.outputs.iter().map(|o|json!({"value":o.value,"script":hex::encode(&o.script)})).collect::<Vec<_>>(),
                "display_v1_hex":hex::encode(record.encode()?)}));
        }
        serde_json::to_writer(
            &mut output,
            &json!({"height":height,"hash":block.hash().to_string(),
            "previousblockhash":block.header.previous_block_hash.to_string(),
            "transactions":block.transactions.len(),"eligible":records.len(),
            "shielded_only":block.transactions.len()-records.len(),"all_txids":all_txids,"records":records}),
        )?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}
