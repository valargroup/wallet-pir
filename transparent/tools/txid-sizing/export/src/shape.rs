//! Parent-free inventory using canonical transactions, shared flags and codec.
//! Unknown fees remain unknown; Python bounds their optional encoded length.
use super::*;
use std::io::{self, BufRead, Read, Write};
use transparent_events::TransactionMetadata;
use transparent_shard::txid::{DisplayOutput, TransparentDisplayRecord};

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
        let block =
            FetchedBlock::from_exact_bytes(&raw).map_err(|e| format!("canonical block: {e}"))?;
        if display(&block.hash()) != frame["hash"].as_str().ok_or("hash")?
            || block.coinbase_height() != Some(height)
        {
            return Err("canonical block identity/height mismatch".into());
        }
        let mut records = Vec::new();
        let mut all_txids = Vec::new();
        let transactions = block.transactions();
        for (index, tx) in transactions.iter().enumerate() {
            all_txids.push(hex::encode(tx.txid));
            let coinbase = tx.coinbase;
            if coinbase != (index == 0) {
                return Err("coinbase position mismatch".into());
            }
            if !coinbase && tx.spends.is_empty() && tx.outputs.is_empty() {
                continue;
            }
            let points: std::collections::HashSet<_> = tx.spends.iter().copied().collect();
            let ni = tx.spends.len();
            if points.len() != ni {
                return Err("duplicate transparent input".into());
            }
            // No-input transactions need no parent facts. Reuse the production
            // value-balance extraction for their exact fee (and for coinbase).
            let record = if ni == 0 {
                let read =
                    block.transparent_transaction(index, height, &mut Previous(HashMap::new()))?;
                extract::index(&[read], height, true)?
                    .display
                    .into_iter()
                    .next()
                    .ok_or("eligible display missing")?
            } else {
                TransparentDisplayRecord {
                    txid: transparent_events::Txid(tx.txid),
                    coinbase,
                    metadata: TransactionMetadata {
                        fee: FeeState::Unknown,
                        transparent_input_count: u32::try_from(ni)?,
                        has_shielded_components: tx.has_shielded_components,
                    },
                    outputs: tx
                        .outputs
                        .iter()
                        .map(|o| DisplayOutput {
                            value: o.value,
                            script: o.script.clone(),
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
            &json!({"height":height,"hash":display(&block.hash()),
            "previousblockhash":display(&block.parent()),
            "transactions":transactions.len(),"eligible":records.len(),
            "shielded_only":transactions.len()-records.len(),"all_txids":all_txids,"records":records}),
        )?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}
