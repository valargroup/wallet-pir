//! Block-bounded canonical extraction; no chain-sized in-memory JSON or UTXO map.
use super::*;
use std::io::{self, BufRead, Read, Write};
use zakura_chain::transaction::Transaction;
const MAX_LINE: u64 = 256 * 1024 * 1024;
fn parse<T: ZcashDeserialize>(raw: &str) -> Result<T, AnyError> {
    let bytes = hex::decode(raw)?;
    let mut cursor = bytes.as_slice();
    let result = T::zcash_deserialize(&mut cursor)?;
    if !cursor.is_empty() {
        return Err("trailing canonical bytes".into());
    }
    Ok(result)
}
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
        let block: Block = parse(frame["raw_block"].as_str().ok_or("raw block")?)?;
        if block.hash().to_string() != frame["hash"].as_str().ok_or("block hash")? {
            return Err("raw block hash differs from RPC pin".into());
        }
        let mut previous = Previous(HashMap::new());
        for parent in frame["parents"].as_array().ok_or("parents")? {
            let tx: Transaction = parse(parent["hex"].as_str().ok_or("parent hex")?)?;
            if tx.hash().to_string() != parent["txid"].as_str().ok_or("parent txid")? {
                return Err("raw parent hash differs from requested txid".into());
            }
            for (index, out) in tx.outputs().iter().enumerate() {
                previous.0.insert(
                    OutPoint {
                        hash: tx.hash(),
                        index: index as u32,
                    },
                    out.clone(),
                );
            }
        }
        let extracted = extract::extract_block(&block.transactions, &mut previous, height)?;
        let mut records = Vec::new();
        let indexes: HashMap<_, _> = block
            .transactions
            .iter()
            .enumerate()
            .map(|(i, t)| (t.hash().0, i))
            .collect();
        for record in extracted.display {
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
        let eligible = block
            .transactions
            .iter()
            .filter(|t| !t.inputs().is_empty() || !t.outputs().is_empty())
            .count();
        if eligible != records.len() {
            return Err("eligible/display inventory mismatch".into());
        }
        serde_json::to_writer(
            &mut output,
            &json!({"height":height,"hash":block.hash().to_string(),
            "previousblockhash":block.header.previous_block_hash.to_string(),
            "transactions":block.transactions.len(),"eligible":eligible,
            "shielded_only":block.transactions.len()-eligible,"records":records}),
        )?;
        output.write_all(b"\n")?;
        output.flush()?;
    }
    Ok(())
}
