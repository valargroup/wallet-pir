//! Analysis-only sequential disk UTXO census, using the production extractor.
use super::*;
use rusqlite::{params, Connection, OptionalExtension};
use std::io::{self, BufRead, Write};

fn point_key(p: &OutPoint) -> Vec<u8> {
    let mut key = p.0.to_vec();
    key.extend_from_slice(&p.1.to_le_bytes());
    key
}
struct DiskPrevious<'a>(&'a Connection);
impl DiskPrevious<'_> {
    /// The stored output at `p`, in its consensus encoding.
    fn get(&self, p: &OutPoint) -> Result<Option<Output>, AnyError> {
        let raw: Option<Vec<u8>> = self
            .0
            .query_row("SELECT raw FROM utxo WHERE point=?", [point_key(p)], |r| {
                r.get(0)
            })
            .optional()?;
        raw.map(|r| Output::from_consensus_bytes(&r).map_err(Into::into))
            .transpose()
    }
}
impl extract::PreviousOutputs for DiskPrevious<'_> {
    fn previous_output(&mut self, txid: [u8; 32], index: u32) -> Result<Option<Output>, AnyError> {
        self.get(&(txid, index))
    }
}
fn leb_len(mut n: u64) -> usize {
    let mut len = 1;
    while n >= 128 {
        n >>= 7;
        len += 1;
    }
    len
}
fn script_size(raw: &[u8]) -> (usize, &'static str) {
    if raw.len() == 25 && raw[..3] == [0x76, 0xa9, 0x14] && raw[23..] == [0x88, 0xac] {
        (21, "p2pkh")
    } else if raw.len() == 23 && raw[..2] == [0xa9, 0x14] && raw[22] == 0x87 {
        (21, "p2sh")
    } else if raw.len() == 35 && raw[0] == 33 && [2, 3].contains(&raw[1]) && raw[34] == 0xac {
        (34, "compressed_p2pk")
    } else if raw.len() == 67 && raw[..2] == [0x41, 0x04] && raw[66] == 0xac {
        (66, "uncompressed_p2pk")
    } else {
        (1 + leb_len(raw.len() as u64) + raw.len(), "raw_escape")
    }
}
fn stats(
    record: &zakura_transparent_private_server::shard::txid_v1::TransparentDisplayRecord,
    missing: usize,
) -> Result<Value, AnyError> {
    let display = record.encode()?;
    let ni = record.metadata.transparent_input_count as u64;
    let no = record.outputs.len() as u64;
    let fee_len = match record.metadata.fee {
        FeeState::Exact(v) => leb_len(v),
        _ => 0,
    };
    let mut compressed = 1 + leb_len(ni) + fee_len + leb_len(no);
    let mut scripts = std::collections::BTreeMap::new();
    let mut empty = 0;
    let mut op_return = 0;
    for out in &record.outputs {
        let (size, kind) = script_size(&out.script);
        compressed += leb_len(out.value) + size;
        *scripts.entry(kind).or_insert(0usize) += 1;
        empty += usize::from(out.script.is_empty());
        op_return += usize::from(out.script.first() == Some(&0x6a));
    }
    let common = compressed - leb_len(ni) - leb_len(no)
        + 1
        + if ni >= 15 { leb_len(ni) } else { 0 }
        + if no >= 15 { leb_len(no) } else { 0 };
    let fee = match record.metadata.fee {
        FeeState::Exact(v) => json!(v),
        FeeState::Unknown => json!("unknown"),
        FeeState::NotApplicable => json!("not_applicable"),
    };
    Ok(
        json!({"txid_internal":hex::encode(record.txid.0),"coinbase":record.coinbase,
        "input_count":ni,"output_count":no,"shielded_components":record.metadata.has_shielded_components,
        "fee":fee,"missing_prevouts":missing,"empty_scripts":empty,"op_return_scripts":op_return,"scripts":scripts,
        "sizes":[display.len(),display.len()-1,compressed,common]}),
    )
}
pub fn run(path: &str) -> Result<(), AnyError> {
    let db = Connection::open(path)?;
    db.execute_batch("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA cache_size=-131072;
        CREATE TABLE IF NOT EXISTS utxo(point BLOB PRIMARY KEY, raw BLOB NOT NULL) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS txids(txid BLOB PRIMARY KEY,height INTEGER NOT NULL) WITHOUT ROWID;
        CREATE TABLE IF NOT EXISTS blocks(height INTEGER PRIMARY KEY,hash TEXT NOT NULL,parent TEXT NOT NULL,raw_sha256 TEXT NOT NULL,summary TEXT NOT NULL);")?;
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let frame: Value = serde_json::from_str(&line?)?;
        let height = u32::try_from(frame["height"].as_u64().ok_or("height")?)?;
        let raw = hex::decode(frame["raw_block"].as_str().ok_or("raw block")?)?;
        let raw_hash = hex::encode(Sha256::digest(&raw));
        let block = read_block(&raw)?;
        let hash = display(&block.hash());
        let parent = display(&block.parent());
        if let Some(expected) = frame["hash"].as_str() {
            if expected != hash {
                return Err("raw hash differs from pin".into());
            }
        }
        if block.coinbase_height() != Some(height) {
            return Err("canonical coinbase height differs".into());
        }
        let old: Option<(String, String)> = db
            .query_row(
                "SELECT raw_sha256,summary FROM blocks WHERE height=?",
                [height],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((old_hash, summary)) = old {
            if old_hash != raw_hash {
                return Err("checkpoint raw identity mismatch".into());
            }
            writeln!(stdout, "{summary}")?;
            stdout.flush()?;
            continue;
        }
        let tip: Option<(u32, String)> = db
            .query_row(
                "SELECT height,hash FROM blocks ORDER BY height DESC LIMIT 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()?;
        if let Some((previous_height, previous_hash)) = tip {
            if height != previous_height + 1 || parent != previous_hash {
                return Err("chain continuity mismatch".into());
            }
        } else if height != 0
            || parent != "0".repeat(64)
            || hash != "00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08"
        {
            return Err("mainnet genesis identity mismatch".into());
        }
        let transactions = block.transactions();
        if transactions.is_empty() {
            return Err("empty mainnet block".into());
        }
        let tx = db.unchecked_transaction()?;
        let mut records = Vec::new();
        let mut shielded_only = 0;
        let mut all_txids = Vec::new();
        for (index, transaction) in transactions.iter().enumerate() {
            let txid = transaction.txid;
            tx.execute(
                "INSERT INTO txids VALUES(?,?)",
                params![txid.as_slice(), height],
            )?;
            all_txids.push(hex::encode(txid));
            let coinbase = transaction.coinbase;
            if coinbase != (index == 0) {
                return Err("canonical coinbase position mismatch".into());
            }
            let mut spent = std::collections::HashSet::new();
            let mut missing = 0;
            for outpoint in &transaction.spends {
                if !spent.insert(*outpoint) {
                    return Err("duplicate transparent input".into());
                }
                if DiskPrevious(&tx).get(outpoint)?.is_none() {
                    missing += 1;
                }
            }
            if coinbase || !transaction.spends.is_empty() || !transaction.outputs.is_empty() {
                let record = if missing == 0 {
                    let read =
                        block.transparent_transaction(index, height, &mut DiskPrevious(&tx))?;
                    extract::index(&[read], height, true)?
                        .display
                        .into_iter()
                        .next()
                        .ok_or("missing eligible record")?
                        .record
                } else {
                    // Extraction refuses partial filters. Census preserves eligibility
                    // with the shared explicitly unknown fee state; no invented fee.
                    zakura_transparent_private_server::shard::txid_v1::TransparentDisplayRecord {
                        txid: zakura_transparent_private_shared::events::Txid(txid),
                        coinbase,
                        metadata: zakura_transparent_private_shared::events::TransactionMetadata {
                            fee: if coinbase {
                                FeeState::NotApplicable
                            } else {
                                FeeState::Unknown
                            },
                            transparent_input_count: u32::try_from(spent.len())?,
                            has_shielded_components: transaction.has_shielded_components,
                        },
                        outputs: transaction
                            .outputs
                            .iter()
                            .map(|o| zakura_transparent_private_server::shard::txid::DisplayOutput {
                                value: o.value,
                                script: o.script.clone(),
                            })
                            .collect(),
                    }
                };
                records.push(stats(&record, missing)?);
            } else {
                shielded_only += 1;
            }
            for point in spent {
                tx.execute("DELETE FROM utxo WHERE point=?", [point_key(&point)])?;
            }
            // Canonical state excludes the unspendable genesis UTXO; its display
            // record and exact raw output are still included in this inventory.
            if height > 0 {
                for (index, output) in transaction.outputs.iter().enumerate() {
                    let encoded = output.consensus_bytes()?;
                    tx.execute(
                        "INSERT INTO utxo VALUES(?,?)",
                        params![
                            point_key(&(transaction.txid, u32::try_from(index)?)),
                            encoded
                        ],
                    )?;
                }
            }
        }
        let summary = serde_json::to_string(
            &json!({"height":height,"hash":hash,"parent":parent,"raw_sha256":raw_hash,
            "raw_bytes":raw.len(),"transactions":transactions.len(),"eligible":records.len(),"shielded_only":shielded_only,"all_txids":all_txids,"records":records}),
        )?;
        tx.execute(
            "INSERT INTO blocks VALUES(?,?,?,?,?)",
            params![height, hash, parent, raw_hash, summary],
        )?;
        tx.commit()?;
        writeln!(stdout, "{summary}")?;
        stdout.flush()?;
    }
    db.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn compact_size_oracle_preserves_raw_scripts_and_count_escapes() {
        use zakura_transparent_private_server::shard::txid::DisplayOutput;
        use zakura_transparent_private_server::shard::txid_v1::TransparentDisplayRecord;
        for ni in [0, 14, 15, 128] {
            for no in [0, 1, 14, 15, 128] {
                let record = TransparentDisplayRecord {
                    txid: zakura_transparent_private_shared::events::Txid([0; 32]),
                    coinbase: false,
                    metadata: zakura_transparent_private_shared::events::TransactionMetadata {
                        fee: FeeState::Exact(0),
                        transparent_input_count: ni,
                        has_shielded_components: false,
                    },
                    outputs: (0..no)
                        .map(|_| DisplayOutput {
                            value: 0,
                            script: vec![0x51],
                        })
                        .collect(),
                };
                let actual = stats(&record, 0).unwrap();
                // Raw script proposal adds a tag; compact count header is
                // explicitly reconstructed, independently of stats' subtraction.
                let expected = 2
                    + if ni >= 15 { leb_len(ni as u64) } else { 0 }
                    + if no >= 15 { leb_len(no) } else { 0 }
                    + 1
                    + no as usize * 4;
                assert_eq!(actual["sizes"][3].as_u64().unwrap() as usize, expected);
                assert_eq!(actual["fee"], json!(0));
            }
        }
        assert_eq!(script_size(&[0x6a, 0]), (4, "raw_escape"));
        assert_eq!(script_size(&[]), (2, "raw_escape"));
    }
}
