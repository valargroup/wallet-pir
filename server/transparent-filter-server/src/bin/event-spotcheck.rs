//! Re-derives sampled blocks' events from the node's verbose RPC and compares
//! them with the journal.
//!
//! Independence is the point. The ingester parses raw blocks with the chain
//! crate's deserializer and resolves previous outputs from the node's RocksDB;
//! this tool reads the node's *verbose JSON* — `getblock` for the transaction
//! list, `getrawtransaction … 1` for outputs, inputs and coinbase status — and
//! applies the indexing rule again from its description: every nonempty output
//! script that does not begin with `OP_RETURN` is a receive; every non-coinbase
//! input is a spend indexed under the script of the output it consumes, subject
//! to the same rule. No extraction code is shared with the ingester.
//!
//! A block is compared as a multiset of events, each with its script and every
//! field the journal stores, so a wrong value, a swapped index or a missing
//! spend all surface as differences rather than as an equal count.
//!
//! Sampling picks the journal's densest blocks (consolidation blocks, where
//! events are dominated by inputs), a seeded random selection and the last
//! blocks, so the early chain, the recent chain and spend-heavy blocks are all
//! represented. Explicit heights can be added.

use clap::Parser;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::path::PathBuf;
use transparent_events::TransparentEvent;
use transparent_filter::ScriptBytes;
use transparent_filter_server::events::EventStore;
use transparent_filter_server::zakura::ZakuraClient;

type BoxError = Box<dyn std::error::Error + Send + Sync>;

#[derive(Parser)]
#[command(
    name = "event-spotcheck",
    about = "Compare sampled journal blocks against an independent RPC extraction"
)]
struct Cli {
    #[arg(long, default_value = "./transparent-event-data")]
    data_dir: PathBuf,
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    zakura_rpc_url: String,
    #[arg(long)]
    zakura_cookie: PathBuf,
    /// Explicit heights, comma separated.
    #[arg(long, value_delimiter = ',')]
    heights: Vec<u64>,
    /// The journal's densest blocks by event count.
    #[arg(long, default_value_t = 3)]
    top: usize,
    /// Seeded random heights across the journal.
    #[arg(long, default_value_t = 5)]
    random: usize,
    #[arg(long, default_value_t = 1)]
    seed: u64,
    /// The journal's last blocks.
    #[arg(long, default_value_t = 3)]
    last: usize,
    /// Only blocks at or below this height are sampled (the anchor).
    #[arg(long)]
    through: Option<u64>,
    /// Verbose transactions fetched per RPC batch.
    ///
    /// The ingester's own batch size against this node. Larger batches have
    /// been refused as a whole.
    #[arg(long, default_value_t = transparent_filter_server::prevout::PREVOUT_BATCH)]
    batch: usize,
    #[arg(long)]
    out: Option<PathBuf>,
    #[arg(long)]
    source_sha: Option<String>,
}

/// One event as the journal stores it, in a form that sorts and compares.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, serde::Serialize)]
struct Line {
    script: String,
    kind: &'static str,
    height: u32,
    transaction_index: u16,
    txid: String,
    index: u32,
    value: u64,
    coinbase: bool,
    spent_txid: String,
    spent_index: u32,
}

fn journal_line(script: &ScriptBytes, event: &TransparentEvent) -> Line {
    match event {
        TransparentEvent::Receive(receive) => Line {
            script: hex::encode(script.as_slice()),
            kind: "receive",
            height: receive.height,
            transaction_index: receive.transaction_index,
            txid: receive.txid.to_display_hex(),
            index: receive.output_index,
            value: receive.value,
            coinbase: receive.coinbase,
            spent_txid: String::new(),
            spent_index: 0,
        },
        TransparentEvent::Spend(spend) => Line {
            script: hex::encode(script.as_slice()),
            kind: "spend",
            height: spend.height,
            transaction_index: spend.transaction_index,
            txid: spend.spending_txid.to_display_hex(),
            index: spend.input_index,
            value: 0,
            coinbase: false,
            spent_txid: spend.spent_txid.to_display_hex(),
            spent_index: spend.spent_output_index,
        },
    }
}

/// The indexing rule, restated: nonempty and not `OP_RETURN` (0x6a) first.
fn indexable(script: &[u8]) -> bool {
    !script.is_empty() && script[0] != 0x6a
}

/// A verbose transaction's parts this check needs.
struct VerboseTx {
    txid: String,
    coinbase: bool,
    inputs: Vec<(String, u32)>,
    outputs: Vec<(Vec<u8>, u64)>,
}

fn parse_verbose(value: &serde_json::Value) -> Result<VerboseTx, BoxError> {
    let txid = value
        .get("txid")
        .and_then(serde_json::Value::as_str)
        .ok_or("verbose transaction without txid")?
        .to_string();
    let vin = value
        .get("vin")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{txid}: no vin array"))?;
    let coinbase = vin.iter().any(|input| input.get("coinbase").is_some());
    let mut inputs = Vec::new();
    if !coinbase {
        for input in vin {
            let previous = input
                .get("txid")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("{txid}: input without txid"))?;
            let index = input
                .get("vout")
                .and_then(serde_json::Value::as_u64)
                .ok_or_else(|| format!("{txid}: input without vout"))?;
            inputs.push((previous.to_string(), u32::try_from(index)?));
        }
    }
    let vout = value
        .get("vout")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| format!("{txid}: no vout array"))?;
    let mut outputs = Vec::new();
    for (position, output) in vout.iter().enumerate() {
        let n = output
            .get("n")
            .and_then(serde_json::Value::as_u64)
            .ok_or_else(|| format!("{txid}: output without n"))?;
        if n != position as u64 {
            return Err(format!("{txid}: output {position} reports n={n}").into());
        }
        let script_hex = output
            .pointer("/scriptPubKey/hex")
            .and_then(serde_json::Value::as_str)
            .ok_or_else(|| format!("{txid}: output {n} without scriptPubKey.hex"))?;
        let value = match output.get("valueZat").or_else(|| output.get("valueSat")) {
            Some(zat) => zat
                .as_u64()
                .ok_or_else(|| format!("{txid}: output {n} has a non-integer valueZat"))?,
            None => {
                // Only a decimal ZEC figure: scale and round. Every amount on
                // the chain is an integer below 2^53 zatoshi, so the rounding
                // recovers it exactly.
                let zec = output
                    .get("value")
                    .and_then(serde_json::Value::as_f64)
                    .ok_or_else(|| format!("{txid}: output {n} without value"))?;
                (zec * 1e8).round() as u64
            }
        };
        outputs.push((hex::decode(script_hex)?, value));
    }
    Ok(VerboseTx {
        txid,
        coinbase,
        inputs,
        outputs,
    })
}

/// The node's code for a batch whose answer would exceed its response limit.
const BATCH_RESPONSE_TOO_LARGE: i64 = -32011;

async fn verbose_transactions(
    client: &ZakuraClient,
    txids: &[String],
    batch: usize,
) -> Result<Vec<VerboseTx>, BoxError> {
    let mut all = Vec::with_capacity(txids.len());
    // Verbose transactions vary from a few hundred bytes to hundreds of
    // kilobytes, so a fixed batch size is either slow or refused. Start at
    // the configured size and halve on the node's too-large refusal; the
    // answers are the same however they are batched.
    let mut size = batch.max(1);
    let mut start = 0;
    while start < txids.len() {
        let end = (start + size).min(txids.len());
        let calls: Vec<(&str, serde_json::Value)> = txids[start..end]
            .iter()
            .map(|txid| ("getrawtransaction", serde_json::json!([txid, 1])))
            .collect();
        match client.call_batch_values(&calls).await {
            Ok(values) => {
                for value in values {
                    all.push(parse_verbose(&value)?);
                }
                start = end;
            }
            Err(transparent_filter_server::zakura::ZakuraError::Rpc(
                BATCH_RESPONSE_TOO_LARGE,
                _,
            )) if size > 1 => {
                size /= 2;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(all)
}

/// Events at `height` as the node's verbose interface describes them.
async fn node_lines(
    client: &ZakuraClient,
    height: u64,
    batch: usize,
) -> Result<(String, Vec<Line>, u64), BoxError> {
    let block = client
        .call_value("getblock", serde_json::json!([height.to_string(), 1]))
        .await?;
    let hash = block
        .get("hash")
        .and_then(serde_json::Value::as_str)
        .ok_or("getblock without hash")?
        .to_string();
    let txids: Vec<String> = block
        .get("tx")
        .and_then(serde_json::Value::as_array)
        .ok_or("getblock without tx")?
        .iter()
        .map(|value| {
            value
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "getblock tx entry is not a txid".into())
        })
        .collect::<Result<_, BoxError>>()?;
    let transactions = verbose_transactions(client, &txids, batch).await?;

    // Outputs created in this block, then every earlier transaction the
    // block's inputs reach back to.
    let mut scripts: HashMap<(String, u32), Vec<u8>> = HashMap::new();
    for tx in &transactions {
        for (index, (script, _)) in tx.outputs.iter().enumerate() {
            scripts.insert((tx.txid.clone(), index as u32), script.clone());
        }
    }
    let wanted: BTreeSet<String> = transactions
        .iter()
        .flat_map(|tx| tx.inputs.iter())
        .filter(|(txid, index)| !scripts.contains_key(&(txid.clone(), *index)))
        .map(|(txid, _)| txid.clone())
        .collect();
    let wanted: Vec<String> = wanted.into_iter().collect();
    let lookups = wanted.len() as u64;
    for tx in verbose_transactions(client, &wanted, batch).await? {
        for (index, (script, _)) in tx.outputs.iter().enumerate() {
            scripts.insert((tx.txid.clone(), index as u32), script.clone());
        }
    }

    let height32 = u32::try_from(height)?;
    let mut lines = Vec::new();
    for (transaction_index, tx) in transactions.iter().enumerate() {
        let transaction_index = u16::try_from(transaction_index)?;
        for (output_index, (script, value)) in tx.outputs.iter().enumerate() {
            if !indexable(script) {
                continue;
            }
            lines.push(Line {
                script: hex::encode(script),
                kind: "receive",
                height: height32,
                transaction_index,
                txid: tx.txid.clone(),
                index: output_index as u32,
                value: *value,
                coinbase: tx.coinbase,
                spent_txid: String::new(),
                spent_index: 0,
            });
        }
        for (input_index, (spent_txid, spent_index)) in tx.inputs.iter().enumerate() {
            let script = scripts
                .get(&(spent_txid.clone(), *spent_index))
                .ok_or_else(|| {
                    format!(
                        "{}: previous output {spent_txid}:{spent_index} not found",
                        tx.txid
                    )
                })?;
            if !indexable(script) {
                continue;
            }
            lines.push(Line {
                script: hex::encode(script),
                kind: "spend",
                height: height32,
                transaction_index,
                txid: tx.txid.clone(),
                index: input_index as u32,
                value: 0,
                coinbase: false,
                spent_txid: spent_txid.clone(),
                spent_index: *spent_index,
            });
        }
    }
    Ok((hash, lines, lookups))
}

/// Multiset difference: what is in `a` but not `b`, honouring repetition.
fn only_in(a: &[Line], b: &[Line]) -> Vec<Line> {
    let mut counts: BTreeMap<&Line, i64> = BTreeMap::new();
    for line in a {
        *counts.entry(line).or_default() += 1;
    }
    for line in b {
        *counts.entry(line).or_default() -= 1;
    }
    counts
        .into_iter()
        .filter(|(_, count)| *count > 0)
        .flat_map(|(line, count)| std::iter::repeat_n(line.clone(), count as usize))
        .collect()
}

/// A small deterministic generator, so a sample is reproducible from its seed
/// without adding a dependency for it.
struct Lcg(u64);

impl Lcg {
    fn next(&mut self) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        self.0 >> 11
    }
}

fn sample_heights(store: &EventStore, cli: &Cli) -> Result<Vec<u64>, BoxError> {
    let start = store.start_height();
    let covered = store.covered_through().ok_or("the journal is empty")?;
    let through = cli.through.unwrap_or(covered).min(covered);
    let mut chosen: BTreeSet<u64> = cli.heights.iter().copied().collect();
    for height in &chosen {
        if *height < start || *height > through {
            return Err(format!("height {height} is outside {start}-{through}").into());
        }
    }
    // Densest blocks: one pass over the block table, keeping the top few.
    let mut densest: Vec<(u64, u64)> = Vec::new();
    for height in start..=through {
        let count = store
            .block_at(height)
            .map(|entry| entry.event_count)
            .unwrap_or(0);
        if densest.len() < cli.top {
            densest.push((count, height));
            densest.sort_unstable_by(|a, b| b.cmp(a));
        } else if let Some(last) = densest.last() {
            if count > last.0 {
                densest.pop();
                densest.push((count, height));
                densest.sort_unstable_by(|a, b| b.cmp(a));
            }
        }
    }
    chosen.extend(densest.iter().map(|(_, height)| *height));
    let mut lcg = Lcg(cli.seed ^ 0x9E37_79B9_7F4A_7C15);
    let span = through - start + 1;
    for _ in 0..cli.random {
        chosen.insert(start + lcg.next() % span);
    }
    for height in (start..=through).rev().take(cli.last) {
        chosen.insert(height);
    }
    Ok(chosen.into_iter().collect())
}

#[tokio::main]
async fn main() -> Result<(), BoxError> {
    let cli = Cli::parse();
    let store = EventStore::open_existing(&cli.data_dir)?;
    let client = ZakuraClient::from_cookie_file(&cli.zakura_rpc_url, &cli.zakura_cookie)?;
    let genesis = client.genesis_hash().await?;
    if genesis != store.genesis_hash() {
        return Err(format!(
            "the node is on genesis {genesis}, the journal on {}",
            store.genesis_hash()
        )
        .into());
    }
    let heights = sample_heights(&store, &cli)?;

    let mut blocks = Vec::new();
    let mut failures = 0usize;
    for height in &heights {
        let entry = store
            .block_at(*height)
            .ok_or_else(|| format!("height {height} is missing from the journal"))?;
        let journal: Vec<Line> = store
            .events_at(*height)?
            .unwrap_or_default()
            .iter()
            .map(|(script, event)| journal_line(script, event))
            .collect();
        let started = std::time::Instant::now();
        let (node_hash, node, lookups) = node_lines(&client, *height, cli.batch).await?;
        let journal_hash = entry.block_hash.to_display_hex();
        let missing = only_in(&node, &journal);
        let extra = only_in(&journal, &node);
        let receives = node.iter().filter(|line| line.kind == "receive").count();
        let agrees = node_hash == journal_hash && missing.is_empty() && extra.is_empty();
        if !agrees {
            failures += 1;
        }
        eprintln!(
            "height {height}: journal {} events, node {} ({} receives, {} spends), \
             {} missing, {} extra, hash {} in {:.1}s",
            journal.len(),
            node.len(),
            receives,
            node.len() - receives,
            missing.len(),
            extra.len(),
            if node_hash == journal_hash {
                "agrees"
            } else {
                "DIFFERS"
            },
            started.elapsed().as_secs_f64()
        );
        blocks.push(serde_json::json!({
            "height": height,
            "journal_hash": journal_hash,
            "node_hash": node_hash,
            "journal_events": journal.len(),
            "node_events": node.len(),
            "node_receives": receives,
            "node_spends": node.len() - receives,
            "previous_transactions_fetched": lookups,
            "missing_from_journal": missing.iter().take(20).collect::<Vec<_>>(),
            "missing_count": missing.len(),
            "extra_in_journal": extra.iter().take(20).collect::<Vec<_>>(),
            "extra_count": extra.len(),
            "agrees": agrees,
            "seconds": started.elapsed().as_secs_f64(),
        }));
    }

    let record = serde_json::json!({
        "schema": "transparent-event-spotcheck-v1",
        "generated_at": chrono::Utc::now().to_rfc3339(),
        "tool_sha": cli.source_sha,
        "method": "node verbose getblock/getrawtransaction JSON, indexing rule reapplied; \
                   shares no extraction code with the ingester",
        "data_dir": cli.data_dir,
        "genesis_hash": genesis,
        "journal": {
            "start_height": store.start_height(),
            "covered_through": store.covered_through(),
        },
        "sample": {
            "explicit": cli.heights,
            "top": cli.top,
            "random": cli.random,
            "seed": cli.seed,
            "last": cli.last,
            "through": cli.through,
            "heights": heights,
        },
        "blocks": blocks,
        "blocks_compared": heights.len(),
        "blocks_disagreeing": failures,
    });
    let bytes = serde_json::to_vec_pretty(&record)?;
    match &cli.out {
        Some(path) => std::fs::write(path, &bytes)?,
        None => println!("{}", String::from_utf8(bytes)?),
    }
    if failures > 0 {
        return Err(format!(
            "{failures} of {} sampled blocks disagree with the independent extraction",
            heights.len()
        )
        .into());
    }
    eprintln!("{} blocks agree exactly", heights.len());
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rule_indexes_every_nonempty_script_except_op_return() {
        assert!(!indexable(&[]));
        assert!(!indexable(&[0x6a]));
        assert!(!indexable(&[0x6a, 0x04, 1, 2, 3, 4]));
        assert!(indexable(&[0x76, 0xa9]));
        assert!(indexable(&[0x00]));
    }

    #[test]
    fn multiset_difference_keeps_repetition() {
        let a = Line {
            script: "aa".into(),
            kind: "receive",
            height: 1,
            transaction_index: 0,
            txid: "t".into(),
            index: 0,
            value: 5,
            coinbase: false,
            spent_txid: String::new(),
            spent_index: 0,
        };
        let twice = vec![a.clone(), a.clone()];
        let once = vec![a.clone()];
        assert_eq!(only_in(&twice, &once), vec![a.clone()]);
        assert!(only_in(&once, &twice).is_empty());
    }

    #[test]
    fn verbose_values_prefer_the_integer_field() {
        let value: serde_json::Value = serde_json::from_str(
            r#"{"txid":"ab","vin":[{"coinbase":"00"}],
                "vout":[{"n":0,"value":0.1,"valueZat":10000000,"scriptPubKey":{"hex":"76a9"}},
                        {"n":1,"value":0.3,"scriptPubKey":{"hex":"6a"}}]}"#,
        )
        .unwrap();
        let tx = parse_verbose(&value).unwrap();
        assert!(tx.coinbase);
        assert!(tx.inputs.is_empty());
        assert_eq!(tx.outputs[0], (vec![0x76, 0xa9], 10_000_000));
        assert_eq!(tx.outputs[1], (vec![0x6a], 30_000_000));
    }
}
