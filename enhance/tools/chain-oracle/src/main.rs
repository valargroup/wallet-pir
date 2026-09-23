//! Extract a small exact-answer oracle directly from canonical node blocks.
//! This tool neither reads the PIR journal nor calls the server's block decoder.
use clap::Parser;
use reqwest::Url;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, path::PathBuf, time::Duration};
use zakura_chain::{block::Block, serialization::ZcashDeserialize};

const RECORD_BYTES: usize = 653;
const MAX_FEE: u64 = 21_000_000 * 100_000_000;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "http://127.0.0.1:8232")]
    rpc_url: String,
    #[arg(long)]
    cookie: PathBuf,
    /// Do not extract records above this published block height.
    #[arg(long)]
    end_height: u64,
    #[arg(long, default_value_t = 128)]
    max_blocks: u64,
    #[arg(long, default_value_t = 16)]
    count: usize,
    /// New output directory; contains only public chain data and hashes.
    #[arg(long)]
    out: PathBuf,
}

#[derive(Deserialize)]
struct Rpc<T> {
    result: Option<T>,
    error: Option<Value>,
}

#[derive(Deserialize)]
struct VerboseBlock {
    trees: Trees,
}

#[derive(Deserialize)]
struct Trees {
    ironwood: Option<TreeSize>,
}

#[derive(Deserialize)]
struct TreeSize {
    size: u64,
}

#[derive(Deserialize)]
struct Health {
    protocol: String,
    anchor_height: u64,
    generation: u64,
    published_replica_counts: BTreeMap<String, u64>,
}

#[derive(Serialize)]
struct OracleRecord {
    position: u64,
    record_hex: String,
}

#[derive(Serialize)]
struct BlockEvidence {
    height: u64,
    hash: String,
    tree_size: u64,
    actions: usize,
}

async fn rpc<T: DeserializeOwned>(
    client: &reqwest::Client,
    url: &str,
    cookie: &PathBuf,
    method: &str,
    params: Value,
) -> Result<T, Box<dyn std::error::Error>> {
    let credential = std::fs::read_to_string(cookie)?;
    let (user, password) = credential
        .trim()
        .split_once(':')
        .ok_or("invalid RPC cookie")?;
    if user.is_empty() || password.is_empty() {
        return Err("invalid RPC cookie".into());
    }
    let response = client
        .post(url)
        .basic_auth(user, Some(password))
        .json(&json!({"jsonrpc":"1.0","id":"chain-oracle","method":method,"params":params}))
        .send()
        .await?
        .error_for_status()?
        .json::<Rpc<T>>()
        .await?;
    if response.error.is_some() {
        return Err(format!("node rejected {method}").into());
    }
    response
        .result
        .ok_or_else(|| format!("node omitted {method} result").into())
}

async fn published_health(client: &reqwest::Client) -> Result<Health, Box<dyn std::error::Error>> {
    let health = client
        .get("http://127.0.0.1:8080/v1/health")
        .send()
        .await?
        .error_for_status()?
        .json::<Health>()
        .await?;
    if health.protocol != "ironwood-enhance-pir-v6"
        || health.published_replica_counts.is_empty()
        || health
            .published_replica_counts
            .values()
            .any(|count| *count < 2)
    {
        return Err("coordinator is not serving two replicas".into());
    }
    Ok(health)
}

fn transaction_records(
    tx: &zakura_chain::transaction::Transaction,
) -> Result<Vec<[u8; RECORD_BYTES]>, Box<dyn std::error::Error>> {
    let mut records = Vec::new();
    let actions: Vec<_> = tx.ironwood_actions().collect();
    if actions.is_empty() {
        return Ok(records);
    }
    let transparent_in = tx.has_transparent_inputs();
    let transparent_out = tx.has_transparent_outputs();
    let pure_ironwood = !transparent_in
        && !transparent_out
        && tx.sapling_spends_per_anchor().next().is_none()
        && tx.sapling_outputs().next().is_none()
        && tx.orchard_actions().next().is_none();
    let fee = if pure_ironwood {
        let balance: i64 = tx.ironwood_value_balance().ironwood_amount().into();
        let fee = u64::try_from(balance)?;
        if fee > MAX_FEE {
            return Err("Ironwood-only fee exceeds protocol bound".into());
        }
        Some(fee)
    } else {
        None
    };
    let expiry = tx.expiry_height().map_or(0, |height| height.0);
    if expiry >= 500_000_000 {
        return Err("transaction expiry exceeds protocol bound".into());
    }
    for action in actions {
        let mut record = [0u8; RECORD_BYTES];
        let ciphertext: [u8; 580] = action.enc_ciphertext.into();
        record[..528].copy_from_slice(&ciphertext[52..]);
        let cv: [u8; 32] = action.cv.into();
        record[528..560].copy_from_slice(&cv);
        let out: [u8; 80] = action.out_ciphertext.into();
        record[560..640].copy_from_slice(&out);
        record[640] = u8::from(transparent_in)
            | (u8::from(transparent_out) << 1)
            | (u8::from(fee.is_some()) << 2);
        record[641..645].copy_from_slice(&expiry.to_le_bytes());
        record[645..653].copy_from_slice(&fee.unwrap_or(0).to_le_bytes());
        records.push(record);
    }
    Ok(records)
}

fn action_records(block: &Block) -> Result<Vec<[u8; RECORD_BYTES]>, Box<dyn std::error::Error>> {
    let mut records = Vec::new();
    for transaction in &block.transactions {
        records.extend(transaction_records(transaction)?);
    }
    Ok(records)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();
    let url = Url::parse(&args.rpc_url)?;
    if url.scheme() != "http"
        || url.host_str() != Some("127.0.0.1")
        || url.username() != ""
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
        || url.port() != Some(8232)
        || args.count == 0
        || args.count > 1024
        || args.max_blocks == 0
        || args.max_blocks > 4096
    {
        return Err("use loopback node RPC and bounded extraction limits".into());
    }
    let client = reqwest::Client::builder()
        .no_proxy()
        .timeout(Duration::from_secs(30))
        .build()?;
    let health_before = published_health(&client).await?;
    let tip: u64 = rpc(
        &client,
        &args.rpc_url,
        &args.cookie,
        "getblockcount",
        json!([]),
    )
    .await?;
    if args.end_height > tip || args.end_height > health_before.anchor_height {
        return Err("end height exceeds node tip or published anchor".into());
    }
    let mut selected = BTreeMap::new();
    let mut blocks = Vec::new();
    for height in (args.end_height.saturating_sub(args.max_blocks - 1)..=args.end_height).rev() {
        let raw_hex: String = rpc(
            &client,
            &args.rpc_url,
            &args.cookie,
            "getblock",
            json!([height.to_string(), 0]),
        )
        .await?;
        let raw = hex::decode(&raw_hex)?;
        let block = Block::zcash_deserialize(raw.as_slice())?;
        let hash = block.hash().to_string();
        let canonical_hash: String = rpc(
            &client,
            &args.rpc_url,
            &args.cookie,
            "getblockhash",
            json!([height]),
        )
        .await?;
        if hash != canonical_hash {
            return Err("canonical block hash changed during extraction".into());
        }
        let records = action_records(&block)?;
        if records.is_empty() {
            continue;
        }
        let verbose: VerboseBlock = rpc(
            &client,
            &args.rpc_url,
            &args.cookie,
            "getblock",
            json!([height.to_string(), 2]),
        )
        .await?;
        let tree_size = verbose
            .trees
            .ironwood
            .ok_or("missing Ironwood tree size")?
            .size;
        let start = tree_size
            .checked_sub(records.len() as u64)
            .ok_or("tree size below action count")?;
        if height > 0 {
            let prior: VerboseBlock = rpc(
                &client,
                &args.rpc_url,
                &args.cookie,
                "getblock",
                json!([(height - 1).to_string(), 2]),
            )
            .await?;
            if prior
                .trees
                .ironwood
                .ok_or("missing prior Ironwood tree size")?
                .size
                != start
            {
                return Err("Ironwood tree growth differs from decoded actions".into());
            }
        }
        for (offset, record) in records.iter().enumerate().rev() {
            if selected.len() == args.count {
                break;
            }
            if selected
                .insert(
                    start + offset as u64,
                    OracleRecord {
                        position: start + offset as u64,
                        record_hex: hex::encode(record),
                    },
                )
                .is_some()
            {
                return Err("duplicate chain position".into());
            }
        }
        blocks.push(BlockEvidence {
            height,
            hash,
            tree_size,
            actions: records.len(),
        });
        if selected.len() == args.count {
            break;
        }
    }
    if selected.len() != args.count {
        return Err("lookback did not contain enough Ironwood actions".into());
    }
    for block in &blocks {
        let current: String = rpc(
            &client,
            &args.rpc_url,
            &args.cookie,
            "getblockhash",
            json!([block.height]),
        )
        .await?;
        if current != block.hash {
            return Err("canonical block changed before extraction completed".into());
        }
    }
    let health_after = published_health(&client).await?;
    if health_after.anchor_height < args.end_height {
        return Err("published anchor rewound during extraction".into());
    }
    let anchor_hash: String = rpc(
        &client,
        &args.rpc_url,
        &args.cookie,
        "getblockhash",
        json!([health_after.anchor_height]),
    )
    .await?;
    let anchor_block: VerboseBlock = rpc(
        &client,
        &args.rpc_url,
        &args.cookie,
        "getblock",
        json!([health_after.anchor_height.to_string(), 2]),
    )
    .await?;
    let anchor_tree_size = anchor_block
        .trees
        .ironwood
        .ok_or("published anchor has no Ironwood tree size")?
        .size;
    let anchor_hash_recheck: String = rpc(
        &client,
        &args.rpc_url,
        &args.cookie,
        "getblockhash",
        json!([health_after.anchor_height]),
    )
    .await?;
    if anchor_hash != anchor_hash_recheck {
        return Err("published anchor changed during extraction".into());
    }
    let mut oracle = serde_json::to_vec_pretty(&selected.into_values().collect::<Vec<_>>())?;
    oracle.push(b'\n');
    let digest = hex::encode(Sha256::digest(&oracle));
    let manifest = json!({"kind":"enhance-chain-oracle-v1","qualification":"unqualified",
        "source":"canonical raw blocks from local node RPC; no PIR journal reads",
        "node_tip_at_start":tip,"published_anchor_at_start":health_before.anchor_height,
        "generation_at_start":health_before.generation,
        "published_anchor_at_end":health_after.anchor_height,
        "published_anchor_hash_at_end":anchor_hash,
        "published_anchor_tree_size_at_end":anchor_tree_size,
        "generation_at_end":health_after.generation,
        "end_height":args.end_height,"blocks":blocks,
        "record_count":args.count,"oracle_sha256":digest,
        "limitations":["Uses the same zakura-chain transaction parser as the server, but reconstructs record encoding separately.",
                       "A separate wallet release must still validate restore and recovery over public HTTPS."]});
    std::fs::create_dir(&args.out)?;
    std::fs::write(args.out.join("oracle.json"), oracle)?;
    std::fs::write(
        args.out.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest)?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use enhance_pir::{EnhanceRecord, EnhanceRecordParts, EnhanceTransactionMetadata};
    use zakura_chain::transaction::Transaction;

    #[test]
    fn canonical_transaction_matches_wallet_record_layout() {
        let bytes = hex::decode(
            include_str!(
                "../../../services/enhance-pir-server/tests/fixtures/ironwood-fee-expiry.hex"
            )
            .trim(),
        )
        .unwrap();
        let tx = Transaction::zcash_deserialize(bytes.as_slice()).unwrap();
        let records = transaction_records(&tx).unwrap();
        assert!(!records.is_empty());
        let metadata = EnhanceTransactionMetadata::new(3_483_371, Some(10_000)).unwrap();
        for (action, actual) in tx.ironwood_actions().zip(records) {
            let ciphertext: [u8; 580] = action.enc_ciphertext.into();
            let expected = EnhanceRecord::from_parts(EnhanceRecordParts {
                enc_ciphertext_suffix: ciphertext[52..].try_into().unwrap(),
                cv_net: action.cv.into(),
                out_ciphertext: action.out_ciphertext.into(),
                has_transparent_inputs: false,
                has_transparent_outputs: false,
                metadata,
            });
            assert_eq!(actual.as_slice(), expected.as_bytes());
        }
    }
}
