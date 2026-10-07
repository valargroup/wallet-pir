use enhance_pir::{EnhanceRecord, EnhanceRecordParts};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use zakura_chain::amount::{Amount, NonNegative};
use zakura_chain::block::Block;
use zakura_chain::serialization::ZcashDeserialize;
use zakura_chain::transaction::{self, Transaction};
use zakura_chain::transparent::{Input, OutPoint, Utxo};

use crate::fee::{self, FeeError, OutputCache};
use crate::types::ACTIVATION_HEIGHT;

/// Previous transactions requested per JSON-RPC batch.
pub const PREVOUT_BATCH: usize = 256;

#[derive(Debug, thiserror::Error)]
pub enum ZakuraError {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cookie error: {0}")]
    Cookie(#[from] std::io::Error),
    #[error("invalid RPC cookie")]
    InvalidCookie,
    #[error("RPC returned {0}: {1}")]
    Rpc(i64, String),
    #[error("RPC response is missing a result")]
    MissingResult,
    #[error("invalid block hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("invalid canonical block: {0}")]
    Block(String),
    #[error("Ironwood tree size is unavailable at height {0}")]
    MissingTreeSize(u64),
    #[error("invalid JSON-RPC batch response: {0}")]
    Batch(String),
    #[error("node has no transaction {0}")]
    MissingTransaction(String),
    #[error("invalid transaction {0}: {1}")]
    Transaction(String, String),
    #[error("spent output {outpoint} is outside its transaction's {outputs} outputs")]
    PrevoutIndex { outpoint: String, outputs: usize },
    #[error("fee of transaction {0} is unavailable: {1}")]
    Fee(String, FeeError),
}

/// Spent-output resolution shared across blocks: a bounded cache and counters.
pub struct Prevouts {
    cache: Mutex<OutputCache>,
    counts: PrevoutCounters,
}

#[derive(Default)]
struct PrevoutCounters {
    same_block_hits: AtomicU64,
    cache_hits: AtomicU64,
    fetched_transactions: AtomicU64,
    batches: AtomicU64,
}

/// Cumulative spent-output resolution counts.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PrevoutCounts {
    pub same_block_hits: u64,
    pub cache_hits: u64,
    pub fetched_transactions: u64,
    pub batches: u64,
}

impl Prevouts {
    /// A cache holding at most about `outputs` transparent outputs.
    pub fn new(outputs: usize) -> Self {
        Self {
            cache: Mutex::new(OutputCache::new(outputs)),
            counts: PrevoutCounters::default(),
        }
    }

    pub fn counts(&self) -> PrevoutCounts {
        PrevoutCounts {
            same_block_hits: self.counts.same_block_hits.load(Ordering::Relaxed),
            cache_hits: self.counts.cache_hits.load(Ordering::Relaxed),
            fetched_transactions: self.counts.fetched_transactions.load(Ordering::Relaxed),
            batches: self.counts.batches.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone)]
pub struct ZakuraClient {
    http: reqwest::Client,
    rpc_url: String,
    credentials: Option<(String, String)>,
}

#[derive(Debug)]
pub struct CanonicalBlock {
    pub height: u64,
    pub hash: String,
    pub records: Vec<EnhanceRecord>,
    pub tree_size: u64,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcError>,
}

#[derive(Deserialize)]
struct RpcError {
    code: i64,
    message: String,
}

/// The tree sizes from a verbose `getblock` response.
#[derive(Deserialize)]
pub(crate) struct VerboseBlock {
    pub(crate) trees: BlockTrees,
}

#[derive(Deserialize)]
pub(crate) struct BlockTrees {
    pub(crate) ironwood: Option<TreeSize>,
}

#[derive(Deserialize)]
pub(crate) struct TreeSize {
    pub(crate) size: u64,
}

impl ZakuraClient {
    pub fn from_cookie_file(
        rpc_url: impl Into<String>,
        cookie_path: impl AsRef<Path>,
    ) -> Result<Self, ZakuraError> {
        let cookie = std::fs::read_to_string(cookie_path)?;
        let (username, password) = cookie
            .trim()
            .split_once(':')
            .ok_or(ZakuraError::InvalidCookie)?;
        if username.is_empty() || password.is_empty() {
            return Err(ZakuraError::InvalidCookie);
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()?,
            rpc_url: rpc_url.into(),
            credentials: Some((username.to_string(), password.to_string())),
        })
    }

    /// Connect to an explicitly selected canonical RPC that disables authentication.
    pub fn unauthenticated(rpc_url: impl Into<String>) -> Result<Self, ZakuraError> {
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(std::time::Duration::from_secs(120))
                .build()?,
            rpc_url: rpc_url.into(),
            credentials: None,
        })
    }

    pub async fn tip_height(&self) -> Result<u64, ZakuraError> {
        self.call("getblockcount", json!([])).await
    }

    pub async fn block_hash(&self, height: u64) -> Result<String, ZakuraError> {
        self.call("getblockhash", json!([height])).await
    }

    pub async fn tree_size(&self, height: u64) -> Result<u64, ZakuraError> {
        let block: VerboseBlock = self
            .call("getblock", json!([height.to_string(), 2]))
            .await?;
        block
            .trees
            .ironwood
            .map(|tree| tree.size)
            .ok_or(ZakuraError::MissingTreeSize(height))
    }

    /// The block's Enhance records and the node's Ironwood tree size after it.
    ///
    /// Fails, rather than publishing an absent or partial fee, when a spent
    /// output cannot be resolved; the caller retries the block later.
    pub async fn block(
        &self,
        height: u64,
        prevouts: &Prevouts,
    ) -> Result<CanonicalBlock, ZakuraError> {
        let (hash, records) = self.block_records(height, prevouts).await?;
        let tree_size = if height >= ACTIVATION_HEIGHT {
            self.tree_size(height).await?
        } else {
            0
        };
        Ok(CanonicalBlock {
            height,
            hash,
            records,
            tree_size,
        })
    }

    /// The parsed block's hash and Enhance records, without the tree size.
    pub async fn block_records(
        &self,
        height: u64,
        prevouts: &Prevouts,
    ) -> Result<(String, Vec<EnhanceRecord>), ZakuraError> {
        let raw_hex: String = self
            .call("getblock", json!([height.to_string(), 0]))
            .await?;
        let raw = hex::decode(raw_hex)?;
        let block = Block::zcash_deserialize(raw.as_slice())
            .map_err(|error| ZakuraError::Block(error.to_string()))?;
        let records = self.records(&block, prevouts).await?;
        Ok((block.hash().to_string(), records))
    }

    /// Records of every Ironwood action in block order.
    ///
    /// Spent outputs are resolved only for non-coinbase transactions with
    /// Ironwood actions and transparent inputs: from this block, then the
    /// cross-block cache, then one batched `getrawtransaction` per 256 missing
    /// transactions. A block without such a transaction issues no extra RPC.
    pub async fn records(
        &self,
        block: &Block,
        prevouts: &Prevouts,
    ) -> Result<Vec<EnhanceRecord>, ZakuraError> {
        let wanted: Vec<OutPoint> = block
            .transactions
            .iter()
            .filter(|tx| tx.ironwood_actions().next().is_some() && fee::needs_prevouts(tx))
            .flat_map(|tx| tx.inputs().iter().filter_map(Input::outpoint))
            .collect();
        let mut values: HashMap<OutPoint, Amount<NonNegative>> = HashMap::new();
        if !wanted.is_empty() {
            let in_block: HashMap<_, _> = block
                .transactions
                .iter()
                .map(|tx| (tx.hash(), tx.as_ref()))
                .collect();
            let mut missing = Vec::new();
            let mut requested = std::collections::HashSet::new();
            {
                let cache = prevouts.cache.lock().expect("prevout cache");
                for outpoint in &wanted {
                    if let Some(tx) = in_block.get(&outpoint.hash) {
                        values.insert(*outpoint, output_value(outpoint, tx.outputs())?);
                        prevouts
                            .counts
                            .same_block_hits
                            .fetch_add(1, Ordering::Relaxed);
                    } else if let Some(value) = cache.get(outpoint) {
                        values.insert(*outpoint, value);
                        prevouts.counts.cache_hits.fetch_add(1, Ordering::Relaxed);
                    } else if requested.insert(outpoint.hash) {
                        missing.push(outpoint.hash);
                    }
                }
            }
            for chunk in missing.chunks(PREVOUT_BATCH) {
                let fetched = self.transactions(chunk).await?;
                prevouts.counts.batches.fetch_add(1, Ordering::Relaxed);
                prevouts
                    .counts
                    .fetched_transactions
                    .fetch_add(fetched.len() as u64, Ordering::Relaxed);
                let mut cache = prevouts.cache.lock().expect("prevout cache");
                for tx in &fetched {
                    for outpoint in wanted.iter().filter(|o| o.hash == tx.hash()) {
                        values.insert(*outpoint, output_value(outpoint, tx.outputs())?);
                    }
                    cache.insert_transaction(tx);
                }
            }
        }
        let mut records = Vec::new();
        for transaction in &block.transactions {
            let utxos = transaction
                .inputs()
                .iter()
                .filter_map(Input::outpoint)
                .filter_map(|o| values.get(&o).map(|v| (o, fee::spent_output(*v))))
                .collect();
            records.extend(transaction_records(transaction, &utxos)?);
        }
        let mut cache = prevouts.cache.lock().expect("prevout cache");
        for transaction in &block.transactions {
            cache.insert_transaction(transaction);
        }
        Ok(records)
    }

    /// Display-order hashes of `heights`, in JSON-RPC 2.0 batches.
    pub async fn block_hashes(&self, heights: &[u64]) -> Result<Vec<String>, ZakuraError> {
        let mut hashes = Vec::with_capacity(heights.len());
        for chunk in heights.chunks(PREVOUT_BATCH) {
            for entry in self
                .batch::<String>("getblockhash", chunk.iter().map(|h| json!([h])).collect())
                .await?
            {
                hashes.push(entry.map_err(|(code, message)| ZakuraError::Rpc(code, message))?);
            }
        }
        Ok(hashes)
    }

    /// Transactions by id in one JSON-RPC 2.0 batch, each identity verified.
    ///
    /// A transaction the node does not have (-5) is an error, never an
    /// absent input value.
    pub async fn transactions(
        &self,
        txids: &[transaction::Hash],
    ) -> Result<Vec<Transaction>, ZakuraError> {
        let entries = self
            .batch::<String>(
                "getrawtransaction",
                txids
                    .iter()
                    .map(|txid| json!([txid.to_string(), 0]))
                    .collect(),
            )
            .await?;
        txids
            .iter()
            .zip(entries)
            .map(|(txid, entry)| {
                let raw_hex = entry.map_err(|(code, message)| match code {
                    -5 => ZakuraError::MissingTransaction(txid.to_string()),
                    _ => ZakuraError::Rpc(code, message),
                })?;
                let raw = hex::decode(raw_hex)?;
                let tx = Transaction::zcash_deserialize(raw.as_slice()).map_err(|error| {
                    ZakuraError::Transaction(txid.to_string(), error.to_string())
                })?;
                if tx.hash() != *txid {
                    return Err(ZakuraError::Transaction(
                        txid.to_string(),
                        "returned transaction identity differs".into(),
                    ));
                }
                Ok(tx)
            })
            .collect()
    }

    /// One JSON-RPC 2.0 batch, results placed by their echoed id.
    ///
    /// The node rejects 1.0 batches, so batches use 2.0 while single calls
    /// stay on 1.0. Per-entry RPC errors are returned to the caller; a
    /// malformed batch response is an error for the whole batch.
    async fn batch<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Vec<serde_json::Value>,
    ) -> Result<Vec<Result<T, (i64, String)>>, ZakuraError> {
        if params.is_empty() {
            return Ok(Vec::new());
        }
        let requests: Vec<_> = params
            .into_iter()
            .enumerate()
            .map(|(id, params)| json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params}))
            .collect();
        let response = self.post().json(&requests).send().await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(ZakuraError::InvalidCookie);
        }
        let entries = match response
            .error_for_status()?
            .json::<serde_json::Value>()
            .await?
        {
            serde_json::Value::Array(entries) => entries,
            other => {
                // A node refusing the whole batch answers with one error object.
                return Err(
                    match other.pointer("/error/code").and_then(|c| c.as_i64()) {
                        Some(code) => ZakuraError::Rpc(
                            code,
                            other
                                .pointer("/error/message")
                                .and_then(|m| m.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        ),
                        None => ZakuraError::Batch("batch response is not an array".into()),
                    },
                );
            }
        };
        if entries.len() != requests.len() {
            return Err(ZakuraError::Batch(format!(
                "asked for {} results, got {}",
                requests.len(),
                entries.len()
            )));
        }
        let mut placed: Vec<Option<Result<T, (i64, String)>>> =
            (0..requests.len()).map(|_| None).collect();
        for entry in entries {
            let id = entry
                .get("id")
                .and_then(|id| id.as_u64())
                .and_then(|id| usize::try_from(id).ok())
                .filter(|id| *id < placed.len())
                .ok_or_else(|| {
                    ZakuraError::Batch("batch entry id is missing or out of range".into())
                })?;
            if placed[id].is_some() {
                return Err(ZakuraError::Batch(format!("batch id {id} appeared twice")));
            }
            let response: RpcResponse<T> = serde_json::from_value(entry)
                .map_err(|error| ZakuraError::Batch(format!("batch entry {id}: {error}")))?;
            placed[id] = Some(match (response.error, response.result) {
                (Some(error), _) => Err((error.code, error.message)),
                (None, Some(result)) => Ok(result),
                (None, None) => {
                    return Err(ZakuraError::Batch(format!(
                        "batch entry {id} has no result"
                    )))
                }
            });
        }
        Ok(placed
            .into_iter()
            .map(|entry| entry.expect("every id placed"))
            .collect())
    }

    /// Read every transaction type for the Status index and verify the raw
    /// block against the node's canonical hash at this height.
    pub async fn status_block(
        &self,
        height: u64,
    ) -> Result<crate::status::index::Block, ZakuraError> {
        let raw_hex: String = self
            .call("getblock", json!([height.to_string(), 0]))
            .await?;
        let raw = hex::decode(raw_hex)?;
        let block = Block::zcash_deserialize(raw.as_slice())
            .map_err(|error| ZakuraError::Block(error.to_string()))?;
        let hash = block.hash();
        let canonical: zakura_chain::block::Hash = self
            .block_hash(height)
            .await?
            .parse()
            .map_err(|error| ZakuraError::Block(format!("invalid canonical hash: {error}")))?;
        if hash != canonical {
            return Err(ZakuraError::Block("raw block is not canonical".into()));
        }
        Ok(crate::status::index::Block {
            height: u32::try_from(height)
                .map_err(|_| ZakuraError::Block("height exceeds Status encoding".into()))?,
            hash: hash.0,
            parent: block.header.previous_block_hash.0,
            txids: block.transactions.iter().map(|tx| tx.hash().0).collect(),
        })
    }

    /// RPC display-order txids are parsed into protocol byte order before
    /// hashing into Status buckets.
    pub async fn status_mempool(&self) -> Result<Vec<[u8; 32]>, ZakuraError> {
        let displayed: Vec<String> = self.call("getrawmempool", json!([false])).await?;
        displayed
            .into_iter()
            .map(|id| {
                id.parse::<zakura_chain::transaction::Hash>()
                    .map(|hash| hash.0)
                    .map_err(|error| ZakuraError::Block(format!("invalid mempool txid: {error}")))
            })
            .collect()
    }

    /// A POST to the node, authenticated unless the RPC disables authentication.
    fn post(&self) -> reqwest::RequestBuilder {
        let request = self.http.post(&self.rpc_url);
        match &self.credentials {
            Some((username, password)) => request.basic_auth(username, Some(password)),
            None => request,
        }
    }

    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, ZakuraError> {
        let response = self
            .post()
            .json(
                &json!({"jsonrpc": "1.0", "id": "enhance-pir", "method": method, "params": params}),
            )
            .send()
            .await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            return Err(ZakuraError::InvalidCookie);
        }
        let response: RpcResponse<T> = response.error_for_status()?.json().await?;
        if let Some(error) = response.error {
            return Err(ZakuraError::Rpc(error.code, error.message));
        }
        response.result.ok_or(ZakuraError::MissingResult)
    }
}

/// One record per Ironwood action, with the transaction's exact fee.
///
/// `utxos` must hold every output a non-coinbase transaction spends; see
/// [`fee::transaction_fee`]. Transactions without Ironwood actions yield none.
pub fn transaction_records(
    transaction: &Transaction,
    utxos: &HashMap<OutPoint, Utxo>,
) -> Result<Vec<EnhanceRecord>, ZakuraError> {
    if transaction.ironwood_actions().next().is_none() {
        return Ok(Vec::new());
    }
    let has_transparent_inputs = transaction.has_transparent_inputs();
    let has_transparent_outputs = transaction.has_transparent_outputs();
    let metadata = transaction_metadata(transaction, utxos)?;
    Ok(transaction
        .ironwood_actions()
        .map(|action| {
            EnhanceRecord::from_parts(EnhanceRecordParts {
                enc_ciphertext_suffix: <[u8; 580]>::from(action.enc_ciphertext)[52..]
                    .try_into()
                    .expect("fixed ciphertext suffix"),
                cv_net: action.cv.into(),
                out_ciphertext: action.out_ciphertext.into(),
                has_transparent_inputs,
                has_transparent_outputs,
                metadata,
            })
        })
        .collect())
}

/// Derive metadata from canonical transaction data, never from a fee estimate.
fn transaction_metadata(
    transaction: &Transaction,
    utxos: &HashMap<OutPoint, Utxo>,
) -> Result<enhance_pir::EnhanceTransactionMetadata, ZakuraError> {
    let fee = fee::transaction_fee(transaction, utxos)
        .map_err(|error| ZakuraError::Fee(transaction.hash().to_string(), error))?;
    // For Ironwood's v6 format, None from this accessor means encoded zero.
    let expiry = transaction.expiry_height().map_or(0, |height| height.0);
    enhance_pir::EnhanceTransactionMetadata::new(expiry, fee)
        .map_err(|error| ZakuraError::Block(error.to_string()))
}

fn output_value(
    outpoint: &OutPoint,
    outputs: &[zakura_chain::transparent::Output],
) -> Result<Amount<NonNegative>, ZakuraError> {
    outputs
        .get(outpoint.index as usize)
        .map(|output| output.value)
        .ok_or_else(|| ZakuraError::PrevoutIndex {
            outpoint: fee::outpoint_label(outpoint),
            outputs: outputs.len(),
        })
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn canonical_ironwood_transaction_has_actual_fee_and_expiry() {
        let bytes =
            hex::decode(include_str!("../tests/fixtures/ironwood-fee-expiry.hex").trim()).unwrap();
        let tx =
            zakura_chain::transaction::Transaction::zcash_deserialize(bytes.as_slice()).unwrap();
        assert_eq!(
            tx.hash().to_string(),
            "f337d9675817668120ae626021f61e5350ff5412beeb5e42b67bd412452b8d13"
        );
        let metadata = transaction_metadata(&tx, &HashMap::new()).unwrap();
        assert_eq!(metadata.expiry_height(), 3483371);
        assert_eq!(metadata.fee_zatoshis(), Some(10000));
    }
}
