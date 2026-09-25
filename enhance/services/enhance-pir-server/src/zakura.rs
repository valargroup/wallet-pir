use enhance_pir::{EnhanceRecord, EnhanceRecordParts};
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::path::Path;
use zakura_chain::block::Block;
use zakura_chain::serialization::ZcashDeserialize;

use crate::types::ACTIVATION_HEIGHT;

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
}

#[derive(Clone)]
pub struct ZakuraClient {
    http: reqwest::Client,
    rpc_url: String,
    username: String,
    password: String,
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

#[derive(Deserialize)]
struct VerboseBlock {
    trees: BlockTrees,
}

#[derive(Deserialize)]
struct BlockTrees {
    ironwood: Option<TreeSize>,
}

#[derive(Deserialize)]
struct TreeSize {
    size: u64,
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
            username: username.to_string(),
            password: password.to_string(),
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

    pub async fn block(&self, height: u64) -> Result<CanonicalBlock, ZakuraError> {
        let raw_hex: String = self
            .call("getblock", json!([height.to_string(), 0]))
            .await?;
        let raw = hex::decode(raw_hex)?;
        let block = Block::zcash_deserialize(raw.as_slice())
            .map_err(|error| ZakuraError::Block(error.to_string()))?;
        let hash = block.hash().to_string();
        let mut records = Vec::new();
        for transaction in &block.transactions {
            if transaction.ironwood_actions().next().is_none() {
                continue;
            }
            let has_transparent_inputs = transaction.has_transparent_inputs();
            let has_transparent_outputs = transaction.has_transparent_outputs();
            let metadata = transaction_metadata(transaction)?;
            records.extend(transaction.ironwood_actions().map(|action| {
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
            }));
        }
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

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, ZakuraError> {
        let response = self
            .http
            .post(&self.rpc_url)
            .basic_auth(&self.username, Some(&self.password))
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

/// Derive metadata from canonical transaction data, never from a fee estimate.
fn transaction_metadata(
    transaction: &zakura_chain::transaction::Transaction,
) -> Result<enhance_pir::EnhanceTransactionMetadata, ZakuraError> {
    let has_transparent_inputs = transaction.has_transparent_inputs();
    let has_transparent_outputs = transaction.has_transparent_outputs();
    let pure_ironwood = !has_transparent_inputs
        && !has_transparent_outputs
        && transaction.sapling_spends_per_anchor().next().is_none()
        && transaction.sapling_outputs().next().is_none()
        && transaction.orchard_actions().next().is_none();
    let fee = if pure_ironwood {
        let value: i64 = transaction
            .ironwood_value_balance()
            .ironwood_amount()
            .into();
        Some(
            u64::try_from(value)
                .map_err(|_| ZakuraError::Block("negative Ironwood-only fee".into()))?,
        )
    } else {
        None
    };
    // For Ironwood's v6 format, None from this accessor means encoded zero.
    let expiry = transaction.expiry_height().map_or(0, |height| height.0);
    enhance_pir::EnhanceTransactionMetadata::new(expiry, fee)
        .map_err(|error| ZakuraError::Block(error.to_string()))
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
        let metadata = transaction_metadata(&tx).unwrap();
        assert_eq!(metadata.expiry_height(), 3483371);
        assert_eq!(metadata.fee_zatoshis(), Some(10000));
    }
}
