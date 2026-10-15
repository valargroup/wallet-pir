//! Zakura JSON-RPC client.
//!
//! Deliberately a separate client from the one in `enhance-pir-server`. This
//! service must be able to run without the PIR coordinator, and depending on
//! that crate would drag in the whole iPIR stack for the sake of sixty lines of
//! JSON-RPC. The cookie handling, auth and error shapes match it on purpose.
//!
//! Blocks and transactions are fetched raw (verbosity 0) and parsed locally.
//! The verbose JSON form renders scripts through the node's own notion of what
//! is standard; raw parsing yields every script exactly as it appears on chain,
//! which is what a filter must cover.

use serde::de::DeserializeOwned;
use serde_json::json;
use std::path::Path;
use zakura_chain::block::Block;
use zakura_chain::serialization::ZcashDeserialize;
use zakura_chain::transaction::Transaction;

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
    #[error("invalid hex: {0}")]
    Hex(#[from] hex::FromHexError),
    #[error("could not parse block: {0}")]
    Block(String),
    #[error("could not parse transaction {0}: {1}")]
    Transaction(String, String),
    #[error("malformed RPC batch response: {0}")]
    Batch(String),
    #[error("RPC response: {0}")]
    Response(node_rpc::Error),
}

impl From<node_rpc::Error> for ZakuraError {
    fn from(error: node_rpc::Error) -> Self {
        match error {
            node_rpc::Error::Http(error) => Self::Http(error),
            node_rpc::Error::Cookie(error) => Self::Cookie(error),
            node_rpc::Error::InvalidCookie => Self::InvalidCookie,
            node_rpc::Error::Rpc(code, message) => Self::Rpc(code, message),
            node_rpc::Error::MissingResult => Self::MissingResult,
            node_rpc::Error::Batch(message) => Self::Batch(message),
            other => Self::Response(other),
        }
    }
}

/// The node's JSON-RPC endpoint, over the shared transport under its
/// cookie-at-construction policy.
#[derive(Clone)]
pub struct ZakuraClient {
    rpc: node_rpc::Client,
}

impl ZakuraClient {
    pub fn from_cookie_file(
        rpc_url: impl Into<String>,
        cookie_path: impl AsRef<Path>,
    ) -> Result<Self, ZakuraError> {
        Ok(Self {
            rpc: node_rpc::Client::new(
                rpc_url,
                Some(cookie_path.as_ref()),
                "transparent-filter",
                node_rpc::Policy::cookie_at_construction(),
            )?,
        })
    }

    pub async fn tip_height(&self) -> Result<u64, ZakuraError> {
        self.call("getblockcount", json!([])).await
    }

    /// Block hash at `height`, in RPC display order.
    pub async fn block_hash(&self, height: u64) -> Result<String, ZakuraError> {
        self.call("getblockhash", json!([height])).await
    }

    /// The genesis block hash, this chain's identity.
    pub async fn genesis_hash(&self) -> Result<String, ZakuraError> {
        self.block_hash(0).await
    }

    /// The full block at `height`, parsed from its canonical serialization.
    pub async fn block(&self, height: u64) -> Result<(String, Block), ZakuraError> {
        let raw_hex: String = self
            .call("getblock", json!([height.to_string(), 0]))
            .await?;
        let raw = hex::decode(raw_hex)?;
        let block = Block::zcash_deserialize(raw.as_slice())
            .map_err(|error| ZakuraError::Block(error.to_string()))?;
        let hash = block.hash().to_string();
        Ok((hash, block))
    }

    /// A transaction by id, parsed from its canonical serialization.
    ///
    /// `txid` is display hex, as the RPC expects. Returns `Ok(None)` when the
    /// node does not have the transaction, which the caller must treat as a
    /// construction failure rather than as an absent script.
    pub async fn transaction(&self, txid: &str) -> Result<Option<Transaction>, ZakuraError> {
        let raw_hex: String = match self.call("getrawtransaction", json!([txid, 0])).await {
            Ok(hex) => hex,
            // -5 is "No information available about transaction" in the
            // zcashd-compatible error set.
            Err(ZakuraError::Rpc(-5, _)) => return Ok(None),
            Err(error) => return Err(error),
        };
        parse_transaction(txid, &raw_hex).map(Some)
    }

    /// Fetches several transactions in one JSON-RPC batch.
    ///
    /// Returns results in request order, `None` where the node does not have
    /// the transaction. Batching matters: a block's previous outputs are
    /// resolved one round trip per transaction otherwise, which dominates
    /// ingest on any link with latency.
    pub async fn transactions(
        &self,
        txids: &[String],
    ) -> Result<Vec<Option<Transaction>>, ZakuraError> {
        let calls: Vec<(&str, serde_json::Value)> = txids
            .iter()
            .map(|txid| ("getrawtransaction", json!([txid, 0])))
            .collect();
        let entries = self.rpc.batch::<String>(&calls).await?;
        txids
            .iter()
            .zip(entries)
            .map(|(txid, entry)| match entry {
                Ok(raw_hex) => parse_transaction(txid, &raw_hex).map(Some),
                // -5 is "No information available about transaction".
                Err(error) if error.code == -5 => Ok(None),
                Err(error) => Err(ZakuraError::Rpc(error.code, error.message)),
            })
            .collect()
    }

    /// One JSON-RPC call whose result is returned as raw JSON.
    ///
    /// For tools that deliberately avoid this crate's block parser — the
    /// independent event spot-check reads the node's *verbose* JSON so that
    /// its extraction shares no code with the ingester's.
    pub async fn call_value(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<serde_json::Value, ZakuraError> {
        self.call(method, params).await
    }

    /// Several JSON-RPC calls in one batch, results raw and in request
    /// order, whatever order the node answered in.
    ///
    /// A call whose result is an RPC error is returned as `Err` for the whole
    /// batch, since a positional hole would be indistinguishable from a null
    /// result.
    pub async fn call_batch_values(
        &self,
        calls: &[(&str, serde_json::Value)],
    ) -> Result<Vec<serde_json::Value>, ZakuraError> {
        self.rpc
            .batch::<serde_json::Value>(calls)
            .await?
            .into_iter()
            .map(|entry| entry.map_err(|error| ZakuraError::Rpc(error.code, error.message)))
            .collect()
    }

    async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, ZakuraError> {
        Ok(self.rpc.call(method, params).await?)
    }
}

/// A transaction from its raw hex, refused unless it hashes to `txid`.
fn parse_transaction(txid: &str, raw_hex: &str) -> Result<Transaction, ZakuraError> {
    let raw = hex::decode(raw_hex)?;
    let transaction = Transaction::zcash_deserialize(raw.as_slice())
        .map_err(|error| ZakuraError::Transaction(txid.to_string(), error.to_string()))?;
    if transaction.hash().to_string() != txid.to_ascii_lowercase() {
        return Err(ZakuraError::Transaction(
            txid.to_string(),
            "returned transaction identity differs".into(),
        ));
    }
    Ok(transaction)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{routing::post, Json, Router};

    /// A node answering every batch in reverse order, with an RPC error for
    /// the call whose first parameter is `"bad"`.
    async fn reversing_node() -> String {
        let app = Router::new().route(
            "/",
            post(|Json(request): Json<serde_json::Value>| async move {
                let mut entries: Vec<serde_json::Value> = request
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|call| {
                        if call["params"][0] == "bad" {
                            json!({"jsonrpc": "2.0", "id": call["id"], "result": null,
                                "error": {"code": -8, "message": "bad"}})
                        } else {
                            json!({"jsonrpc": "2.0", "id": call["id"],
                                "result": {"echo": call["params"][0]}})
                        }
                    })
                    .collect();
                entries.reverse();
                Json(serde_json::Value::Array(entries))
            }),
        );
        let socket = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", socket.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(socket, app).await.unwrap() });
        url
    }

    /// Generic batch results come back in request order even when the node
    /// answers out of order, and one RPC error fails the whole batch.
    #[tokio::test]
    async fn generic_batches_follow_request_order() {
        let dir = tempfile::tempdir().unwrap();
        let cookie = dir.path().join(".cookie");
        std::fs::write(&cookie, "user:password").unwrap();
        let rpc = ZakuraClient::from_cookie_file(reversing_node().await, &cookie).unwrap();
        let calls: Vec<(&str, serde_json::Value)> =
            (0..5).map(|i| ("getblock", json!([i]))).collect();
        let results = rpc.call_batch_values(&calls).await.unwrap();
        let echoed: Vec<_> = results
            .iter()
            .map(|r| r["echo"].as_u64().unwrap())
            .collect();
        assert_eq!(echoed, vec![0, 1, 2, 3, 4]);
        assert!(matches!(
            rpc.call_batch_values(&[("getblock", json!([1])), ("getblock", json!(["bad"]))])
                .await,
            Err(ZakuraError::Rpc(-8, _))
        ));
        assert!(rpc.call_batch_values(&[]).await.unwrap().is_empty());
    }
}
