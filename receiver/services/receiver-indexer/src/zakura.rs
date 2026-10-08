//! A minimal JSON-RPC client for a Zakura node: what the indexer reads.
//!
//! It copies the parts of `enhance-pir-server`'s client the indexer needs, so the two
//! services stay independent until a shared node client exists.
use reqwest::StatusCode;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::json;
use std::path::Path;

/// A failed node request.
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

/// Nodes' JSON-RPC endpoints, tried in order, and their credentials, if they require
/// any.
#[derive(Clone)]
pub struct ZakuraClient {
    http: reqwest::Client,
    rpc_urls: Vec<String>,
    credentials: Option<(String, String)>,
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

/// The note commitment tree sizes after a block.
#[derive(Deserialize)]
pub(crate) struct BlockTrees {
    pub(crate) ironwood: Option<TreeSize>,
}

/// One note commitment tree's size.
#[derive(Deserialize)]
pub(crate) struct TreeSize {
    pub(crate) size: u64,
}

impl ZakuraClient {
    /// A client that authenticates with the node's `user:password` cookie file.
    pub fn from_cookie_file(
        rpc_urls: Vec<String>,
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
        Self::with_credentials(rpc_urls, Some((username.to_string(), password.to_string())))
    }

    /// A client for explicitly selected nodes whose RPC disables authentication.
    pub fn unauthenticated(rpc_urls: Vec<String>) -> Result<Self, ZakuraError> {
        Self::with_credentials(rpc_urls, None)
    }

    /// See [`Self::from_cookie_file`] and [`Self::unauthenticated`].
    fn with_credentials(
        rpc_urls: Vec<String>,
        credentials: Option<(String, String)>,
    ) -> Result<Self, ZakuraError> {
        if rpc_urls.is_empty() {
            return Err(ZakuraError::Block("no node RPC endpoint".into()));
        }
        Ok(Self {
            http: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(5))
                .timeout(std::time::Duration::from_secs(120))
                .build()?,
            rpc_urls,
            credentials,
        })
    }

    /// The node's chain tip height.
    pub async fn tip_height(&self) -> Result<u64, ZakuraError> {
        self.call("getblockcount", json!([])).await
    }

    /// The hash of the node's block at `height`, in RPC display order.
    pub async fn block_hash(&self, height: u64) -> Result<String, ZakuraError> {
        self.call("getblockhash", json!([height])).await
    }

    /// One JSON-RPC call to the first node that answers it. Callers validate answers
    /// against each other (batch links and anchors), so nodes may differ between calls.
    pub(crate) async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, ZakuraError> {
        let mut last = None;
        for url in &self.rpc_urls {
            match self.call_at(url, method, &params).await {
                Ok(result) => return Ok(result),
                Err(error) => last = Some(error),
            }
        }
        Err(last.expect("at least one node RPC endpoint"))
    }

    /// One JSON-RPC call to `url`, authenticated unless the node disables
    /// authentication.
    async fn call_at<T: DeserializeOwned>(
        &self,
        url: &str,
        method: &str,
        params: &serde_json::Value,
    ) -> Result<T, ZakuraError> {
        let mut request = self.http.post(url);
        if let Some((username, password)) = &self.credentials {
            request = request.basic_auth(username, Some(password));
        }
        let response = request
            .json(&json!({"jsonrpc": "1.0", "id": "receiver-indexer", "method": method, "params": params}))
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
