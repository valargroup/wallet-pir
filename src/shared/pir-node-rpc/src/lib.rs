//! Bounded JSON-RPC transport to a Zakura node.
//!
//! This crate authenticates, frames, bounds and times out node calls. It never
//! decodes blocks or transactions: chain interpretation stays in
//! `zakura-server-chain`, which builds its `NodeClient` on this transport, and the
//! root publisher's chain follower uses it directly.
use reqwest::StatusCode;
use serde::Deserialize;
use serde::de::DeserializeOwned;
use serde_json::json;
use std::path::{Path, PathBuf};
use std::time::Duration;

/// A failed call: transport, credential, framing or JSON-RPC errors only.
#[derive(Debug, thiserror::Error)]
pub enum RpcError {
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
    #[error("response is larger than {0} bytes")]
    Oversized(usize),
    #[error("invalid RPC response: {0}")]
    Decode(serde_json::Error),
    #[error("invalid JSON-RPC batch response: {0}")]
    Batch(String),
}

/// Deadlines and the whole-response bound for every call, singles and batches.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct HttpPolicy {
    pub connection_timeout: Option<Duration>,
    pub request_timeout: Duration,
    pub response_limit: usize,
}

impl HttpPolicy {
    /// No separate connection deadline, 120 s per call and 64 MiB per response,
    /// including whole batches: the policy of every read-once-cookie client.
    pub const SHARED: Self = Self {
        connection_timeout: None,
        request_timeout: Duration::from_secs(120),
        response_limit: 64 * 1024 * 1024,
    };
}

/// How a client authenticates.
#[derive(Clone, Debug)]
pub enum Credentials {
    /// A node whose RPC disables authentication.
    None,
    /// A cookie file reread on every request, since the node rotates it when it
    /// restarts.
    CookieFile(PathBuf),
    /// Credentials read once, at construction; a refusal is [`RpcError::InvalidCookie`].
    Fixed { username: String, password: String },
}

impl Credentials {
    /// The credentials the cookie file at `path` holds now.
    pub fn read_once(path: &Path) -> Result<Self, RpcError> {
        let (username, password) = read_cookie(path)?;
        Ok(Self::Fixed { username, password })
    }
}

/// The `user` and `password` of a cookie file.
fn read_cookie(path: &Path) -> Result<(String, String), RpcError> {
    let cookie = std::fs::read_to_string(path)?;
    match cookie.trim().split_once(':') {
        Some((username, password)) if !username.is_empty() && !password.is_empty() => {
            Ok((username.to_owned(), password.to_owned()))
        }
        _ => Err(RpcError::InvalidCookie),
    }
}

/// One node's JSON-RPC endpoint and how to authenticate to it.
#[derive(Clone)]
pub struct RpcClient {
    http: reqwest::Client,
    url: String,
    credentials: Credentials,
    limit: usize,
    /// The JSON-RPC request ID, which names the caller in node logs.
    id: &'static str,
}

#[derive(Deserialize)]
struct RpcResponse<T> {
    result: Option<T>,
    error: Option<RpcErrorObject>,
}

#[derive(Deserialize)]
struct RpcErrorObject {
    code: i64,
    message: String,
}

/// `response`'s body, refused as soon as it declares or reaches more than `limit`
/// bytes, so an oversized body is never buffered past the limit.
async fn read_limited(mut response: reqwest::Response, limit: usize) -> Result<Vec<u8>, RpcError> {
    if response
        .content_length()
        .is_some_and(|length| length > limit as u64)
    {
        return Err(RpcError::Oversized(limit));
    }
    let mut body = Vec::new();
    while let Some(chunk) = response.chunk().await? {
        if body.len() + chunk.len() > limit {
            return Err(RpcError::Oversized(limit));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

impl RpcClient {
    /// A client for `url` naming itself `id`, bounded by `policy`.
    pub fn new(
        url: String,
        credentials: Credentials,
        id: &'static str,
        policy: HttpPolicy,
    ) -> Result<Self, RpcError> {
        let mut http = reqwest::Client::builder().timeout(policy.request_timeout);
        if let Some(timeout) = policy.connection_timeout {
            http = http.connect_timeout(timeout);
        }
        Ok(Self {
            http: http.build()?,
            url,
            credentials,
            limit: policy.response_limit,
            id,
        })
    }

    /// This client, naming itself `id` in its JSON-RPC requests, as the node logs.
    pub fn with_request_id(mut self, id: &'static str) -> Self {
        self.id = id;
        self
    }

    /// The endpoint this client calls.
    pub fn url(&self) -> &str {
        &self.url
    }

    /// The node's chain tip height.
    pub async fn tip_height(&self) -> Result<u64, RpcError> {
        self.call("getblockcount", json!([])).await
    }

    /// The hash of the node's block at `height`, in RPC display order.
    pub async fn block_hash_display(&self, height: u64) -> Result<String, RpcError> {
        self.call("getblockhash", json!([height])).await
    }

    /// The request builder for one POST to the node, with its credentials.
    fn post(&self) -> Result<reqwest::RequestBuilder, RpcError> {
        let request = self.http.post(&self.url);
        Ok(match &self.credentials {
            Credentials::None => request,
            Credentials::CookieFile(path) => {
                let (username, password) = read_cookie(path)?;
                request.basic_auth(username, Some(password))
            }
            Credentials::Fixed { username, password } => {
                request.basic_auth(username, Some(password))
            }
        })
    }

    /// Sends `request` and reads its body within this client's bound, refusing a
    /// non-success status.
    async fn send(&self, request: reqwest::RequestBuilder) -> Result<Vec<u8>, RpcError> {
        let response = request.send().await?;
        if matches!(self.credentials, Credentials::Fixed { .. })
            && response.status() == StatusCode::UNAUTHORIZED
        {
            return Err(RpcError::InvalidCookie);
        }
        let response = response.error_for_status()?;
        read_limited(response, self.limit).await
    }

    /// One JSON-RPC 1.0 call to the node.
    pub async fn call<T: DeserializeOwned>(
        &self,
        method: &str,
        params: serde_json::Value,
    ) -> Result<T, RpcError> {
        let body = json!({"jsonrpc": "1.0", "id": self.id, "method": method, "params": params});
        let body = self.send(self.post()?.json(&body)).await?;
        let response: RpcResponse<T> = serde_json::from_slice(&body).map_err(RpcError::Decode)?;
        if let Some(error) = response.error {
            return Err(RpcError::Rpc(error.code, error.message));
        }
        response.result.ok_or(RpcError::MissingResult)
    }

    /// One JSON-RPC 2.0 batch of calls of any methods, each given as
    /// `{"method": ..., "params": ...}`, results placed by their echoed id.
    ///
    /// The node rejects 1.0 batches, so batches use 2.0 while single calls
    /// stay on 1.0. Per-entry RPC errors are returned to the caller; a
    /// malformed batch response is an error for the whole batch.
    pub async fn batch_mixed<T: DeserializeOwned>(
        &self,
        calls: Vec<serde_json::Value>,
    ) -> Result<Vec<Result<T, (i64, String)>>, RpcError> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        let requests: Vec<_> = calls
            .into_iter()
            .enumerate()
            .map(|(id, call)| {
                json!({"jsonrpc": "2.0", "id": id, "method": call["method"], "params": call["params"]})
            })
            .collect();
        let body = self.send(self.post()?.json(&requests)).await?;
        let value: serde_json::Value = serde_json::from_slice(&body).map_err(RpcError::Decode)?;
        let entries = match value {
            serde_json::Value::Array(entries) => entries,
            other => {
                // A node refusing the whole batch answers with one error object.
                return Err(
                    match other.pointer("/error/code").and_then(|c| c.as_i64()) {
                        Some(code) => RpcError::Rpc(
                            code,
                            other
                                .pointer("/error/message")
                                .and_then(|m| m.as_str())
                                .unwrap_or_default()
                                .to_string(),
                        ),
                        None => RpcError::Batch("batch response is not an array".into()),
                    },
                );
            }
        };
        if entries.len() != requests.len() {
            return Err(RpcError::Batch(format!(
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
                    RpcError::Batch("batch entry id is missing or out of range".into())
                })?;
            if placed[id].is_some() {
                return Err(RpcError::Batch(format!("batch id {id} appeared twice")));
            }
            let response: RpcResponse<T> = serde_json::from_value(entry)
                .map_err(|error| RpcError::Batch(format!("batch entry {id}: {error}")))?;
            placed[id] = Some(match (response.error, response.result) {
                (Some(error), _) => Err((error.code, error.message)),
                (None, Some(result)) => Ok(result),
                (None, None) => {
                    return Err(RpcError::Batch(format!("batch entry {id} has no result")));
                }
            });
        }
        Ok(placed
            .into_iter()
            .map(|entry| entry.expect("every id placed"))
            .collect())
    }

    /// [`Self::batch_mixed`] for calls of one method.
    pub async fn batch<T: DeserializeOwned>(
        &self,
        method: &str,
        params: Vec<serde_json::Value>,
    ) -> Result<Vec<Result<T, (i64, String)>>, RpcError> {
        self.batch_mixed(
            params
                .into_iter()
                .map(|params| json!({"method": method, "params": params}))
                .collect(),
        )
        .await
    }
}

#[cfg(test)]
mod tests;
