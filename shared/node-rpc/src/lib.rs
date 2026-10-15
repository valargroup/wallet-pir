//! The JSON-RPC transport the Enhance, Transparent and Receiver services use to
//! read a Zakura node: authentication from the node's cookie file, deadlines, a
//! bound on every response, typed single calls and strictly checked batches.
//!
//! Only transport lives here. What a product asks for, how it parses blocks and
//! transactions, which chain it trusts and how it maps errors stay with the
//! product. Each product chooses a [`Policy`]:
//!
//! - Enhance and Transparent read the cookie once, when the client is built;
//!   allow 120 s for a whole request with no separate connect deadline; accept
//!   at most [`WHOLE_RESPONSE_BYTES`] of response; and report a 401 as
//!   [`Error::InvalidCookie`]. The 64 MiB bound is deliberately a whole-response
//!   bound, far above any block or batch they read, so a misbehaving endpoint
//!   cannot make them buffer without limit.
//! - Receiver re-reads the cookie on every request, since the node rotates it
//!   when it restarts; allows 120 s per request and 20 s to connect; bounds a
//!   response by what its largest read can be, derived from the chain's limits;
//!   and reports a 401 as the HTTP status error it always has.
//!
//! A single call is JSON-RPC 1.0 under the product's fixed id, as the node and
//! zcashd expect. A batch is JSON-RPC 2.0, since the node refuses 1.0 batches,
//! with integer ids `0..n`; its answer must hold exactly one response per id,
//! each placed back in request order whatever order the node used. There are no
//! retries, no splitting and no fallback: a failure is the caller's to handle.

use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use std::time::Duration;

/// The response bound Enhance and Transparent use: a whole-response limit, not a
/// per-field one.
pub const WHOLE_RESPONSE_BYTES: usize = 64 << 20;

/// When the cookie file is read.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CookieReading {
    /// Once, when the client is built.
    AtConstruction,
    /// Before every request, so a rotated or replaced cookie is followed.
    EveryRequest,
}

/// How a 401 is reported.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unauthorized {
    /// As [`Error::InvalidCookie`].
    InvalidCookie,
    /// As the HTTP status error ([`Error::Http`]).
    HttpStatus,
}

/// A product's transport settings.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Policy {
    pub cookie: CookieReading,
    /// The whole request, response body included.
    pub total: Duration,
    /// Establishing the connection, when bounded separately.
    pub connect: Option<Duration>,
    /// Most response body bytes accepted.
    pub response_bytes: usize,
    pub unauthorized: Unauthorized,
}

impl Policy {
    /// Enhance's and Transparent's policy.
    pub const fn cookie_at_construction() -> Self {
        Self {
            cookie: CookieReading::AtConstruction,
            total: Duration::from_secs(120),
            connect: None,
            response_bytes: WHOLE_RESPONSE_BYTES,
            unauthorized: Unauthorized::InvalidCookie,
        }
    }

    /// Receiver's policy, bounding responses at `response_bytes`.
    pub const fn cookie_every_request(response_bytes: usize) -> Self {
        Self {
            cookie: CookieReading::EveryRequest,
            total: Duration::from_secs(120),
            connect: Some(Duration::from_secs(20)),
            response_bytes,
            unauthorized: Unauthorized::HttpStatus,
        }
    }
}

/// A failed node request.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("HTTP error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("cookie error: {0}")]
    Cookie(#[from] std::io::Error),
    #[error("invalid RPC cookie")]
    InvalidCookie,
    #[error("response is larger than {0} bytes")]
    Oversized(usize),
    #[error("invalid RPC response: {0}")]
    Decode(serde_json::Error),
    #[error("RPC returned {0}: {1}")]
    Rpc(i64, String),
    #[error("RPC response is missing a result")]
    MissingResult,
    #[error("invalid JSON-RPC batch response: {0}")]
    Batch(String),
}

/// One batch entry's RPC error.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct RpcError {
    pub code: i64,
    pub message: String,
}

#[derive(Clone)]
enum Credentials {
    None,
    Fixed(String, String),
    File(PathBuf),
}

/// One node's JSON-RPC endpoint under a [`Policy`]. Cheap to clone: clones
/// share one connection pool.
#[derive(Clone)]
pub struct Client(std::sync::Arc<Inner>);

struct Inner {
    http: reqwest::Client,
    url: String,
    id: &'static str,
    credentials: Credentials,
    policy: Policy,
}

/// The `user` and `password` of a cookie file.
fn read_cookie(path: &Path) -> Result<(String, String), Error> {
    let cookie = std::fs::read_to_string(path)?;
    match cookie.trim().split_once(':') {
        Some((user, password)) if !user.is_empty() && !password.is_empty() => {
            Ok((user.to_owned(), password.to_owned()))
        }
        _ => Err(Error::InvalidCookie),
    }
}

#[derive(Deserialize)]
struct Response {
    #[serde(default)]
    result: Option<Value>,
    #[serde(default)]
    error: Option<RpcError>,
}

impl Client {
    /// A client for the node at `url` that names its single calls `id` and
    /// authenticates with the cookie file at `cookie`, or sends no credentials
    /// for `None`. Under [`CookieReading::AtConstruction`] the cookie is read
    /// here, and a missing or malformed one fails now.
    pub fn new(
        url: impl Into<String>,
        cookie: Option<&Path>,
        id: &'static str,
        policy: Policy,
    ) -> Result<Self, Error> {
        let credentials = match (cookie, policy.cookie) {
            (None, _) => Credentials::None,
            (Some(path), CookieReading::AtConstruction) => {
                let (user, password) = read_cookie(path)?;
                Credentials::Fixed(user, password)
            }
            (Some(path), CookieReading::EveryRequest) => Credentials::File(path.to_owned()),
        };
        let mut http = reqwest::Client::builder().timeout(policy.total);
        if let Some(connect) = policy.connect {
            http = http.connect_timeout(connect);
        }
        Ok(Self(std::sync::Arc::new(Inner {
            http: http.build()?,
            url: url.into(),
            id,
            credentials,
            policy,
        })))
    }

    pub fn url(&self) -> &str {
        &self.0.url
    }

    pub fn policy(&self) -> Policy {
        self.0.policy
    }

    /// One JSON-RPC 1.0 call, its result decoded as `T`.
    pub async fn call<T: DeserializeOwned>(&self, method: &str, params: Value) -> Result<T, Error> {
        let body = json!({"jsonrpc": "1.0", "id": self.0.id, "method": method, "params": params});
        let response: Response =
            serde_json::from_slice(&self.post(&body).await?).map_err(Error::Decode)?;
        if let Some(error) = response.error {
            return Err(Error::Rpc(error.code, error.message));
        }
        match response.result {
            None | Some(Value::Null) => Err(Error::MissingResult),
            Some(result) => serde_json::from_value(result).map_err(Error::Decode),
        }
    }

    /// One JSON-RPC 2.0 batch of `calls`, answered in request order: each
    /// entry's result decoded as `T`, or its RPC error.
    ///
    /// The answer must be an array holding exactly one response per issued id,
    /// in any order; a missing, repeated, unexpected, negative or non-integer id
    /// fails the whole batch, as does an entry with neither a result nor an
    /// error. A node refusing the batch as a whole answers with one error
    /// object, reported as [`Error::Rpc`].
    pub async fn batch<T: DeserializeOwned>(
        &self,
        calls: &[(&str, Value)],
    ) -> Result<Vec<Result<T, RpcError>>, Error> {
        if calls.is_empty() {
            return Ok(Vec::new());
        }
        let requests: Vec<Value> = calls
            .iter()
            .enumerate()
            .map(|(id, (method, params))| {
                json!({"jsonrpc": "2.0", "id": id, "method": method, "params": params})
            })
            .collect();
        let raw: Value = serde_json::from_slice(&self.post(&Value::Array(requests)).await?)
            .map_err(Error::Decode)?;
        let entries = match raw {
            Value::Array(entries) => entries,
            other => {
                return Err(
                    match other
                        .get("error")
                        .cloned()
                        .map(serde_json::from_value::<RpcError>)
                    {
                        Some(Ok(error)) => Error::Rpc(error.code, error.message),
                        _ => Error::Batch("the batch response is not an array".into()),
                    },
                )
            }
        };
        if entries.len() != calls.len() {
            return Err(Error::Batch(format!(
                "asked for {} results, got {}",
                calls.len(),
                entries.len()
            )));
        }
        let mut placed: Vec<Option<Result<T, RpcError>>> = (0..calls.len()).map(|_| None).collect();
        for entry in entries {
            let id = match entry.get("id") {
                Some(Value::Number(id)) => id.as_u64(),
                _ => None,
            }
            .and_then(|id| usize::try_from(id).ok())
            .ok_or_else(|| {
                Error::Batch(format!(
                    "batch entry id {:?} is not a request id",
                    entry.get("id")
                ))
            })?;
            let slot = placed
                .get_mut(id)
                .ok_or_else(|| Error::Batch(format!("batch id {id} was not issued")))?;
            if slot.is_some() {
                return Err(Error::Batch(format!("batch id {id} appeared twice")));
            }
            let response: Response = serde_json::from_value(entry)
                .map_err(|error| Error::Batch(format!("batch entry {id}: {error}")))?;
            *slot = Some(match (response.error, response.result) {
                (Some(error), _) => Err(error),
                (None, None | Some(Value::Null)) => {
                    return Err(Error::Batch(format!("batch entry {id} has no result")))
                }
                (None, Some(result)) => Ok(serde_json::from_value(result)
                    .map_err(|error| Error::Batch(format!("batch entry {id}: {error}")))?),
            });
        }
        Ok(placed
            .into_iter()
            .map(|entry| entry.expect("every id was placed"))
            .collect())
    }

    /// Posts `body` and returns the response body, read within the policy's
    /// deadline and bound whether or not the response declares its length.
    async fn post(&self, body: &Value) -> Result<Vec<u8>, Error> {
        let mut request = self.0.http.post(&self.0.url).json(body);
        match &self.0.credentials {
            Credentials::None => {}
            Credentials::Fixed(user, password) => {
                request = request.basic_auth(user, Some(password))
            }
            Credentials::File(path) => {
                let (user, password) = read_cookie(path)?;
                request = request.basic_auth(user, Some(password));
            }
        }
        let mut response = request.send().await?;
        if response.status() == reqwest::StatusCode::UNAUTHORIZED
            && self.0.policy.unauthorized == Unauthorized::InvalidCookie
        {
            return Err(Error::InvalidCookie);
        }
        response = response.error_for_status()?;
        let limit = self.0.policy.response_bytes;
        if response
            .content_length()
            .is_some_and(|length| length > limit as u64)
        {
            return Err(Error::Oversized(limit));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await? {
            if bytes.len() + chunk.len() > limit {
                return Err(Error::Oversized(limit));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(bytes)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The production policies, as the plan fixes them.
    #[test]
    fn production_policies() {
        let shared = Policy::cookie_at_construction();
        assert_eq!(shared.cookie, CookieReading::AtConstruction);
        assert_eq!(shared.total, Duration::from_secs(120));
        assert_eq!(shared.connect, None);
        assert_eq!(shared.response_bytes, 64 * 1024 * 1024);
        assert_eq!(shared.unauthorized, Unauthorized::InvalidCookie);
        let receiver = Policy::cookie_every_request(5_000_000);
        assert_eq!(receiver.cookie, CookieReading::EveryRequest);
        assert_eq!(receiver.total, Duration::from_secs(120));
        assert_eq!(receiver.connect, Some(Duration::from_secs(20)));
        assert_eq!(receiver.response_bytes, 5_000_000);
        assert_eq!(receiver.unauthorized, Unauthorized::HttpStatus);
    }
}
