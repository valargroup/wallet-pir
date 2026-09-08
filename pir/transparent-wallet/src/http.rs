//! Reference HTTP adapters, behind the `reqwest` feature.
//!
//! One blocking client each for the private shard service and the public
//! filter origin, reading the reply's status, `retry-after` and body before
//! anything is discarded, so the two refusals a wallet can act on reach it as
//! typed errors. A wallet with its own HTTP stack implements the transport
//! traits itself; this is what the tests, the measurement tools and a first
//! integration use.

use crate::client::Table;
pub use crate::init::parse_init;
use crate::sync::ServiceGeometry;
use crate::transport::{refusal, BoxError, FilterSource, ShardTransport};
use std::time::Duration;

/// How the reference clients are built.
#[derive(Clone, Debug)]
pub struct HttpOptions {
    pub timeout: Duration,
    pub user_agent: String,
}

impl Default for HttpOptions {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(60),
            user_agent: "transparent-wallet".into(),
        }
    }
}

fn build(options: &HttpOptions) -> Result<reqwest::blocking::Client, BoxError> {
    Ok(reqwest::blocking::Client::builder()
        .timeout(options.timeout)
        .user_agent(options.user_agent.clone())
        .build()?)
}

/// Reads a reply, turning a refusal the wallet can act on into one.
///
/// The body and the `retry-after` header are read before the status is thrown
/// away: `error_for_status` keeps only the code, and the map digest a stale
/// refusal carries lives in the body.
pub fn checked(
    response: reqwest::blocking::Response,
    shard_id: u64,
    revision: &str,
) -> Result<Vec<u8>, BoxError> {
    let status = response.status();
    if status.is_success() {
        return Ok(response.bytes()?.to_vec());
    }
    let retry_after = response
        .headers()
        .get(reqwest::header::RETRY_AFTER)
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned);
    let body = response.bytes()?.to_vec();
    if let Some(refused) = refusal(
        status.as_u16(),
        retry_after.as_deref(),
        &body,
        shard_id,
        revision,
    ) {
        return Err(refused);
    }
    Err(format!(
        "shard {shard_id} revision {revision}: HTTP {status}: {}",
        String::from_utf8_lossy(&body)
    )
    .into())
}

/// The private shard service over HTTP.
pub struct HttpShardTransport {
    base: String,
    client: reqwest::blocking::Client,
}

impl HttpShardTransport {
    pub fn new(base_url: impl Into<String>, options: &HttpOptions) -> Result<Self, BoxError> {
        Ok(Self {
            base: base_url.into().trim_end_matches('/').to_string(),
            client: build(options)?,
        })
    }

    /// Fetches and parses the init document.
    pub fn geometry(&mut self) -> Result<ServiceGeometry, BoxError> {
        let (raw, _) = self.init()?;
        parse_init(&raw)
    }
}

impl ShardTransport for HttpShardTransport {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self
            .client
            .get(format!("{}/v1/shards/init", self.base))
            .send()?
            .error_for_status()?
            .bytes()?
            .to_vec();
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        let response = self
            .client
            .get(format!(
                "{}/v1/shards/{shard_id}/revisions/{revision}/manifest",
                self.base
            ))
            .send()?;
        let bytes = checked(response, shard_id, revision)?;
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn setup(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError> {
        let response = self
            .client
            .get(format!(
                "{}/v1/shards/{shard_id}/revisions/{revision}/setup/{}/{segment}",
                self.base,
                table.as_str()
            ))
            .send()?;
        let bytes = checked(response, shard_id, revision)?;
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn query(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        body: &[u8],
    ) -> Result<Vec<u8>, BoxError> {
        let response = self
            .client
            .post(format!(
                "{}/v1/shards/{shard_id}/revisions/{revision}/query/{}",
                self.base,
                table.as_str()
            ))
            .body(body.to_vec())
            .send()?;
        checked(response, shard_id, revision)
    }
}

/// The public filter origin over HTTP: the map and each shard's filter.
///
/// Point it at the filter service or at the retrieval origin; both serve the
/// same paths. A wallet that wants its public and private bytes on different
/// network paths gives this a different base than the transport.
pub struct HttpFilterSource {
    base: String,
    client: reqwest::blocking::Client,
}

impl HttpFilterSource {
    pub fn new(base_url: impl Into<String>, options: &HttpOptions) -> Result<Self, BoxError> {
        Ok(Self {
            base: base_url.into().trim_end_matches('/').to_string(),
            client: build(options)?,
        })
    }

    /// Fetches and parses the map, returning it with its bytes' length.
    pub fn map(&mut self) -> Result<(transparent_filter::ShardMap, u64), BoxError> {
        let (raw, len) = self.shard_map()?;
        let map: transparent_filter::ShardMap = serde_json::from_slice(&raw)?;
        map.check_shape()?;
        Ok((map, len))
    }
}

impl FilterSource for HttpFilterSource {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self
            .client
            .get(format!("{}/v1/filters/shards", self.base))
            .send()?
            .error_for_status()?
            .bytes()?
            .to_vec();
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self
            .client
            .get(format!("{}/v1/filters/shards/{shard_id}/filter", self.base))
            .send()?
            .error_for_status()?
            .bytes()?
            .to_vec();
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }
}
