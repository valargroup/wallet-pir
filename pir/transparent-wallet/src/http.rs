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
use std::sync::Arc;
use std::time::{Duration, Instant};

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

/// One completed HTTP attempt. Contains no URL, query body, script or key.
#[derive(Clone, Debug)]
pub struct HttpObservation {
    pub stage: &'static str,
    pub status: Option<u16>,
    pub elapsed: Duration,
    /// Submitted request payload size; not a measurement of socket delivery.
    pub bytes_up: u64,
    /// Response payload bytes read, including error responses.
    pub bytes_down: u64,
    pub failed: bool,
}

/// Optional local observer. Called once per attempt, including transport errors.
pub type HttpObserver = Arc<dyn Fn(HttpObservation) + Send + Sync>;

fn execute(
    request: reqwest::blocking::RequestBuilder,
    stage: &'static str,
    bytes_up: u64,
    binding: Option<(u64, &str)>,
    observer: &Option<HttpObserver>,
) -> Result<Vec<u8>, BoxError> {
    if observer.is_none() {
        let response = request.send()?;
        return match binding {
            Some((shard, revision)) => checked(response, shard, revision),
            None => Ok(response.error_for_status()?.bytes()?.to_vec()),
        };
    }
    let started = Instant::now();
    let mut status = None;
    let mut bytes_down = 0;
    let result = (|| {
        let response = request.send()?;
        let code = response.status();
        status = Some(code.as_u16());
        let public_error = response.error_for_status_ref().err();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|h| h.to_str().ok())
            .map(str::to_owned);
        let body = response.bytes()?.to_vec();
        bytes_down = body.len() as u64;
        if let Some((shard, revision)) = binding {
            if !code.is_success() {
                if let Some(error) = refusal(
                    code.as_u16(),
                    retry_after.as_deref(),
                    &body,
                    shard,
                    revision,
                ) {
                    return Err(error);
                }
                return Err(format!(
                    "shard {shard} revision {revision}: HTTP {code}: {}",
                    String::from_utf8_lossy(&body)
                )
                .into());
            }
        } else if let Some(error) = public_error {
            return Err(Box::new(error) as BoxError);
        }
        Ok(body)
    })();
    if let Some(observer) = observer {
        observer(HttpObservation {
            stage,
            status,
            elapsed: started.elapsed(),
            bytes_up,
            bytes_down,
            failed: result.is_err(),
        });
    }
    result
}

/// The private shard service over HTTP.
pub struct HttpShardTransport {
    base: String,
    client: reqwest::blocking::Client,
    observer: Option<HttpObserver>,
}

impl HttpShardTransport {
    pub fn new(base_url: impl Into<String>, options: &HttpOptions) -> Result<Self, BoxError> {
        Ok(Self {
            base: base_url.into().trim_end_matches('/').to_string(),
            client: build(options)?,
            observer: None,
        })
    }

    /// Attach an observer without changing request or retry behavior.
    pub fn with_observer(mut self, observer: HttpObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Fetches and parses the init document.
    pub fn geometry(&mut self) -> Result<ServiceGeometry, BoxError> {
        let (raw, _) = self.init()?;
        parse_init(&raw)
    }
}

impl ShardTransport for HttpShardTransport {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = execute(
            self.client.get(format!("{}/v1/shards/init", self.base)),
            "init",
            0,
            None,
            &self.observer,
        )?;
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = execute(
            self.client.get(format!(
                "{}/v1/shards/{shard_id}/revisions/{revision}/manifest",
                self.base
            )),
            "manifest",
            0,
            Some((shard_id, revision)),
            &self.observer,
        )?;
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
        let bytes = execute(
            self.client.get(format!(
                "{}/v1/shards/{shard_id}/revisions/{revision}/setup/{}/{segment}",
                self.base,
                table.as_str()
            )),
            match table {
                Table::Directory => "setup_directory",
                Table::Pages => "setup_pages",
            },
            0,
            Some((shard_id, revision)),
            &self.observer,
        )?;
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
        execute(
            self.client
                .post(format!(
                    "{}/v1/shards/{shard_id}/revisions/{revision}/query/{}",
                    self.base,
                    table.as_str()
                ))
                .body(body.to_vec()),
            match table {
                Table::Directory => "query_directory",
                Table::Pages => "query_pages",
            },
            body.len() as u64,
            Some((shard_id, revision)),
            &self.observer,
        )
    }
}

/// The public filter origin over HTTP: the map and each shard's filter.
pub struct HttpFilterSource {
    base: String,
    client: reqwest::blocking::Client,
    observer: Option<HttpObserver>,
}

impl HttpFilterSource {
    pub fn new(base_url: impl Into<String>, options: &HttpOptions) -> Result<Self, BoxError> {
        Ok(Self {
            base: base_url.into().trim_end_matches('/').to_string(),
            client: build(options)?,
            observer: None,
        })
    }

    /// Attach an observer without changing request or retry behavior.
    pub fn with_observer(mut self, observer: HttpObserver) -> Self {
        self.observer = Some(observer);
        self
    }

    /// Fetches and validates the map, returning its payload length.
    pub fn map(&mut self) -> Result<(transparent_filter::ShardMap, u64), BoxError> {
        let (raw, len) = self.shard_map()?;
        let map: transparent_filter::ShardMap = serde_json::from_slice(&raw)?;
        map.check_shape()?;
        Ok((map, len))
    }
}

impl FilterSource for HttpFilterSource {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = execute(
            self.client.get(format!("{}/v1/filters/shards", self.base)),
            "map",
            0,
            None,
            &self.observer,
        )?;
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = execute(
            self.client
                .get(format!("{}/v1/filters/shards/{shard_id}/filter", self.base)),
            "filters",
            0,
            None,
            &self.observer,
        )?;
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }
}

#[cfg(test)]
mod observation_tests {
    use super::*;
    use std::io::{Read, Write};
    use std::sync::Mutex;

    #[test]
    fn observes_success_refusal_payload_and_connection_failure_without_changing_results() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            for (status, body) in [(200, "ok"), (503, "busy")] {
                let (mut stream, _) = listener.accept().unwrap();
                let mut request = [0; 1024];
                let _ = stream.read(&mut request).unwrap();
                write!(stream, "HTTP/1.1 {status} Test\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}", body.len()).unwrap();
            }
        });
        let events = Arc::new(Mutex::new(Vec::new()));
        let observed = events.clone();
        let mut filters =
            HttpFilterSource::new(format!("http://{address}"), &HttpOptions::default())
                .unwrap()
                .with_observer(Arc::new(move |o| observed.lock().unwrap().push(o)));
        assert_eq!(filters.filter(0).unwrap().0, b"ok");
        assert!(filters.filter(0).unwrap_err().to_string().contains("503"));
        server.join().unwrap();
        assert!(filters.filter(0).is_err());
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 3);
        assert_eq!(events[0].status, Some(200));
        assert_eq!(events[0].bytes_down, 2);
        assert!(!events[0].failed);
        assert_eq!(events[1].status, Some(503));
        assert_eq!(events[1].bytes_down, 4);
        assert!(events[1].failed);
        assert_eq!(events[2].status, None);
        assert!(events[2].failed);
    }
}
