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
    /// Total budget for one HTTP call, including every attempt and backoff.
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

#[derive(Debug, thiserror::Error)]
#[error("{message}")]
struct HttpStatusError {
    status: u16,
    message: String,
    #[source]
    cause: Option<BoxError>,
}

fn status_error(status: reqwest::StatusCode, shard: u64, revision: &str, body: &[u8]) -> BoxError {
    Box::new(HttpStatusError {
        status: status.as_u16(),
        cause: None,
        message: format!(
            "shard {shard} revision {revision}: HTTP {status}: {}",
            String::from_utf8_lossy(body)
        ),
    })
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
        return Err(Box::new(HttpStatusError {
            status: status.as_u16(),
            message: refused.to_string(),
            cause: Some(refused),
        }));
    }
    Err(status_error(status, shard_id, revision, &body))
}

/// One completed HTTP attempt. Contains no URL, query body, script or key.
#[derive(Clone, Debug)]
pub struct HttpObservation {
    pub stage: &'static str,
    /// Process-local identifier shared by all attempts of one HTTP call.
    pub request_id: u64,
    pub attempt: usize,
    pub status: Option<u16>,
    pub elapsed: Duration,
    /// Submitted request payload size; not a measurement of socket delivery.
    pub bytes_up: u64,
    /// Response payload bytes read, including error responses.
    pub bytes_down: u64,
    pub failed: bool,
    /// Underlying transport causes, without the top-level request URL.
    pub transport_error: Option<String>,
}

/// Optional local observer. Called once per attempt, including transport errors.
pub type HttpObserver = Arc<dyn Fn(HttpObservation) + Send + Sync>;

#[derive(Clone, Copy)]
struct RetryPolicy {
    attempts: usize,
    overload: bool,
    timeout: Duration,
}

/// Opt-in retries replay the same buffered request within one shared deadline.
/// Application decoding and revision validation occur after this function and
/// are never retried here. The observer records every attempt; callers must
/// still impose an overall recovery deadline.
fn execute(
    request: reqwest::blocking::RequestBuilder,
    stage: &'static str,
    bytes_up: u64,
    binding: Option<(u64, &str)>,
    observer: &Option<HttpObserver>,
    policy: RetryPolicy,
) -> Result<Vec<u8>, BoxError> {
    static REQUEST_ID: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);
    let request_id = REQUEST_ID.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let attempts = policy.attempts.max(1);
    let retry_overload = policy.overload;
    let started = Instant::now();
    for attempt in 0..attempts {
        let remaining = policy.timeout.saturating_sub(started.elapsed());
        if remaining.is_zero() {
            return Err("HTTP call deadline exhausted".into());
        }
        let observed = observer.as_ref().map(|observer| {
            let observer = observer.clone();
            Arc::new(move |mut event: HttpObservation| {
                event.request_id = request_id;
                event.attempt = attempt + 1;
                observer(event);
            }) as HttpObserver
        });
        let copy = request
            .try_clone()
            .ok_or("HTTP request cannot be retried")?
            .timeout(remaining);
        let result = execute_once(copy, stage, bytes_up, binding, &observed);
        let delay = result.as_ref().err().and_then(|error| {
            if !retry_overload
                && error
                    .downcast_ref::<HttpStatusError>()
                    .is_some_and(|e| matches!(e.status, 502 | 504))
            {
                return Some(Duration::from_secs(attempt as u64 + 1));
            }
            if let Some(overloaded) = crate::transport::Overloaded::found_in(error) {
                return if retry_overload {
                    overloaded.retry_after
                } else {
                    None
                };
            }
            if error
                .downcast_ref::<HttpStatusError>()
                .is_some_and(|error| {
                    matches!(error.status, 408 | 502 | 504)
                        || (retry_overload && error.status == 503)
                })
            {
                return Some(Duration::from_secs(attempt as u64 + 1));
            }
            error.downcast_ref::<reqwest::Error>().and_then(|error| {
                (error.is_timeout()
                    || error.is_connect()
                    // These read-only API calls have buffered, replayable bodies.
                    // Established-connection failures are often Request or Body,
                    // rather than Connect. Builder/status errors stay separate.
                    || error.is_request()
                    || error.is_body()
                    // Response::bytes classifies an interrupted body as Decode.
                    // Protocol decoding happens after execute and is not retried.
                    || error.is_decode()
                    || error.status().is_some_and(|s| {
                        matches!(s.as_u16(), 408 | 502 | 504)
                            || (retry_overload && s.as_u16() == 503)
                    }))
                .then_some(Duration::from_secs(attempt as u64 + 1))
            })
        });
        if attempt + 1 == attempts || delay.is_none() {
            return result;
        }
        let delay = delay.unwrap();
        if delay >= policy.timeout.saturating_sub(started.elapsed()) {
            // Return the last refusal/error without oversleeping its budget.
            return result;
        }
        std::thread::sleep(delay);
    }
    unreachable!("attempts is positive")
}

fn response_body(response: reqwest::blocking::Response, stage: &str) -> Result<Vec<u8>, BoxError> {
    let limit = match stage {
        "parent_manifest" => 2 * 1024 * 1024,
        "parent_filters" => transparent_filter::experimental_parent::LIMITS.max_bytes,
        _ => return Ok(response.bytes()?.to_vec()),
    };
    use std::io::Read;
    let mut bytes = Vec::new();
    response.take(limit as u64 + 1).read_to_end(&mut bytes)?;
    if bytes.len() > limit {
        return Err("experimental parent response exceeds size limit".into());
    }
    Ok(bytes)
}

fn execute_once(
    request: reqwest::blocking::RequestBuilder,
    stage: &'static str,
    bytes_up: u64,
    binding: Option<(u64, &str)>,
    observer: &Option<HttpObserver>,
) -> Result<Vec<u8>, BoxError> {
    let started = Instant::now();
    let mut status = None;
    let mut bytes_down = 0;
    let result = (|| -> Result<Vec<u8>, BoxError> {
        let response = request.send()?;
        let code = response.status();
        status = Some(code.as_u16());
        let public_error = response.error_for_status_ref().err();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|h| h.to_str().ok())
            .map(str::to_owned);
        let body = response_body(response, stage)?;
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
                    return Err(Box::new(HttpStatusError {
                        status: code.as_u16(),
                        message: error.to_string(),
                        cause: Some(error),
                    }));
                }
                return Err(status_error(code, shard, revision, &body));
            }
        } else if let Some(error) = public_error {
            if let Some(overloaded) =
                crate::transport::Overloaded::from_http(code.as_u16(), retry_after.as_deref())
            {
                return Err(Box::new(HttpStatusError {
                    status: code.as_u16(),
                    message: overloaded.to_string(),
                    cause: Some(Box::new(overloaded)),
                }));
            }
            return Err(Box::new(error) as BoxError);
        }
        Ok(body)
    })();
    if let Some(observer) = observer {
        observer(HttpObservation {
            request_id: 0,
            attempt: 1,
            stage,
            status,
            elapsed: started.elapsed(),
            bytes_up,
            bytes_down,
            failed: result.is_err(),
            transport_error: result.as_ref().err().and_then(|error| {
                let error = error.downcast_ref::<reqwest::Error>()?;
                if error.is_status() {
                    return None;
                }
                let mut causes = Vec::new();
                let mut source = std::error::Error::source(error);
                while let Some(cause) = source {
                    causes.push(cause.to_string());
                    source = cause.source();
                }
                Some(causes.join(": "))
            }),
        });
    }
    result
}

/// A public immutable resource downloaded with the same measured retry policy.
/// Response bytes are returned before any application decoding occurs.
pub struct HttpResourceClient {
    client: reqwest::blocking::Client,
    observer: Option<HttpObserver>,
    retry: RetryPolicy,
}
impl HttpResourceClient {
    pub fn new(
        options: &HttpOptions,
        observer: Option<HttpObserver>,
        attempts: usize,
    ) -> Result<Self, BoxError> {
        Ok(Self {
            client: reqwest::blocking::Client::builder()
                .timeout(options.timeout)
                .user_agent(options.user_agent.clone())
                .no_gzip()
                .no_brotli()
                .no_deflate()
                .no_zstd()
                .build()?,
            observer,
            retry: RetryPolicy {
                attempts: attempts.clamp(1, 3),
                overload: false,
                timeout: options.timeout,
            },
        })
    }
    pub fn get(&self, url: &str, stage: &'static str) -> Result<Vec<u8>, BoxError> {
        execute(
            // Explicit negotiation also prevents intermediary HTTP clients from
            // transparently decompressing a response before forwarding it.
            self.client
                .get(url)
                .header("accept-encoding", "gzip, identity"),
            stage,
            0,
            None,
            &self.observer,
            self.retry,
        )
    }
}

/// The private shard service over HTTP.
pub struct HttpShardTransport {
    base: String,
    client: reqwest::blocking::Client,
    observer: Option<HttpObserver>,
    retry: RetryPolicy,
}

impl HttpShardTransport {
    pub fn new(base_url: impl Into<String>, options: &HttpOptions) -> Result<Self, BoxError> {
        Ok(Self {
            base: base_url.into().trim_end_matches('/').to_string(),
            client: build(options)?,
            observer: None,
            retry: RetryPolicy {
                attempts: 1,
                overload: true,
                timeout: options.timeout,
            },
        })
    }

    /// Opt into a bounded number of attempts per HTTP call (1–3). This is
    /// separate from wallet-level overload retries and defaults to one attempt.
    pub fn with_retry_attempts(mut self, attempts: usize) -> Self {
        self.retry.attempts = attempts.clamp(1, 3);
        self
    }

    /// Retry transient gateway/upload/connection failures without layering
    /// additional overload retries on the wallet's refusal policy.
    pub fn with_transient_retry_attempts(mut self, attempts: usize) -> Self {
        self.retry.attempts = attempts.clamp(1, 3);
        self.retry.overload = false;
        self
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
            self.retry,
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
            self.retry,
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
            self.retry,
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
            self.retry,
        )
    }
}

/// The public filter origin over HTTP: the map and each shard's filter.
#[path = "experimental_parent_http.rs"]
mod experimental_parent_http;
use experimental_parent_http::ParentExperiment;

pub struct HttpFilterSource {
    parent_experiments: Vec<ParentExperiment>,
    base: String,
    client: reqwest::blocking::Client,
    observer: Option<HttpObserver>,
    retry: RetryPolicy,
}

impl HttpFilterSource {
    /// Explicit research opt-in. The geometry prevents archive discovery from
    /// adding any requests to wallets that only need recent history.
    pub fn with_parent_experiment(mut self, manifest_url: String, geometry: String) -> Self {
        self.parent_experiments
            .push(ParentExperiment::new(manifest_url, geometry));
        self
    }

    pub fn new(base_url: impl Into<String>, options: &HttpOptions) -> Result<Self, BoxError> {
        Ok(Self {
            parent_experiments: Vec::new(),
            base: base_url.into().trim_end_matches('/').to_string(),
            client: build(options)?,
            observer: None,
            retry: RetryPolicy {
                attempts: 1,
                overload: true,
                timeout: options.timeout,
            },
        })
    }

    /// Opt into a bounded number of attempts per HTTP call (1–3). This is
    /// separate from wallet-level overload retries and defaults to one attempt.
    pub fn with_retry_attempts(mut self, attempts: usize) -> Self {
        self.retry.attempts = attempts.clamp(1, 3);
        self
    }

    /// Retry transient gateway/upload/connection failures without layering
    /// additional overload retries on the wallet's refusal policy.
    pub fn with_transient_retry_attempts(mut self, attempts: usize) -> Self {
        self.retry.attempts = attempts.clamp(1, 3);
        self.retry.overload = false;
        self
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
    fn uses_parents(&self) -> bool {
        !self.parent_experiments.is_empty()
    }
    fn prepare_parents(
        &mut self,
        map: &transparent_filter::ShardMap,
        uncached: &[u64],
        store: &mut dyn crate::WalletStore,
    ) -> Result<u64, BoxError> {
        let mut states = std::mem::take(&mut self.parent_experiments);
        let result = states.iter_mut().try_fold(0, |cost, state| {
            Ok(cost + state.prepare(self, map, uncached, store)?)
        });
        self.parent_experiments = states;
        result
    }
    fn parent_negative(
        &mut self,
        map: &transparent_filter::ShardMap,
        id: u64,
        scripts: &[Vec<u8>],
        store: &mut dyn crate::WalletStore,
    ) -> Result<(bool, u64), BoxError> {
        let mut states = std::mem::take(&mut self.parent_experiments);
        let result = states
            .iter_mut()
            .try_fold((false, 0), |(negative, cost), state| {
                let (next, bytes) = state.negative(self, map, id, scripts, store)?;
                Ok((negative || next, cost + bytes))
            });
        self.parent_experiments = states;
        result
    }

    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = execute(
            self.client.get(format!("{}/v1/filters/shards", self.base)),
            "map",
            0,
            None,
            &self.observer,
            self.retry,
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
            self.retry,
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

    fn read_headers(stream: &mut std::net::TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(5)))
            .unwrap();
        let mut headers = Vec::new();
        while !headers.ends_with(b"\r\n\r\n") {
            let mut byte = [0];
            stream.read_exact(&mut byte).unwrap();
            headers.push(byte[0]);
            assert!(headers.len() < 8192);
        }
    }

    #[test]
    fn retry_after_cannot_extend_the_deadline_on_public_or_bound_requests() {
        for bound in [false, true] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let server = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().unwrap();
                read_headers(&mut stream);
                stream.write_all(b"HTTP/1.1 503 Busy\r\nRetry-After: 10\r\nContent-Length: 0\r\nConnection: close\r\n\r\n").unwrap();
            });
            let events = Arc::new(Mutex::new(Vec::new()));
            let saved = events.clone();
            let mut client = HttpShardTransport::new(
                format!("http://{address}"),
                &HttpOptions {
                    timeout: Duration::from_millis(500),
                    user_agent: "deadline-test".into(),
                },
            )
            .unwrap()
            .with_retry_attempts(3)
            .with_observer(Arc::new(move |e| saved.lock().unwrap().push(e)));
            let started = Instant::now();
            let result = if bound {
                client.manifest(1, "revision")
            } else {
                client.init()
            };
            let error = result.unwrap_err();
            assert!(crate::transport::Overloaded::found_in(&error).is_some());
            assert!(started.elapsed() < Duration::from_secs(1));
            server.join().unwrap();
            assert_eq!(events.lock().unwrap().len(), 1);
        }
    }

    #[test]
    fn retries_share_one_deadline_including_backoff_and_response_reads() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        let server = std::thread::spawn(move || {
            let (mut first, _) = listener.accept().unwrap();
            read_headers(&mut first);
            first
                .write_all(b"HTTP/1.1 503 Busy\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
                .unwrap();
            drop(first);
            let (mut second, _) = listener.accept().unwrap();
            read_headers(&mut second);
            // Longer than the remaining budget, but shorter than a fresh one.
            std::thread::sleep(Duration::from_millis(900));
            let _ = second.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        });
        let events = Arc::new(Mutex::new(Vec::new()));
        let saved = events.clone();
        let mut client = HttpShardTransport::new(
            format!("http://{address}"),
            &HttpOptions {
                timeout: Duration::from_millis(1500),
                user_agent: "deadline-test".into(),
            },
        )
        .unwrap()
        .with_retry_attempts(3)
        .with_observer(Arc::new(move |e| saved.lock().unwrap().push(e)));
        let started = Instant::now();
        assert!(
            client.init().is_err(),
            "second attempt must not receive a fresh timeout"
        );
        let elapsed = started.elapsed();
        server.join().unwrap();
        assert!(elapsed < Duration::from_millis(1850), "{elapsed:?}");
        let events = events.lock().unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].status, Some(503));
        assert!(events.iter().all(|e| e.failed));
    }

    #[test]
    fn retries_established_connection_failures_with_bounded_attempts() {
        // Exercise both public GETs and private POSTs, with and without telemetry.
        for partial_body in [false, true] {
            for private in [false, true] {
                for (attempts, failures, observe) in
                    [(1, 1, true), (3, 1, true), (3, 3, true), (3, 1, false)]
                {
                    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
                    let address = listener.local_addr().unwrap();
                    listener.set_nonblocking(true).unwrap();
                    let expected = attempts.min(failures + 1);
                    let server = std::thread::spawn(move || {
                        let deadline = Instant::now() + Duration::from_secs(60);
                        for index in 0..expected {
                            let mut stream = loop {
                                match listener.accept() {
                                    Ok((stream, _)) => break stream,
                                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                                        assert!(Instant::now() < deadline, "missing retry {index}");
                                        std::thread::sleep(Duration::from_millis(10));
                                    }
                                    Err(e) => panic!("{e}"),
                                }
                            };
                            stream.set_nonblocking(false).unwrap();
                            stream
                                .set_read_timeout(Some(Duration::from_secs(30)))
                                .unwrap();
                            let mut headers = Vec::new();
                            while !headers.ends_with(b"\r\n\r\n") {
                                let mut byte = [0];
                                stream.read_exact(&mut byte).unwrap();
                                headers.push(byte[0]);
                                assert!(headers.len() <= 4096);
                            }
                            if private {
                                let mut body = [0; 7];
                                stream.read_exact(&mut body).unwrap();
                                assert_eq!(&body, b"payload");
                            }
                            if index >= failures {
                                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nok").unwrap();
                            } else if partial_body {
                                stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 100\r\nConnection: close\r\n\r\npartial").unwrap();
                            }
                            // Close an established connection before headers or a full body.
                        }
                    });
                    let events = Arc::new(Mutex::new(Vec::new()));
                    let observed = events.clone();
                    let observer: Option<HttpObserver> = observe.then(|| {
                        Arc::new(move |o: HttpObservation| observed.lock().unwrap().push(o))
                            as HttpObserver
                    });
                    let client = reqwest::blocking::Client::builder()
                        .timeout(Duration::from_secs(30))
                        .build()
                        .unwrap();
                    let url = format!("http://{address}");
                    let request = if private {
                        client.post(&url).body("payload")
                    } else {
                        client.get(&url)
                    };
                    let result = execute(
                        request,
                        "test",
                        if private { 7 } else { 0 },
                        private.then_some((1, "revision")),
                        &observer,
                        RetryPolicy {
                            attempts,
                            overload: false,
                            timeout: Duration::from_secs(30),
                        },
                    );
                    server.join().unwrap();
                    assert_eq!(
                        result.is_ok(),
                        failures < attempts,
                        "partial={partial_body} private={private}: {result:?}"
                    );
                    if let Ok(body) = result {
                        assert_eq!(body, b"ok");
                    }
                    let events = events.lock().unwrap();
                    assert_eq!(events.len(), if observe { expected } else { 0 });
                    for (index, event) in events.iter().enumerate() {
                        assert_eq!(event.request_id, events[0].request_id);
                        assert_eq!(event.attempt, index + 1);
                        assert_eq!(event.bytes_up, if private { 7 } else { 0 });
                        assert_eq!(event.failed, index < failures);
                        assert_eq!(
                            event.status,
                            (partial_body || index >= failures).then_some(200)
                        );
                        if event.failed {
                            let cause = event.transport_error.as_ref().unwrap();
                            assert!(!cause.is_empty());
                            assert!(!cause.contains(&url));
                        } else {
                            assert!(event.transport_error.is_none());
                        }
                    }
                }
            }
        }
    }

    #[test]
    fn opt_in_retries_are_bounded_and_account_for_every_response() {
        for (retry_overload, statuses) in [
            (true, vec![502, 200]),
            (true, vec![502, 502, 502]),
            (false, vec![502, 200]),
            (false, vec![504, 200]),
            (false, vec![408, 200]),
            (false, vec![408, 408, 408]),
            (true, vec![503, 200]),
            (false, vec![503]),
            (false, vec![404]),
        ] {
            let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let address = listener.local_addr().unwrap();
            let expected = statuses.len();
            let success = statuses.last() == Some(&200);
            let server = std::thread::spawn(move || {
                for status in statuses {
                    let (mut stream, _) = listener.accept().unwrap();
                    let mut request = Vec::new();
                    while !request.ends_with(b"\r\n\r\n") {
                        let mut byte = [0];
                        stream.read_exact(&mut byte).unwrap();
                        request.push(byte[0]);
                        assert!(request.len() <= 4096);
                    }
                    write!(stream, "HTTP/1.1 {status} Test\r\nContent-Length: 2\r\nRetry-After: 0\r\nConnection: close\r\n\r\nok").unwrap();
                }
            });
            let observations = Arc::new(Mutex::new(Vec::new()));
            let observed = observations.clone();
            let observer: HttpObserver = Arc::new(move |o| observed.lock().unwrap().push(o));
            let request = reqwest::blocking::Client::new().get(format!("http://{address}"));
            let result = execute(
                request,
                "setup_directory",
                0,
                Some((1, "revision")),
                &Some(observer),
                RetryPolicy {
                    attempts: 3,
                    overload: retry_overload,
                    timeout: Duration::from_secs(30),
                },
            );
            assert_eq!(result.is_ok(), success);
            server.join().unwrap();
            let observations = observations.lock().unwrap();
            assert_eq!(observations.len(), expected);
            assert!(observations
                .iter()
                .all(|o| o.request_id == observations[0].request_id));
            assert_eq!(
                observations.iter().map(|o| o.attempt).collect::<Vec<_>>(),
                (1..=expected).collect::<Vec<_>>()
            );
            assert_eq!(
                observations.iter().map(|o| o.bytes_down).sum::<u64>(),
                expected as u64 * 2
            );
            assert_eq!(
                observations.iter().filter(|o| o.failed).count(),
                expected - usize::from(success)
            );
        }
    }

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
