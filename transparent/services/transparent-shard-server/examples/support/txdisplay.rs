//! Reference client for the tiered txid display publication: tooling and
//! tests only, with no wallet scheduling, persistence or authority.
//!
//! A lookup is the map, the shard's manifest, the bucket's setup (all cached),
//! then a fixed transcript: exactly two directory queries, even when both
//! candidate rows coincide, and exactly `pages` page queries for a paged
//! record. Decoded rows are deduplicated by (segment, row) before the entry is
//! found and the record assembled. Any failure is an error, never `Absent`;
//! a 409 refreshes the map and retries the lookup once.
//!
//! Every request's header and body bytes are computed from what HTTP/1.1
//! puts on the wire, and the client sets every request header itself so the
//! computation is exact; `bytemeter` checks it against a TCP relay.
#![allow(dead_code)]

use base64::{engine::general_purpose::STANDARD as B64, Engine};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, HashMap};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use transparent_events::Txid;
use transparent_native::{NativeScheme, TableProfile};
use transparent_shard::display::{
    self, display_by_name, DisplayManifest, DisplayMap, DisplayMapEntry, DisplayTable,
};
use transparent_shard::layout::Geometry;
use transparent_shard::txid::{self, TransparentDisplayRecord};
use transparent_shard_server::display::kind;
use transparent_shard_server::shardset::{setup_seed, Table};

#[derive(Debug)]
pub enum LookupError {
    Transport(String),
    Status {
        code: u16,
        path: String,
        body: String,
    },
    Protocol(String),
}

impl std::fmt::Display for LookupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            LookupError::Transport(error) => write!(f, "transport: {error}"),
            LookupError::Status { code, path, body } => write!(f, "{code} from {path}: {body}"),
            LookupError::Protocol(error) => write!(f, "protocol: {error}"),
        }
    }
}

impl std::error::Error for LookupError {}

impl LookupError {
    pub fn status(&self) -> Option<u16> {
        match self {
            LookupError::Status { code, .. } => Some(*code),
            _ => None,
        }
    }
}

fn protocol(error: impl std::fmt::Display) -> LookupError {
    LookupError::Protocol(error.to_string())
}

#[derive(Clone, Debug, PartialEq)]
pub enum LookupResult {
    Found(TransparentDisplayRecord),
    Absent,
    PlacementUnknown,
    Unsupported,
}

/// One private query as it happened.
#[derive(Clone, Debug, Default, Serialize)]
pub struct QueryTrace {
    /// `directory-{b}` or `pages`.
    pub table: String,
    /// The row selected within every segment.
    pub row: u64,
    pub http_s: f64,
    pub prepare_s: f64,
    pub decode_s: f64,
    pub up_body: u64,
    pub down_body: u64,
    pub up_headers: u64,
    pub down_headers: u64,
    pub status: u16,
}

/// Bytes one lookup moved, by purpose. Metadata entries are response bodies;
/// header bytes cover every request of the lookup.
#[derive(Clone, Copy, Debug, Default, Serialize)]
pub struct ByteCounts {
    pub init: u64,
    pub map: u64,
    pub manifest: u64,
    pub setup: u64,
    pub query_up: u64,
    pub query_down: u64,
    pub headers_up: u64,
    pub headers_down: u64,
}

impl ByteCounts {
    pub fn up(&self) -> u64 {
        self.query_up + self.headers_up
    }

    pub fn down(&self) -> u64 {
        self.init + self.map + self.manifest + self.setup + self.query_down + self.headers_down
    }

    pub fn total(&self) -> u64 {
        self.up() + self.down()
    }
}

/// Which cached inputs this lookup had to fetch.
#[derive(Clone, Debug, Default, Serialize)]
pub struct ColdFlags {
    pub init: bool,
    pub map: bool,
    pub manifest: bool,
    pub setups: u32,
}

#[derive(Clone, Debug, Serialize)]
pub struct LookupReport {
    #[serde(skip)]
    pub result: LookupResult,
    /// `found`, `absent`, `placement_unknown` or `unsupported`.
    pub outcome: &'static str,
    pub shard_id: Option<u64>,
    pub sealed: Option<bool>,
    pub digest: Option<String>,
    pub map_sha256: Option<String>,
    pub bucket: Option<u32>,
    /// `inline`, `pages-{k}` or `absent`; empty when nothing was queried.
    pub class: String,
    pub queries: Vec<QueryTrace>,
    pub bytes: ByteCounts,
    pub cold: ColdFlags,
    pub total_s: f64,
    pub stale_retries: u32,
}

impl LookupReport {
    fn new() -> Self {
        Self {
            result: LookupResult::PlacementUnknown,
            outcome: "placement_unknown",
            shard_id: None,
            sealed: None,
            digest: None,
            map_sha256: None,
            bucket: None,
            class: String::new(),
            queries: Vec::new(),
            bytes: ByteCounts::default(),
            cold: ColdFlags::default(),
            total_s: 0.0,
            stale_retries: 0,
        }
    }

    fn set(&mut self, result: LookupResult) {
        self.outcome = match &result {
            LookupResult::Found(_) => "found",
            LookupResult::Absent => "absent",
            LookupResult::PlacementUnknown => "placement_unknown",
            LookupResult::Unsupported => "unsupported",
        };
        self.result = result;
    }
}

#[derive(Deserialize)]
struct InitTable {
    rows: u64,
    row_bytes: u32,
    scheme: NativeScheme,
    setup_seed: u64,
}

#[derive(Deserialize)]
struct InitGeometry {
    name: String,
    txdirectory: InitTable,
    txpages: InitTable,
}

#[derive(Deserialize)]
struct InitDocument {
    schema: String,
    codec: String,
    bucket_domain: String,
    native_schema: String,
    geometries: Vec<InitGeometry>,
}

struct Init {
    supported: bool,
    geometries: HashMap<String, InitGeometry>,
}

/// What a lookup knows before its first query.
struct Located {
    init: Arc<Init>,
    entry: Arc<DisplayMapEntry>,
    geometry: &'static Geometry,
    bucket: u32,
    manifest: Arc<DisplayManifest>,
}

/// One table of one revision, ready to be queried.
#[derive(Clone)]
struct Target {
    entry: Arc<DisplayMapEntry>,
    table: DisplayTable,
    profile: Arc<TableProfile>,
    setups: Arc<Vec<Arc<Setup>>>,
}

struct CachedMap {
    map: DisplayMap,
    sha256: String,
}

struct Setup {
    public_params: Vec<u8>,
    epoch: [u8; 8],
}

/// Native profiles are a pure function of geometry and kind and are costly
/// to derive, so fresh clients may share them; nothing network-facing is.
pub type ProfileCache = Arc<Mutex<HashMap<(&'static str, Table), Arc<TableProfile>>>>;

type Cell<T> = Arc<tokio::sync::OnceCell<Arc<T>>>;

struct Inner {
    http: reqwest::Client,
    base: reqwest::Url,
    profiles: ProfileCache,
    init: tokio::sync::OnceCell<Arc<Init>>,
    map: tokio::sync::Mutex<Option<Arc<CachedMap>>>,
    manifests: Mutex<HashMap<String, Cell<DisplayManifest>>>,
    setups: Mutex<HashMap<(String, DisplayTable, u32), Cell<Setup>>>,
}

#[derive(Clone)]
pub struct DisplayClient {
    inner: Arc<Inner>,
}

/// One HTTP exchange and its wire bytes.
struct Exchange {
    status: u16,
    body: Vec<u8>,
    headers: reqwest::header::HeaderMap,
    up_headers: u64,
    down_headers: u64,
    up_body: u64,
    seconds: f64,
}

/// What a lookup accumulates while its requests run concurrently.
#[derive(Default)]
struct Accounting {
    bytes: ByteCounts,
    cold: ColdFlags,
}

impl DisplayClient {
    /// A client for `base`, optionally connecting through `via` (a byte meter)
    /// while keeping the URL's host, and so TLS server name, unchanged.
    pub fn new(
        base: &str,
        via: Option<SocketAddr>,
        profiles: Option<ProfileCache>,
    ) -> Result<Self, LookupError> {
        let base = reqwest::Url::parse(base).map_err(protocol)?;
        let mut builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(60))
            .http1_only()
            .no_gzip()
            .no_brotli()
            .no_deflate()
            .no_zstd()
            .no_proxy();
        if let Some(via) = via {
            let host = base
                .host_str()
                .ok_or_else(|| protocol("base URL has no host"))?;
            builder = builder.resolve(host, via);
        }
        Ok(Self {
            inner: Arc::new(Inner {
                http: builder
                    .build()
                    .map_err(|e| LookupError::Transport(e.to_string()))?,
                base,
                profiles: profiles.unwrap_or_default(),
                init: tokio::sync::OnceCell::new(),
                map: tokio::sync::Mutex::new(None),
                manifests: Mutex::new(HashMap::new()),
                setups: Mutex::new(HashMap::new()),
            }),
        })
    }

    /// A client with no cached network state and its own connections, sharing
    /// only derived native profiles.
    pub fn fresh(&self, via: Option<SocketAddr>) -> Result<Self, LookupError> {
        Self::new(
            self.inner.base.as_str(),
            via,
            Some(self.inner.profiles.clone()),
        )
    }

    pub fn profiles(&self) -> ProfileCache {
        self.inner.profiles.clone()
    }

    /// The map this client currently holds, if any.
    pub async fn cached_map(&self) -> Option<(DisplayMap, String)> {
        self.inner
            .map
            .lock()
            .await
            .as_ref()
            .map(|cached| (cached.map.clone(), cached.sha256.clone()))
    }

    /// Fetches the map afresh, as a poller does.
    pub async fn refresh_map(&self) -> Result<(DisplayMap, String, u64), LookupError> {
        let accounting = Mutex::new(Accounting::default());
        let cached = self.map(&accounting, true).await?;
        let bytes = accounting.lock().unwrap().bytes;
        Ok((
            cached.map.clone(),
            cached.sha256.clone(),
            bytes.map + bytes.headers_down + bytes.headers_up,
        ))
    }

    fn authority(&self) -> String {
        let host = self.inner.base.host_str().unwrap_or_default();
        match self.inner.base.port() {
            Some(port) => format!("{host}:{port}"),
            None => host.to_string(),
        }
    }

    async fn exchange(&self, path: &str, body: Option<Vec<u8>>) -> Result<Exchange, LookupError> {
        use reqwest::header::{
            HeaderValue, ACCEPT, CONTENT_LENGTH, CONTENT_TYPE, HOST, USER_AGENT,
        };
        let url = self.inner.base.join(path).map_err(protocol)?;
        let method = if body.is_some() { "POST" } else { "GET" };
        let target = match url.query() {
            Some(query) => format!("{}?{query}", url.path()),
            None => url.path().to_string(),
        };
        let mut request = match &body {
            Some(_) => self.inner.http.post(url.clone()),
            None => self.inner.http.get(url.clone()),
        };
        // Every header is set here, so hyper adds none and the request's wire
        // size is exactly what is computed below.
        let mut headers = reqwest::header::HeaderMap::new();
        headers.insert(
            HOST,
            HeaderValue::from_str(&self.authority()).map_err(protocol)?,
        );
        headers.insert(ACCEPT, HeaderValue::from_static("*/*"));
        headers.insert(USER_AGENT, HeaderValue::from_static("txid-display-client"));
        let up_body = body.as_ref().map_or(0, |b| b.len() as u64);
        if let Some(body) = body {
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/octet-stream"),
            );
            headers.insert(CONTENT_LENGTH, HeaderValue::from(body.len() as u64));
            request = request.body(body);
        }
        let up_headers =
            format!("{method} {target} HTTP/1.1\r\n").len() as u64 + header_bytes(&headers) + 2;
        let started = Instant::now();
        let response = request
            .headers(headers)
            .send()
            .await
            .map_err(|e| LookupError::Transport(error_chain(&e)))?;
        let status = response.status();
        let response_headers = response.headers().clone();
        let down_headers = format!(
            "HTTP/1.1 {} {}\r\n",
            status.as_u16(),
            status.canonical_reason().unwrap_or("<none>")
        )
        .len() as u64
            + header_bytes(&response_headers)
            + 2;
        let body = response
            .bytes()
            .await
            .map_err(|e| LookupError::Transport(error_chain(&e)))?
            .to_vec();
        Ok(Exchange {
            status: status.as_u16(),
            body,
            headers: response_headers,
            up_headers,
            down_headers,
            up_body,
            seconds: started.elapsed().as_secs_f64(),
        })
    }

    /// An exchange that must succeed, its metadata bytes attributed by `slot`.
    async fn fetch(
        &self,
        path: &str,
        accounting: &Mutex<Accounting>,
        slot: fn(&mut ByteCounts) -> &mut u64,
    ) -> Result<Exchange, LookupError> {
        let exchange = self.exchange(path, None).await?;
        {
            let mut accounting = accounting.lock().unwrap();
            accounting.bytes.headers_up += exchange.up_headers;
            accounting.bytes.headers_down += exchange.down_headers;
            *slot(&mut accounting.bytes) += exchange.body.len() as u64;
        }
        if exchange.status != 200 {
            return Err(LookupError::Status {
                code: exchange.status,
                path: path.to_string(),
                body: String::from_utf8_lossy(&exchange.body).into_owned(),
            });
        }
        Ok(exchange)
    }

    async fn init(&self, accounting: &Mutex<Accounting>) -> Result<Arc<Init>, LookupError> {
        self.inner
            .init
            .get_or_try_init(|| async {
                let exchange = self
                    .fetch("/v1/txid/init", accounting, |b| &mut b.init)
                    .await?;
                accounting.lock().unwrap().cold.init = true;
                let document: InitDocument =
                    serde_json::from_slice(&exchange.body).map_err(protocol)?;
                let supported = document.schema == display::DISPLAY_SCHEMA
                    && document.codec == txid::CODEC
                    && document.bucket_domain.as_bytes() == display::BUCKET_DOMAIN
                    && document.native_schema == transparent_shard::manifest::SCHEMA;
                Ok(Arc::new(Init {
                    supported,
                    geometries: document
                        .geometries
                        .into_iter()
                        .map(|g| (g.name.clone(), g))
                        .collect(),
                }))
            })
            .await
            .cloned()
    }

    async fn map(
        &self,
        accounting: &Mutex<Accounting>,
        refresh: bool,
    ) -> Result<Arc<CachedMap>, LookupError> {
        let mut held = self.inner.map.lock().await;
        if let (false, Some(cached)) = (refresh, held.as_ref()) {
            return Ok(cached.clone());
        }
        let exchange = self
            .fetch("/v1/txid/shards", accounting, |b| &mut b.map)
            .await?;
        accounting.lock().unwrap().cold.map = true;
        let sha256 = hex::encode(Sha256::digest(&exchange.body));
        if exchange
            .headers
            .get("x-txid-map-sha256")
            .is_some_and(|h| h.as_bytes() != sha256.as_bytes())
        {
            return Err(protocol("map digest header disagrees with the map"));
        }
        let map: DisplayMap = serde_json::from_slice(&exchange.body).map_err(protocol)?;
        if map.to_bytes() != exchange.body {
            return Err(protocol("map is not canonical"));
        }
        let cached = Arc::new(CachedMap { map, sha256 });
        *held = Some(cached.clone());
        Ok(cached)
    }

    fn profile(
        &self,
        init: &Init,
        geometry: &'static Geometry,
        table: Table,
    ) -> Result<Arc<TableProfile>, LookupError> {
        let profile = {
            let cached = self
                .inner
                .profiles
                .lock()
                .unwrap()
                .get(&(geometry.name, table))
                .cloned();
            match cached {
                Some(profile) => profile,
                None => {
                    // The native profile is the history kind's: history schema
                    // and table name. Only the binding is display-specific.
                    let profile = Arc::new(
                        TableProfile::new(
                            transparent_shard::manifest::SCHEMA,
                            geometry.name,
                            table.as_str(),
                            table.rows(geometry),
                            table.row_bytes(geometry),
                        )
                        .map_err(protocol)?,
                    );
                    self.inner
                        .profiles
                        .lock()
                        .unwrap()
                        .insert((geometry.name, table), profile.clone());
                    profile
                }
            }
        };
        let served = init
            .geometries
            .get(geometry.name)
            .map(|g| match table {
                Table::TxPages => &g.txpages,
                _ => &g.txdirectory,
            })
            .ok_or_else(|| protocol("init does not declare the geometry"))?;
        if served.scheme != profile.scheme
            || served.rows != table.rows(geometry)
            || served.row_bytes != table.row_bytes(geometry)
            || served.setup_seed != setup_seed(geometry, table)
        {
            return Err(protocol(format!(
                "served {} parameters for {} differ from the derivation",
                table.as_str(),
                geometry.name
            )));
        }
        Ok(profile)
    }

    async fn manifest(
        &self,
        entry: &DisplayMapEntry,
        accounting: &Mutex<Accounting>,
    ) -> Result<Arc<DisplayManifest>, LookupError> {
        let cell = self
            .inner
            .manifests
            .lock()
            .unwrap()
            .entry(entry.manifest_digest.clone())
            .or_default()
            .clone();
        cell.get_or_try_init(|| async {
            let path = format!(
                "/v1/txid/shards/{}/revisions/{}/manifest",
                entry.shard_id, entry.manifest_digest
            );
            let exchange = self.fetch(&path, accounting, |b| &mut b.manifest).await?;
            accounting.lock().unwrap().cold.manifest = true;
            let manifest: DisplayManifest =
                serde_json::from_slice(&exchange.body).map_err(protocol)?;
            if manifest.canonical_bytes() != exchange.body || !entry.describes(&manifest) {
                return Err(protocol("manifest identity or placement mismatch"));
            }
            manifest.validate().map_err(protocol)?;
            Ok(Arc::new(manifest))
        })
        .await
        .cloned()
    }

    async fn setup(
        &self,
        entry: &DisplayMapEntry,
        table: DisplayTable,
        segment: u32,
        segments: u32,
        profile: &TableProfile,
        accounting: &Mutex<Accounting>,
    ) -> Result<Arc<Setup>, LookupError> {
        let digest = &entry.manifest_digest;
        let cell = self
            .inner
            .setups
            .lock()
            .unwrap()
            .entry((digest.clone(), table, segment))
            .or_default()
            .clone();
        cell.get_or_try_init(|| async {
            let path = format!(
                "/v1/txid/{}/shards/{}/revisions/{digest}/setup/{}/{segment}",
                entry.tier(),
                entry.shard_id,
                table.label()
            );
            let exchange = self.fetch(&path, accounting, |b| &mut b.setup).await?;
            accounting.lock().unwrap().cold.setups += 1;
            let s: serde_json::Value = serde_json::from_slice(&exchange.body).map_err(protocol)?;
            let bucket = match table {
                DisplayTable::Directory(b) => serde_json::json!(b),
                DisplayTable::Pages => serde_json::Value::Null,
            };
            if s["manifest_digest"] != *digest
                || s["shard_id"] != entry.shard_id
                || s["table"] != table.label()
                || s["bucket"] != bucket
                || s["segment"] != segment
                || s["segments"] != segments
                || s["geometry"] != entry.geometry
            {
                return Err(protocol("setup identity mismatch"));
            }
            let public_params = B64
                .decode(
                    s["public_params"]
                        .as_str()
                        .ok_or_else(|| protocol("setup params"))?,
                )
                .map_err(protocol)?;
            if public_params.len() != profile.scheme.public_bytes
                || s["public_params_sha256"] != hex::encode(Sha256::digest(&public_params))
            {
                return Err(protocol("setup digest or length"));
            }
            let epoch: [u8; 8] = hex::decode(
                s["public_params_epoch"]
                    .as_str()
                    .ok_or_else(|| protocol("setup epoch"))?,
            )
            .map_err(protocol)?
            .try_into()
            .map_err(|_| protocol("setup epoch length"))?;
            Ok(Arc::new(Setup {
                public_params,
                epoch,
            }))
        })
        .await
        .cloned()
    }

    /// One query against every segment of the target table, decoded per
    /// segment.
    async fn query(
        &self,
        target: Target,
        row: u64,
        accounting: Arc<Mutex<Accounting>>,
    ) -> Result<(Vec<Vec<u8>>, QueryTrace), LookupError> {
        let Target {
            entry,
            table,
            profile,
            setups,
        } = target;
        let mut trace = QueryTrace {
            table: table.label(),
            row,
            ..QueryTrace::default()
        };
        let prepared = Instant::now();
        let prepare_profile = profile.clone();
        let (secret, upload) =
            tokio::task::spawn_blocking(move || prepare_profile.prepare(row as usize))
                .await
                .map_err(protocol)?
                .map_err(protocol)?;
        trace.prepare_s = prepared.elapsed().as_secs_f64();
        let binding = display::query_binding(&entry.manifest_digest, table);
        let mut body = binding.to_vec();
        body.extend(upload);
        let path = format!(
            "/v1/txid/{}/shards/{}/revisions/{}/query/{}",
            entry.tier(),
            entry.shard_id,
            entry.manifest_digest,
            table.label()
        );
        let exchange = self.exchange(&path, Some(body)).await?;
        trace.http_s = exchange.seconds;
        trace.status = exchange.status;
        trace.up_body = exchange.up_body;
        trace.down_body = exchange.body.len() as u64;
        trace.up_headers = exchange.up_headers;
        trace.down_headers = exchange.down_headers;
        {
            let mut accounting = accounting.lock().unwrap();
            accounting.bytes.query_up += trace.up_body;
            accounting.bytes.query_down += trace.down_body;
            accounting.bytes.headers_up += trace.up_headers;
            accounting.bytes.headers_down += trace.down_headers;
        }
        if exchange.status != 200 {
            return Err(LookupError::Status {
                code: exchange.status,
                path,
                body: String::from_utf8_lossy(&exchange.body).into_owned(),
            });
        }
        let stride = 16 + profile.scheme.response_bytes;
        if exchange.body.len() != setups.len() * stride {
            return Err(protocol("response length"));
        }
        let decoded = Instant::now();
        let response = exchange.body;
        let rows = tokio::task::spawn_blocking(move || {
            response
                .chunks_exact(stride)
                .zip(setups.iter())
                .map(|(frame, setup)| {
                    if frame[..8] != binding || frame[8..16] != setup.epoch {
                        return Err(protocol("response binding or epoch"));
                    }
                    profile
                        .decode(&secret, &setup.public_params, &frame[16..])
                        .map_err(protocol)
                })
                .collect::<Result<Vec<_>, _>>()
        })
        .await
        .map_err(protocol)??;
        trace.decode_s = decoded.elapsed().as_secs_f64();
        Ok((rows, trace))
    }

    /// Sends one query per row, all at once, and returns the decoded rows by
    /// (segment, row).
    async fn queries(
        &self,
        target: &Target,
        rows: &[u64],
        accounting: &Arc<Mutex<Accounting>>,
        report: &mut LookupReport,
    ) -> Result<BTreeMap<(usize, u64), Vec<u8>>, LookupError> {
        let mut tasks = tokio::task::JoinSet::new();
        for (index, row) in rows.iter().copied().enumerate() {
            let (client, target, accounting) = (self.clone(), target.clone(), accounting.clone());
            tasks.spawn(async move { (index, client.query(target, row, accounting).await) });
        }
        let mut done = Vec::with_capacity(rows.len());
        while let Some(joined) = tasks.join_next().await {
            done.push(joined.map_err(protocol)?);
        }
        done.sort_by_key(|(index, _)| *index);
        // Coincident rows come back once per query; keep one copy of each.
        let mut unique = BTreeMap::new();
        let mut first_error = None;
        for (_, result) in done {
            match result {
                Ok((segments, trace)) => {
                    for (segment, bytes) in segments.into_iter().enumerate() {
                        unique.insert((segment, trace.row), bytes);
                    }
                    report.queries.push(trace);
                }
                Err(error) => {
                    first_error.get_or_insert(error);
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(unique),
        }
    }

    /// Looks up `txid` mined at `height`, which the caller's accepted chain
    /// supplies: placement is never discovered by asking.
    pub async fn lookup(
        &self,
        txid: Txid,
        height: Option<u64>,
    ) -> Result<LookupReport, LookupError> {
        let started = Instant::now();
        let mut report = LookupReport::new();
        let Some(height) = height else {
            return Ok(report);
        };
        let accounting = Arc::new(Mutex::new(Accounting::default()));
        let mut refresh = false;
        loop {
            report.queries.clear();
            let attempt = self
                .attempt(txid, height, &accounting, refresh, &mut report)
                .await;
            let accounted = accounting.lock().unwrap();
            report.bytes = accounted.bytes;
            report.cold = accounted.cold.clone();
            drop(accounted);
            match attempt {
                Ok(result) => {
                    report.set(result);
                    report.total_s = started.elapsed().as_secs_f64();
                    return Ok(report);
                }
                Err(error) if error.status() == Some(409) && !refresh => {
                    refresh = true;
                    report.stale_retries += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// One directory query, the history reference's unit of load: candidate
    /// row `choice` (0 or 1) of `txid`'s bucket in the shard `height` names.
    /// Returns the report and whether that row holds `txid`; a report whose
    /// outcome is not `queried` is a placement or support result.
    pub async fn directory_query(
        &self,
        txid: Txid,
        height: u64,
        choice: usize,
    ) -> Result<(LookupReport, bool), LookupError> {
        let started = Instant::now();
        let mut report = LookupReport::new();
        let accounting = Arc::new(Mutex::new(Accounting::default()));
        let mut refresh = false;
        loop {
            report.queries.clear();
            let attempt = async {
                let located = match self
                    .locate(txid, height, &accounting, refresh, &mut report)
                    .await?
                {
                    Ok(located) => located,
                    Err(result) => return Ok(Err(result)),
                };
                let rows = display::candidate_rows(
                    &txid,
                    located.entry.shard_id,
                    located.bucket,
                    located.geometry.directory_rows,
                );
                let decoded = self
                    .directory(
                        &located,
                        &rows[choice % 2..choice % 2 + 1],
                        &accounting,
                        &mut report,
                    )
                    .await?;
                let rows: Vec<Vec<u8>> = decoded.into_values().collect();
                let contains = txid::find_directory(&rows, txid)
                    .map_err(protocol)?
                    .is_some();
                Ok::<_, LookupError>(Ok(contains))
            }
            .await;
            let accounted = accounting.lock().unwrap();
            report.bytes = accounted.bytes;
            report.cold = accounted.cold.clone();
            drop(accounted);
            report.total_s = started.elapsed().as_secs_f64();
            match attempt {
                Ok(Ok(contains)) => {
                    report.outcome = "queried";
                    report.class = "query".into();
                    return Ok((report, contains));
                }
                Ok(Err(result)) => {
                    report.set(result);
                    return Ok((report, false));
                }
                Err(error) if error.status() == Some(409) && !refresh => {
                    refresh = true;
                    report.stale_retries += 1;
                }
                Err(error) => return Err(error),
            }
        }
    }

    /// Init, map, placement and manifest: everything before a query.
    async fn locate(
        &self,
        txid: Txid,
        height: u64,
        accounting: &Arc<Mutex<Accounting>>,
        refresh: bool,
        report: &mut LookupReport,
    ) -> Result<Result<Located, LookupResult>, LookupError> {
        let init = self.init(accounting).await?;
        if !init.supported {
            return Ok(Err(LookupResult::Unsupported));
        }
        let cached = self.map(accounting, refresh).await?;
        report.map_sha256 = Some(cached.sha256.clone());
        if cached.map.schema != display::DISPLAY_SCHEMA {
            return Ok(Err(LookupResult::Unsupported));
        }
        cached.map.check_shape().map_err(protocol)?;
        let Some(entry) = cached.map.shard_for_height(height) else {
            return Ok(Err(LookupResult::PlacementUnknown));
        };
        let entry = Arc::new(entry.clone());
        report.shard_id = Some(entry.shard_id);
        report.sealed = Some(entry.sealed);
        report.digest = Some(entry.manifest_digest.clone());
        let Some(geometry) = display_by_name(&entry.geometry) else {
            return Ok(Err(LookupResult::Unsupported));
        };
        if !init.geometries.contains_key(geometry.name) {
            return Ok(Err(LookupResult::Unsupported));
        }
        let bucket = display::bucket(&txid, entry.n_buckets);
        report.bucket = Some(bucket);
        let manifest = self.manifest(&entry, accounting).await?;
        Ok(Ok(Located {
            init,
            entry,
            geometry,
            bucket,
            manifest,
        }))
    }

    /// The profile and every segment's setup of one table of a located shard.
    async fn target(
        &self,
        located: &Located,
        table: DisplayTable,
        segments: u32,
        accounting: &Arc<Mutex<Accounting>>,
    ) -> Result<Target, LookupError> {
        let profile = self.profile(&located.init, located.geometry, kind(table))?;
        let mut setups = Vec::new();
        for segment in 0..segments {
            setups.push(
                self.setup(
                    &located.entry,
                    table,
                    segment,
                    segments,
                    &profile,
                    accounting,
                )
                .await?,
            );
        }
        Ok(Target {
            entry: located.entry.clone(),
            table,
            profile,
            setups: Arc::new(setups),
        })
    }

    /// Setup for the bucket's directory, then one query per row in `rows`.
    async fn directory(
        &self,
        located: &Located,
        rows: &[u64],
        accounting: &Arc<Mutex<Accounting>>,
        report: &mut LookupReport,
    ) -> Result<BTreeMap<(usize, u64), Vec<u8>>, LookupError> {
        let segments = located.entry.directory_segments[located.bucket as usize];
        let table = DisplayTable::Directory(located.bucket);
        let target = self.target(located, table, segments, accounting).await?;
        self.queries(&target, rows, accounting, report).await
    }

    async fn attempt(
        &self,
        txid: Txid,
        height: u64,
        accounting: &Arc<Mutex<Accounting>>,
        refresh: bool,
        report: &mut LookupReport,
    ) -> Result<LookupResult, LookupError> {
        let located = match self
            .locate(txid, height, accounting, refresh, report)
            .await?
        {
            Ok(located) => located,
            Err(result) => return Ok(result),
        };
        let geometry = located.geometry;

        // Directory phase: exactly two queries, whether or not they coincide.
        let rows = display::candidate_rows(
            &txid,
            located.entry.shard_id,
            located.bucket,
            geometry.directory_rows,
        );
        let decoded = self.directory(&located, &rows, accounting, report).await?;
        let rows: Vec<Vec<u8>> = decoded.into_values().collect();
        let Some(found) = txid::find_directory(&rows, txid).map_err(protocol)? else {
            report.class = "absent".into();
            return Ok(LookupResult::Absent);
        };
        if found.pages == 0 {
            report.class = "inline".into();
            let record = txid::assemble(&found, &[]).map_err(protocol)?;
            return Ok(LookupResult::Found(record));
        }

        // Page phase: exactly `pages` queries, one per page of the extent.
        report.class = format!("pages-{}", found.pages);
        let page_rows = geometry.page_rows;
        let segments = located.manifest.page_segments.len() as u32;
        let first = u64::from(found.first_page)
            .checked_sub(1)
            .ok_or_else(|| protocol("page locator"))?;
        let end = first + u64::from(found.pages);
        if end > u64::from(segments) * page_rows {
            return Err(protocol("page extent outside the shard"));
        }
        let target = self
            .target(&located, DisplayTable::Pages, segments, accounting)
            .await?;
        let pages: Vec<u64> = (first..end).collect();
        let selected: Vec<u64> = pages.iter().map(|page| page % page_rows).collect();
        let decoded = self.queries(&target, &selected, accounting, report).await?;
        let mut rows = Vec::with_capacity(pages.len());
        for page in pages {
            let key = ((page / page_rows) as usize, page % page_rows);
            rows.push(
                decoded
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| protocol("page row missing"))?,
            );
        }
        let record = txid::assemble(&found, &rows).map_err(protocol)?;
        if record.txid != txid {
            return Err(protocol("assembled another transaction"));
        }
        Ok(LookupResult::Found(record))
    }
}

fn header_bytes(headers: &reqwest::header::HeaderMap) -> u64 {
    headers
        .iter()
        .map(|(name, value)| (name.as_str().len() + 2 + value.as_bytes().len() + 2) as u64)
        .sum()
}

fn error_chain(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        text.push_str(": ");
        text.push_str(&cause.to_string());
        source = cause.source();
    }
    text
}

/// SHA-256 of a record's canonical encoding: the exactness oracle fixtures
/// carry.
pub fn record_sha256(record: &TransparentDisplayRecord) -> String {
    hex::encode(Sha256::digest(
        record.encode().expect("a decoded record encodes"),
    ))
}

/// Parses a txid in display (reversed) hex.
pub fn parse_txid(display_hex: &str) -> Option<Txid> {
    let mut bytes: [u8; 32] = hex::decode(display_hex).ok()?.try_into().ok()?;
    bytes.reverse();
    Some(Txid(bytes))
}
