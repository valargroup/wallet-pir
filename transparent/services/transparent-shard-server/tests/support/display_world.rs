//! The in-process tiered display deployment the txid tests share: an
//! archive-owner and a recent-replica worker behind an edge that routes
//! `/v1/txid/archive/` to the owner, the published fixtures, and the wallet
//! client (`transparent-txid-client`) over a blocking HTTP transport that
//! logs, and on request rewrites, every exchange.
#![allow(dead_code)]
use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tower::ServiceExt;
use transparent_events::Txid;
use transparent_shard::display::{self, DisplaySealParams, TXID_2K};
use transparent_shard::txid::{DisplayEntry, DisplayFacts, DisplayOutput, DisplayRecord, Tag};
use transparent_shard_server::assignment::WorkerRole;
use transparent_shard_server::display::live::{DisplayCommand, DisplayLive, DisplayPublication};
use transparent_shard_server::display::service::DisplayRuntime;
use transparent_shard_server::display::synth::{self, Published, ShardSpec};
use transparent_shard_server::service::{ReadinessMode, ServiceConfig};
pub use transparent_txid_client::Provenance;
use transparent_txid_client::{
    Method, ProfileCache, Route, TransportError, TxidDisplayClient, TxidError, TxidLookup,
    TxidReply, TxidRequest, TxidTransport,
};

/// Every directory query uploads this many bytes: the 8-byte binding and the
/// 2,048-row native selection.
pub const QUERY_BYTES: u64 = 40_200;
/// A one-segment directory reply: the binding, the epoch and one response.
pub const REPLY_BYTES: u64 = 5_648;

pub fn params(buckets: u32) -> DisplaySealParams {
    DisplaySealParams {
        n_archive: buckets,
        n_recent: buckets,
        archive_target: 1,
        recent_floor: 1,
        reorg_margin: 1,
    }
}

pub fn spec(shard_id: u64, start: u64, end: u64, sealed: bool, parent: &str) -> ShardSpec {
    ShardSpec {
        shard_id,
        start_height: start,
        end_height: end,
        sealed,
        revision: 0,
        supersedes: String::new(),
        parent_manifest_digest: parent.to_string(),
        n_buckets: 1,
        archive_target: 1,
        geometry: &TXID_2K,
    }
}

/// The publisher's pair for synthetic entry `index` under `seed`.
pub fn pair(seed: u64, index: u64, entry: DisplayEntry) -> DisplayRecord {
    DisplayRecord::new(synth::txid(seed, index), entry).unwrap()
}

/// The publisher's pair for a transaction described by `facts`, with the
/// synthetic txid of (`seed`, `index`).
fn derived(seed: u64, index: u64, mut facts: DisplayFacts) -> DisplayRecord {
    facts.txid = synth::txid(seed, index);
    facts.record().unwrap()
}

fn p2pkh(byte: u8) -> Vec<u8> {
    [&[0x76, 0xa9, 0x14][..], &[byte; 20], &[0x88, 0xac]].concat()
}

/// One entry of every case the format distinguishes, in this order: a
/// complete regular entry, an unshield, an entry with more than two outputs,
/// one with named omissions (two source scripts, an output without an
/// address), one funded by both pools, and a coinbase.
pub fn classes(seed: u64) -> Vec<DisplayRecord> {
    let out = |value, script| DisplayOutput { value, script };
    let mixed = DisplayFacts {
        txid: Txid([0; 32]),
        coinbase: false,
        fee: 10_000,
        has_shielded_components: true,
        spent: vec![out(500_000, p2pkh(1))],
        outputs: vec![out(990_000, p2pkh(2))],
    };
    let coinbase = DisplayFacts {
        coinbase: true,
        fee: 0,
        has_shielded_components: false,
        spent: Vec::new(),
        outputs: vec![out(312_500_000, p2pkh(3)), out(1, p2pkh(4))],
        ..mixed.clone()
    };
    vec![
        pair(seed, 1, synth::record(seed, 1, 25, 0)),
        pair(seed, 2, synth::unshield(seed, 2)),
        pair(seed, 3, synth::record(seed, 3, 25, 4)),
        pair(seed, 22, synth::record(seed, 22, 1, 1)),
        derived(seed, 5, mixed),
        derived(seed, 6, coinbase),
    ]
}

/// Index of the first class in [`records`].
pub const CLASSES: usize = 40;

/// A record of `seed` whose two candidate rows coincide in `shard_id`.
pub fn coincident(seed: u64, shard_id: u64) -> DisplayRecord {
    let index = (1_000u64..)
        .find(|index| {
            let tag = Tag::of(&synth::txid(seed, *index));
            let [a, b] = display::candidate_rows(&tag, shard_id, 0, 2_048);
            a == b
        })
        .unwrap();
    pair(seed, index, synth::record(seed, index, 25, 0))
}

/// Forty mixed entries, then [`classes`], then `extra`.
pub fn records(seed: u64, extra: &[DisplayRecord]) -> Vec<DisplayRecord> {
    let mixed = seed + 1_000;
    let mut records: Vec<DisplayRecord> = synth::records(40, mixed)
        .into_iter()
        .enumerate()
        .map(|(index, entry)| pair(mixed, index as u64, entry))
        .collect();
    records.extend(classes(seed));
    records.extend_from_slice(extra);
    records
}

/// What a shard publishes of `records`: the entries alone.
pub fn entries(records: &[DisplayRecord]) -> Vec<DisplayEntry> {
    records.iter().map(|r| r.entry).collect()
}

pub fn write_shard(root: &Path, spec: &ShardSpec, records: &[DisplayRecord]) -> Published {
    synth::write_shard(root, spec, &entries(records)).unwrap()
}

pub fn config() -> ServiceConfig {
    ServiceConfig {
        cache_bytes: 2 << 30,
        readiness: ReadinessMode::Warm,
        ..ServiceConfig::default()
    }
}

pub fn loaded_only() -> ServiceConfig {
    ServiceConfig {
        readiness: ReadinessMode::LoadedOnly,
        ..config()
    }
}

/// Faults the edge injects in front of the workers.
#[derive(Clone, Default)]
pub struct Faults {
    /// Fail this many queries with 503.
    pub queries: Arc<AtomicUsize>,
    /// Publish an init document naming a codec this client does not know.
    pub foreign_codec: Arc<AtomicBool>,
}

pub fn take(counter: &AtomicUsize) -> bool {
    counter
        .fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| n.checked_sub(1))
        .is_ok()
}

#[derive(Clone)]
pub struct Edge {
    pub archive: Router,
    pub recent: Router,
    pub faults: Faults,
}

pub async fn edge(State(edge): State<Edge>, request: Request) -> Response {
    let path = request.uri().path().to_string();
    if path.contains("/query/") && take(&edge.faults.queries) {
        let _ = axum::body::to_bytes(request.into_body(), usize::MAX).await;
        return (StatusCode::SERVICE_UNAVAILABLE, "injected").into_response();
    }
    let target = if path.starts_with("/v1/txid/archive/") {
        edge.archive
    } else {
        edge.recent
    };
    let response = target.oneshot(request).await.unwrap();
    if path == "/v1/txid/init" && edge.faults.foreign_codec.load(Ordering::Acquire) {
        let (parts, body) = response.into_parts();
        let bytes = axum::body::to_bytes(body, usize::MAX).await.unwrap();
        let mut init: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        init["codec"] = "transparent-txid-display-v9".into();
        let mut parts = parts;
        parts.headers.remove("content-length");
        return Response::from_parts(parts, Body::from(init.to_string()));
    }
    response
}

pub struct Worker {
    pub live: DisplayLive,
    pub runtime: Arc<DisplayRuntime>,
}

impl Worker {
    pub fn new(
        root: &Path,
        role: WorkerRole,
        config: ServiceConfig,
        collect: Vec<PathBuf>,
    ) -> Self {
        let runtime = DisplayRuntime::new(config, None);
        let live = DisplayLive::new(
            runtime.clone(),
            role,
            3,
            root.join(format!("{}.active.json", role.as_str())),
            collect,
            None,
        )
        .unwrap();
        Self { live, runtime }
    }

    pub fn builds(&self) -> u64 {
        self.runtime.metrics.builds.load(Ordering::Relaxed)
    }

    pub async fn command(&self, command: DisplayCommand) -> Result<serde_json::Value, String> {
        self.live
            .command(command)
            .await
            .map(serde_json::Value::Object)
    }

    /// Prepares and activates a publication; returns the prepare reply.
    pub async fn publish(&self, directory: &Path, map_sha256: &str) -> serde_json::Value {
        let expected = self.live.expected();
        let prepared = self
            .command(DisplayCommand::Prepare {
                expected: expected.clone(),
                publication: DisplayPublication {
                    directory: directory.to_path_buf(),
                    map_sha256: map_sha256.to_string(),
                },
            })
            .await
            .unwrap();
        self.command(DisplayCommand::Activate {
            expected,
            map_sha256: map_sha256.to_string(),
        })
        .await
        .unwrap();
        prepared
    }
}

pub struct World {
    pub root: tempfile::TempDir,
    pub archive: Worker,
    pub recent: Worker,
    pub faults: Faults,
    pub url: String,
    pub server: tokio::task::JoinHandle<()>,
    /// Archive shard 0 over 100..=199 and recent shard 1 over 200..=260.
    pub a0: Published,
    pub r0: Published,
    pub a0_records: Vec<DisplayRecord>,
    pub r0_records: Vec<DisplayRecord>,
    pub p0: (PathBuf, String),
}

impl World {
    pub async fn start() -> Self {
        Self::start_with(config()).await
    }

    /// The owner collects `<root>/sealed`, where shards are written, as a
    /// worker collects the root sealed revisions are shipped to for staging.
    pub async fn start_with(config: ServiceConfig) -> Self {
        let root = tempfile::tempdir().unwrap();
        let a0_records = records(10, &[]);
        let r0_records = records(20, &[coincident(20, 1)]);
        let a0 = write_shard(root.path(), &spec(0, 100, 199, true, ""), &a0_records);
        let r0 = write_shard(
            root.path(),
            &spec(1, 200, 260, false, &a0.digest),
            &r0_records,
        );
        let p0 = synth::write_candidate(root.path(), &params(1), &[a0.clone(), r0.clone()], "p0")
            .unwrap();
        let archive = Worker::new(
            root.path(),
            WorkerRole::ArchiveOwner,
            config,
            vec![root.path().join("sealed")],
        );
        let recent = Worker::new(root.path(), WorkerRole::RecentReplica, config, Vec::new());
        // Both start empty and take the publication through control, as a
        // freshly deployed worker does: one table each, the shard's bucket.
        assert_eq!(archive.publish(&p0.0, &p0.1).await["built"], 1);
        assert_eq!(recent.publish(&p0.0, &p0.1).await["built"], 1);
        let faults = Faults::default();
        let app = Router::new().fallback(edge).with_state(Edge {
            archive: archive.live.router(),
            recent: recent.live.router(),
            faults: faults.clone(),
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        Self {
            root,
            archive,
            recent,
            faults,
            url,
            server,
            a0,
            r0,
            a0_records,
            r0_records,
            p0,
        }
    }

    /// A fresh wallet client and transport against the edge.
    pub fn wallet(&self) -> Wallet {
        Wallet::at(&self.url)
    }

    pub async fn get(&self, path: &str) -> (u16, serde_json::Value) {
        let response = reqwest::get(format!("{}{path}", self.url)).await.unwrap();
        let status = response.status().as_u16();
        let body = response.bytes().await.unwrap();
        (status, serde_json::from_slice(&body).unwrap_or_default())
    }
}

impl Drop for World {
    fn drop(&mut self) {
        self.server.abort();
    }
}

/// Native profiles every client of the test binary shares: they are a pure
/// function of geometry and table kind, and costly to derive.
pub fn profiles() -> ProfileCache {
    static PROFILES: OnceLock<ProfileCache> = OnceLock::new();
    PROFILES.get_or_init(ProfileCache::default).clone()
}

/// One exchange as the transport saw it, after any rewrite.
#[derive(Clone, Debug)]
pub struct Sent {
    pub method: Method,
    pub route: Route,
    pub path: String,
    pub template: &'static str,
    pub body: Vec<u8>,
    pub status: u16,
    pub reply_bytes: usize,
}

pub type Intercept = Box<dyn FnMut(&TxidRequest) -> Option<TxidReply>>;
pub type Tamper = Box<dyn FnMut(&TxidRequest, &mut TxidReply)>;
pub type Reroute = Box<dyn FnMut(&str) -> String>;

/// A blocking reqwest transport with fault hooks.
pub struct Http {
    pub client: reqwest::blocking::Client,
    pub base: String,
    pub log: Vec<Sent>,
    /// Answers a request without sending it.
    pub intercept: Option<Intercept>,
    /// Rewrites a reply before the client sees it.
    pub tamper: Option<Tamper>,
    /// Rewrites the path that is actually sent.
    pub reroute: Option<Reroute>,
}

impl Http {
    pub fn new(base: &str) -> Self {
        Self {
            client: reqwest::blocking::Client::builder()
                .timeout(Duration::from_secs(120))
                .build()
                .unwrap(),
            base: base.to_string(),
            log: Vec::new(),
            intercept: None,
            tamper: None,
            reroute: None,
        }
    }

    pub fn take_log(&mut self) -> Vec<Sent> {
        std::mem::take(&mut self.log)
    }
}

impl TxidTransport for Http {
    fn send(&mut self, request: TxidRequest) -> Result<TxidReply, TransportError> {
        let mut reply = match self.intercept.as_mut().and_then(|f| f(&request)) {
            Some(reply) => reply,
            None => {
                let path = match self.reroute.as_mut() {
                    Some(f) => f(request.path()),
                    None => request.path().to_string(),
                };
                let url = format!("{}{path}", self.base);
                let builder = match request.method {
                    Method::Get => self.client.get(url),
                    Method::Post => self
                        .client
                        .post(url)
                        .header("content-type", request.content_type().unwrap())
                        .body(request.body.clone()),
                };
                let response = builder.send().map_err(|e| TransportError(e.to_string()))?;
                let header = |name: &str| {
                    response
                        .headers()
                        .get(name)
                        .and_then(|v| v.to_str().ok())
                        .map(str::to_string)
                };
                TxidReply {
                    status: response.status().as_u16(),
                    retry_after: header("retry-after"),
                    map_sha256: header("x-txid-map-sha256"),
                    body: response
                        .bytes()
                        .map_err(|e| TransportError(e.to_string()))?
                        .to_vec(),
                }
            }
        };
        if let Some(tamper) = self.tamper.as_mut() {
            tamper(&request, &mut reply);
        }
        self.log.push(Sent {
            method: request.method,
            route: request.route,
            path: request.path().to_string(),
            template: request.template(),
            body: request.body.clone(),
            status: reply.status,
            reply_bytes: reply.body.len(),
        });
        Ok(reply)
    }
}

/// A wallet client with its own logging transport.
///
/// Usable from synchronous tests and from multi-threaded async ones: the
/// blocking transport is created and driven in place, off the runtime's
/// scheduling.
pub struct Wallet {
    pub client: TxidDisplayClient,
    pub http: Http,
}

impl Wallet {
    pub fn at(base: &str) -> Self {
        tokio::task::block_in_place(|| Self {
            client: TxidDisplayClient::with_profiles(profiles()),
            http: Http::new(base),
        })
    }

    pub fn lookup(&mut self, txid: Txid, height: u64) -> Result<TxidLookup, TxidError> {
        let Self { client, http } = self;
        tokio::task::block_in_place(|| client.lookup(http, txid.0, height, &|| false))
    }

    /// Looks up a published record and checks the entry is exactly the
    /// publisher's, omission flags included. Returns the transcript.
    pub fn found(&mut self, record: &DisplayRecord, height: u64) -> (Provenance, Vec<Sent>) {
        let lookup = self.lookup(record.txid, height).unwrap();
        (found(lookup, record), self.http.take_log())
    }
}

/// The provenance of a lookup that must have found `record`'s entry, exactly.
pub fn found(lookup: TxidLookup, record: &DisplayRecord) -> Provenance {
    match lookup {
        TxidLookup::Found { entry, provenance } => {
            assert_eq!(entry, record.entry);
            assert_eq!(entry.tag, Tag::of(&record.txid));
            provenance
        }
        other => panic!("{other:?} for a published record"),
    }
}

pub fn routes(log: &[Sent]) -> Vec<Route> {
    log.iter().map(|s| s.route).collect()
}

pub fn posts(log: &[Sent]) -> usize {
    log.iter().filter(|s| s.method == Method::Post).count()
}

/// Request and response body bytes of a transcript.
pub fn body_bytes(log: &[Sent]) -> (usize, usize) {
    (
        log.iter().map(|s| s.body.len()).sum(),
        log.iter().map(|s| s.reply_bytes).sum(),
    )
}

/// A lookup's private queries: exactly two, both to the directory of
/// `bucket`, each a fixed upload answered 200 with one frame per segment of
/// that table.
pub fn assert_queries(log: &[Sent], bucket: u32) {
    let posts: Vec<&Sent> = log.iter().filter(|s| s.method == Method::Post).collect();
    assert_eq!(posts.len(), 2, "{:?}", routes(log));
    for post in posts {
        assert_eq!(post.route, Route::Query);
        assert!(
            post.path.ends_with(&format!("/query/directory-{bucket}")),
            "{}",
            post.path
        );
        assert_eq!(post.body.len() as u64, QUERY_BYTES);
        assert_eq!(post.status, 200);
        assert_eq!(post.reply_bytes as u64 % REPLY_BYTES, 0);
        assert!(post.reply_bytes > 0);
    }
}

/// The shape a privacy observer of one transport sees: the route templates
/// in order, with request and reply body lengths and statuses.
pub fn shape(log: &[Sent]) -> Vec<(&'static str, usize, usize, u16)> {
    log.iter()
        .map(|s| (s.template, s.body.len(), s.reply_bytes, s.status))
        .collect()
}
