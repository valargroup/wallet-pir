//! Chain reorganizations and source outages through the deployed serving path.
//! The `pir-publisher` process follows a JSON-RPC node double. Two workers, a
//! packing router and the query ingress run as separate `pir-query-server`
//! hosts with their own synchronization managers; they reach the publisher
//! through a TLS edge and each other over their private listeners. Wallet
//! probes fetch the manifest, session and one encrypted row from the public
//! listener, and every decrypted record is checked against the contents of the
//! block that the manifest's anchor names.
//!
//! An answer bound to an anchor is legitimate only while that anchor's block is
//! canonical, or within one source authority lifetime after it was replaced:
//! the publisher starts every node check before its lookup, and no role admits
//! a request after the lease from that check ends. Timings come from this
//! host's wall clock with 50 ms observation polling. They are reported as
//! measured here and say nothing about deployed hardware or fleet sizes.
#[path = "support/query_host.rs"]
mod query_host;
mod support;

use axum::{
    body::Bytes,
    http::{HeaderMap, StatusCode},
    routing::post,
    Router,
};
use enhance_shared::{
    client::{record_in_row, QuerySession},
    protocol::{digest, Manifest, ShardSession},
    publication::{AuthorityResponse, PREFIX, SOURCE_AUTHORITY_LIFETIME},
    EnhanceRecord, ACTIVATION_HEIGHT,
};
use pir_e2e_tests::enhance_fixture::{tls_with_discovery, Tls};
use query_host::{free, Host};
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    path::{Path, PathBuf},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::{Duration, Instant},
};
use support::{SharedChain, Tx};

/// Every case prepares deployed-geometry publications (about 1 GB each), so
/// cases run one at a time and their timings are not shared with another.
static DEPLOYED: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());
const LIFETIME_MS: u64 = SOURCE_AUTHORITY_LIFETIME.as_millis() as u64;
const POLL: Duration = Duration::from_millis(50);
const PROBE_INTERVAL: Duration = Duration::from_millis(100);
/// The publisher polls the node every 100 ms; ten seconds also absorbs a
/// node request in flight across the switch.
const WITHDRAWAL_BUDGET: Duration = Duration::from_secs(10);
/// Preparing one deployed-geometry publication took 24-34 s on 3 CPUs for the
/// existing qualification cases; the bound leaves room for a loaded runner.
const PREPARATION_BUDGET: Duration = Duration::from_secs(240);
/// From a replacement publication to its first public answer: each role
/// polls once per second while deferred, so three layers, a 600 MB packing
/// download and hashing fit far inside this bound. The removed failure
/// backoff alone reached 40 s on a layer after three refused passes.
const ADOPTION_BUDGET: Duration = Duration::from_secs(30);
/// Ordinary advancement keeps serving the previous generation, so each layer
/// polls at the steady five-second cadence: three polls, the router's packing
/// load (up to 25 s measured here with four wallets querying on 3 CPUs) and
/// one extra handover poll per layer once five generations are retained.
const ADVANCE_BUDGET: Duration = Duration::from_secs(90);
/// After a node or edge outage ends, requests renew authority on demand; no
/// layer waits for its synchronization cadence or failure backoff.
const OUTAGE_RECOVERY_BUDGET: Duration = Duration::from_secs(5);
/// Long enough for every manager to fail repeatedly and enter backoff.
const OUTAGE: Duration = Duration::from_secs(20);

/// What a decrypted record must be, by construction of its block.
#[derive(Clone, Copy, Debug, PartialEq)]
enum Kind {
    /// The public fixture's frozen record for this action.
    Fixture(usize),
    /// A mixed transaction carrying the fixture action, a hand-computed
    /// 20,000 zatoshi fee and transparent inputs.
    Mixed {
        action: usize,
        transparent_outputs: bool,
    },
}
use Kind::{Fixture, Mixed};
fn mixed(action: usize, transparent_outputs: bool) -> Kind {
    Mixed {
        action,
        transparent_outputs,
    }
}

fn check(record: &EnhanceRecord, kind: Kind) -> Result<(), String> {
    let matches = match kind {
        Fixture(action) => record.as_bytes() == support::fixture_record(action).as_bytes(),
        Mixed {
            action,
            transparent_outputs,
        } => {
            let fixture = support::fixture_record(action);
            let flags = record.as_bytes()[enhance_shared::RECORD_FLAGS_OFFSET];
            record.enc_ciphertext_suffix() == fixture.enc_ciphertext_suffix()
                && record.cv_net() == fixture.cv_net()
                && record.out_ciphertext() == fixture.out_ciphertext()
                && record.metadata().fee_zatoshis() == Some(20_000)
                && flags & enhance_shared::FLAG_HAS_TRANSPARENT_INPUTS != 0
                && (flags & enhance_shared::FLAG_HAS_TRANSPARENT_OUTPUTS != 0)
                    == transparent_outputs
        }
    };
    matches
        .then_some(())
        .ok_or_else(|| format!("record is not {kind:?}"))
}

/// Contents and canonical intervals (ms since the case started) of every
/// block that has been the chain's anchor.
#[derive(Default)]
struct Anchor {
    kinds: Vec<Kind>,
    canonical: Vec<(u64, Option<u64>)>,
}
type Anchors = Arc<Mutex<HashMap<String, Anchor>>>;

/// The node double's chain and the expected records of each anchor.
struct ChainFixture {
    shared: SharedChain,
    blocks: BTreeMap<u64, (String, Vec<Kind>)>,
    anchors: Anchors,
    t0: Instant,
}
impl ChainFixture {
    fn new(t0: Instant) -> Self {
        let shared = SharedChain::default();
        // The root follower confirms the node's genesis on every pass.
        let (raw, hash) = support::block(&[support::fixture()], u8::MAX);
        shared.lock().unwrap().blocks.insert(0, (raw, hash, 0));
        Self {
            shared,
            blocks: Default::default(),
            anchors: Default::default(),
            t0,
        }
    }
    /// Make `transactions` the canonical block at `height`, replacing the
    /// block there and every block above it. The double's block hash depends
    /// only on `nonce`, so each distinct block needs its own; reusing a
    /// block's nonce and contents restores that block.
    fn put(&mut self, height: u64, transactions: &[Tx], kinds: &[Kind], nonce: u8) -> String {
        let hash = {
            let mut chain = self.shared.lock().unwrap();
            chain.blocks.retain(|at, _| *at < height);
            chain.tip = height - 1;
            chain.push_block(height, transactions, nonce)
        };
        let now = self.t0.elapsed().as_millis() as u64;
        let mut anchors = self.anchors.lock().unwrap();
        for (_, (replaced, _)) in self.blocks.split_off(&height) {
            for interval in &mut anchors.get_mut(&replaced).unwrap().canonical {
                interval.1.get_or_insert(now);
            }
        }
        let mut cumulative = self
            .blocks
            .values()
            .next_back()
            .map(|block| block.1.clone())
            .unwrap_or_default();
        cumulative.extend_from_slice(kinds);
        let anchor = anchors.entry(hash.clone()).or_default();
        assert!(
            anchor.canonical.is_empty() || anchor.kinds == cumulative,
            "nonce {nonce} names another block"
        );
        anchor.kinds = cumulative.clone();
        anchor.canonical.push((now, None));
        self.blocks.insert(height, (hash.clone(), cumulative));
        hash
    }
}

/// The transactions mixed blocks draw on; `earlier` is fetched through
/// `getrawtransaction`, the others are spent inside their block.
struct Transactions {
    same_block: Tx,
    without_outputs: Tx,
    with_outputs: Tx,
}
impl Transactions {
    fn new(chain: &SharedChain) -> Self {
        let earlier = support::funding(1, &[300_000]);
        let same_block = support::funding(2, &[420_000, 99_000]);
        chain.lock().unwrap().add_transaction(&earlier);
        Self {
            without_outputs: support::Mixed::new(
                vec![support::spend(&same_block, 0)],
                &[],
                -400_000,
            )
            .build(),
            with_outputs: support::Mixed::new(
                vec![support::spend(&same_block, 1), support::spend(&earlier, 0)],
                &[100_000],
                -279_000,
            )
            .build(),
            same_block,
        }
    }
}

/// The node double behind a switch that refuses every call, as an unreachable
/// node does. Pooled connections to the double keep answering after its
/// listener stops, so the outage is injected in front of it.
struct Node {
    url: String,
    down: Arc<AtomicBool>,
    _tasks: [tokio::task::JoinHandle<()>; 2],
}
impl Node {
    async fn start(chain: SharedChain) -> Self {
        let (inner, double) = support::serve_chain(chain).await;
        let down = Arc::new(AtomicBool::new(false));
        let switch = down.clone();
        let http = reqwest::Client::new();
        let (url, front) = support::serve(Router::new().route(
            "/",
            post(move |headers: HeaderMap, body: Bytes| {
                let (http, inner) = (http.clone(), inner.clone());
                let down = switch.load(Ordering::Acquire);
                async move {
                    let json = [("content-type", "application/json")];
                    if down {
                        return (StatusCode::SERVICE_UNAVAILABLE, json, Bytes::new());
                    }
                    let mut request = http.post(&inner).body(body);
                    for name in ["authorization", "content-type"] {
                        if let Some(value) = headers.get(name) {
                            request = request.header(name, value.clone());
                        }
                    }
                    match request.send().await {
                        Ok(response) => (
                            StatusCode::from_u16(response.status().as_u16()).unwrap(),
                            json,
                            response.bytes().await.unwrap_or_default(),
                        ),
                        Err(_) => (StatusCode::BAD_GATEWAY, json, Bytes::new()),
                    }
                }
            }),
        ))
        .await;
        Self {
            url,
            down,
            _tasks: [double, front],
        }
    }
}

/// The `pir-publisher` process and its TLS edge, whose fault file can refuse
/// every consumer request.
struct Publisher {
    child: tokio::process::Child,
    log: PathBuf,
    tls: Tls,
    edge: PathBuf,
}
impl Publisher {
    async fn start(root: &Path, node: &str) -> Self {
        let (management, listen, artifact) = (free(), free(), free());
        let edge = root.join("edge");
        std::fs::create_dir_all(&edge).unwrap();
        let tls = tls_with_discovery(&edge, &format!("http://{artifact}"), Some(&management)).await;
        let config = root.join("publisher.json");
        let document = json!({
            "management_listen": management,
            "chain": {"rpc_url": node, "rpc_cookie": support::cookie(root), "poll_ms": 100},
            "enhance": {"enabled": true, "settings": {
                "listen": listen, "artifact_listen": artifact,
                "data_dir": root.join("publisher"), "prevout_cache_outputs": 1000,
            }},
        });
        std::fs::write(&config, serde_json::to_vec_pretty(&document).unwrap()).unwrap();
        let log = root.join("publisher.log");
        let file = std::fs::File::create(&log).unwrap();
        let child = tokio::process::Command::new(env!("CARGO_BIN_EXE_pir-publisher-test"))
            .arg("--config")
            .arg(&config)
            .env(
                "RUST_LOG",
                std::env::var("RUST_LOG").unwrap_or("info".into()),
            )
            .kill_on_drop(true)
            .stdout(file.try_clone().unwrap())
            .stderr(file)
            .spawn()
            .unwrap();
        Self {
            child,
            log,
            tls,
            edge,
        }
    }
    /// The consumers' publisher source. Budgets follow the deployment
    /// inventory example (8 GiB), so the fixture adds no tighter cache bound.
    fn source(&self) -> serde_json::Value {
        json!({
            "publisher_origin": self.tls.origin,
            "trusted_ca": self.edge.join("ca.pem"),
            "download_budget_bytes": 8u64 << 30,
            "cache_limit_bytes": 8u64 << 30,
        })
    }
    fn refuse_consumers(&self, refused: bool) {
        let fault = if refused { "refused-body" } else { "" };
        std::fs::write(self.edge.join("fault"), fault).unwrap();
    }
}

/// One observed state of a source; the timeline records each change.
#[derive(Clone, Debug, PartialEq)]
enum Seen {
    Publisher {
        eligible: bool,
        manifest: Option<String>,
        anchor: Option<String>,
        generation: Option<u64>,
    },
    Worker {
        active: Option<String>,
        ready: bool,
    },
    Router {
        active: Option<String>,
        generation: Option<u64>,
        ready: bool,
        retained: usize,
        slots_full: bool,
        pins: usize,
    },
    Ingress {
        anchor: Option<String>,
        generation: Option<u64>,
    },
    Refused(u16),
    Unreachable,
}

#[derive(Clone, Debug, PartialEq)]
enum Outcome {
    Answered,
    Refused(&'static str, u16),
    Failed(&'static str),
    Wrong(String),
}

#[derive(Clone, Debug)]
struct Probe {
    wallet: &'static str,
    sent: u64,
    done: u64,
    anchor: Option<String>,
    generation: Option<u64>,
    outcome: Outcome,
}

#[derive(Default)]
struct Observed {
    probes: Vec<Probe>,
    timeline: Vec<(u64, String, Seen)>,
    last: HashMap<String, Seen>,
}
impl Observed {
    fn see(&mut self, at: u64, source: &str, seen: Seen) {
        if self.last.get(source) != Some(&seen) {
            self.last.insert(source.into(), seen.clone());
            self.timeline.push((at, source.into(), seen));
        }
    }
    fn first(&self, after: u64, source: &str, f: impl Fn(&Seen) -> bool) -> Option<u64> {
        self.timeline
            .iter()
            .find(|(at, from, seen)| *at >= after && from == source && f(seen))
            .map(|entry| entry.0)
    }
    /// The first answer bound to `anchor` among requests sent at or after `after`.
    fn first_answer(&self, anchor: &str, after: u64) -> Option<&Probe> {
        self.probes.iter().find(|probe| {
            probe.sent >= after
                && probe.outcome == Outcome::Answered
                && probe.anchor.as_deref() == Some(anchor)
        })
    }
}

/// A wallet that binds to the published manifest and queries one row. A
/// sticky wallet keeps its binding until refused as stale, as the client
/// library does; the other rebinds before every query.
struct Wallet {
    name: &'static str,
    sticky: bool,
    binding: Option<(Manifest, Arc<QuerySession>)>,
    sessions: HashMap<String, Arc<QuerySession>>,
}
impl Wallet {
    async fn bind(&mut self, http: &reqwest::Client, origin: &str) -> Result<(), Outcome> {
        if self.sticky && self.binding.is_some() {
            return Ok(());
        }
        let response = http
            .get(format!("{origin}/v1/enhance/init"))
            .send()
            .await
            .map_err(|_| Outcome::Failed("init"))?;
        if !response.status().is_success() {
            return Err(Outcome::Refused("init", response.status().as_u16()));
        }
        let manifest: Manifest = response.json().await.map_err(|_| Outcome::Failed("init"))?;
        let id = digest(&manifest);
        if let Some(session) = self.sessions.get(&id) {
            self.binding = Some((manifest, session.clone()));
            return Ok(());
        }
        let session = hex::encode(manifest.session_id(0).map_err(Outcome::Wrong)?);
        let response = http
            .get(format!("{origin}/v1/enhance/session/{session}"))
            .send()
            .await
            .map_err(|_| Outcome::Failed("session"))?;
        if !response.status().is_success() {
            return Err(Outcome::Refused("session", response.status().as_u16()));
        }
        let document: ShardSession = response
            .json()
            .await
            .map_err(|_| Outcome::Failed("session"))?;
        let session = Arc::new(
            QuerySession::new(&manifest, document)
                .map_err(|e| Outcome::Wrong(format!("session: {e:?}")))?,
        );
        if self.sessions.len() >= 2 {
            self.sessions.clear();
        }
        self.sessions.insert(id, session.clone());
        self.binding = Some((manifest, session));
        Ok(())
    }
    async fn query(&mut self, http: &reqwest::Client, origin: &str, anchors: &Anchors) -> Outcome {
        let (manifest, session) = self.binding.clone().unwrap();
        let query = match session.prepare_row(0) {
            Ok(query) => query,
            Err(e) => return Outcome::Wrong(format!("prepare: {e:?}")),
        };
        let response = match http
            .post(format!("{origin}/v1/enhance/query"))
            .body(query.body().to_vec())
            .send()
            .await
        {
            Ok(response) => response,
            Err(_) => return Outcome::Failed("query"),
        };
        let status = response.status().as_u16();
        if status != 200 {
            if matches!(status, 409 | 410) {
                self.binding = None;
            }
            return Outcome::Refused("query", status);
        }
        let Ok(bytes) = response.bytes().await else {
            return Outcome::Failed("query");
        };
        let row = match session.decode(query, &bytes) {
            Ok(row) => row,
            Err(e) => return Outcome::Wrong(format!("decode: {e:?}")),
        };
        let Some(kinds) = anchors
            .lock()
            .unwrap()
            .get(&manifest.anchor_block_hash)
            .map(|anchor| anchor.kinds.clone())
        else {
            return Outcome::Wrong(format!("unknown anchor {}", manifest.anchor_block_hash));
        };
        if manifest.coverage.records != kinds.len() as u64 {
            return Outcome::Wrong(format!(
                "coverage {} for {} anchor records",
                manifest.coverage.records,
                kinds.len()
            ));
        }
        for (position, kind) in kinds.into_iter().enumerate() {
            let (_, row_index, slot) = manifest.coverage.locate(position as u64).unwrap();
            assert_eq!(row_index, 0, "fixture coverage fits one row");
            if let Err(e) = record_in_row(&row, slot)
                .map_err(|e| format!("{e:?}"))
                .and_then(|record| check(&record, kind))
            {
                return Outcome::Wrong(format!("position {position}: {e}"));
            }
        }
        Outcome::Answered
    }
}

async fn run_wallet(
    mut wallet: Wallet,
    origin: String,
    anchors: Anchors,
    observed: Arc<Mutex<Observed>>,
    t0: Instant,
    stop: Arc<AtomicBool>,
) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
        .unwrap();
    while !stop.load(Ordering::Acquire) {
        let sent = t0.elapsed().as_millis() as u64;
        let outcome = match wallet.bind(&http, &origin).await {
            Err(refused) => refused,
            Ok(()) => wallet.query(&http, &origin, &anchors).await,
        };
        let (anchor, generation) = match &outcome {
            Outcome::Refused("init" | "session", _) | Outcome::Failed("init" | "session") => {
                (None, None)
            }
            _ => wallet
                .binding
                .as_ref()
                .map(|(m, _)| (Some(m.anchor_block_hash.clone()), Some(m.generation)))
                .unwrap_or_default(),
        };
        observed.lock().unwrap().probes.push(Probe {
            wallet: wallet.name,
            sent,
            done: t0.elapsed().as_millis() as u64,
            anchor,
            generation,
            outcome,
        });
        tokio::time::sleep(PROBE_INTERVAL).await;
    }
}

/// Observed origins polled by the monitor.
#[derive(Clone)]
struct Origins {
    public: String,
    workers: Vec<String>,
    router_control: String,
}

async fn json(http: &reqwest::Client, url: String) -> Result<serde_json::Value, Seen> {
    let response = http.get(url).send().await.map_err(|_| Seen::Unreachable)?;
    if !response.status().is_success() {
        return Err(Seen::Refused(response.status().as_u16()));
    }
    response.json().await.map_err(|_| Seen::Unreachable)
}

async fn monitor(
    origins: Origins,
    publisher: reqwest::Client,
    publisher_origin: String,
    observed: Arc<Mutex<Observed>>,
    t0: Instant,
    stop: Arc<AtomicBool>,
) {
    let http = reqwest::Client::builder()
        .timeout(Duration::from_secs(2))
        .build()
        .unwrap();
    let text = |value: &serde_json::Value| value.as_str().map(str::to_owned);
    while !stop.load(Ordering::Acquire) {
        let current = match json(&publisher, format!("{publisher_origin}{PREFIX}current")).await {
            Ok(value) => match serde_json::from_value::<AuthorityResponse>(value) {
                Ok(response) => {
                    let manifest = response.current.manifest.as_ref();
                    Seen::Publisher {
                        eligible: response.current.eligible,
                        manifest: manifest.map(digest),
                        anchor: manifest.map(|m| m.anchor_block_hash.clone()),
                        generation: manifest.map(|m| m.generation),
                    }
                }
                Err(_) => Seen::Unreachable,
            },
            Err(seen) => seen,
        };
        let at = t0.elapsed().as_millis() as u64;
        observed.lock().unwrap().see(at, "publisher", current);
        for (index, worker) in origins.workers.iter().enumerate() {
            let seen = match json(&http, format!("{worker}/internal/health")).await {
                Ok(health) => Seen::Worker {
                    active: text(&health["active"]),
                    ready: health["ready"] == true,
                },
                Err(seen) => seen,
            };
            let at = t0.elapsed().as_millis() as u64;
            observed
                .lock()
                .unwrap()
                .see(at, &format!("worker-{index}"), seen);
        }
        let seen = match json(&http, format!("{}/internal/health", origins.router_control)).await {
            Ok(health) => Seen::Router {
                active: text(&health["active"]),
                generation: health["generation"].as_u64(),
                ready: health["ready"] == true,
                retained: health["handover"]["retained"]
                    .as_array()
                    .map_or(0, Vec::len),
                slots_full: health["handover"]["retention_slots_full"] == true,
                pins: health["handover"]["pins"].as_array().map_or(0, Vec::len),
            },
            Err(seen) => seen,
        };
        let at = t0.elapsed().as_millis() as u64;
        observed.lock().unwrap().see(at, "router", seen);
        let seen = match json(&http, format!("{}/v1/enhance/init", origins.public)).await {
            Ok(manifest) => Seen::Ingress {
                anchor: text(&manifest["anchor_block_hash"]),
                generation: manifest["generation"].as_u64(),
            },
            Err(seen) => seen,
        };
        let at = t0.elapsed().as_millis() as u64;
        observed.lock().unwrap().see(at, "ingress", seen);
        tokio::time::sleep(POLL).await;
    }
}

/// The whole path: node, publisher process, two workers, packing router and
/// query ingress, with wallets and the monitor running in the background.
struct Stack {
    _root: tempfile::TempDir,
    t0: Instant,
    chain: ChainFixture,
    node: Node,
    publisher: Publisher,
    hosts: Vec<Host>,
    observed: Arc<Mutex<Observed>>,
    stop: Arc<AtomicBool>,
    tasks: Vec<tokio::task::JoinHandle<()>>,
}
impl Stack {
    async fn start(chain: ChainFixture, wallets: usize) -> Self {
        let t0 = chain.t0;
        let root = tempfile::tempdir().unwrap();
        let node = Node::start(chain.shared.clone()).await;
        let publisher = Publisher::start(root.path(), &node.url).await;
        let source = publisher.source();
        let workers = [free(), free()];
        let (router, router_control, ingress_control, public) = (free(), free(), free(), free());
        let mut roles = Vec::new();
        for (index, worker) in workers.iter().enumerate() {
            let mut synchronization = source.clone();
            synchronization["shards"] = json!([0]);
            roles.push((
                format!("worker-{index}"),
                json!({"role": "worker", "listen": worker, "synchronization": synchronization}),
            ));
        }
        let mut synchronization = source.clone();
        synchronization["workers"] =
            json!({"0": workers.iter().map(|w| format!("http://{w}")).collect::<Vec<_>>()});
        roles.push((
            "router".into(),
            json!({"role": "packing-router", "listen": router,
                "control_listen": router_control, "synchronization": synchronization}),
        ));
        let mut synchronization = source.clone();
        synchronization["routers"] = json!({"0": [{
            "control_origin": format!("http://{router_control}"),
            "query_origin": format!("http://{router}"),
        }]});
        roles.push((
            "ingress".into(),
            json!({"role": "query-ingress", "control_listen": ingress_control,
                "synchronization": synchronization}),
        ));
        let mut hosts = Vec::new();
        for (name, mut settings) in roles {
            settings["data_dir"] = root.path().join(&name).to_str().unwrap().into();
            let mut document = json!({
                "management_listen": free(),
                "enhance": {"enabled": true, "settings": settings},
            });
            if name == "ingress" {
                document["public_listen"] = json!(public);
            }
            hosts.push(Host::start(root.path(), &name, document).await);
        }
        let origins = Origins {
            public: format!("http://{public}"),
            workers: workers.iter().map(|w| format!("http://{w}")).collect(),
            router_control: format!("http://{router_control}"),
        };
        let observed = Arc::new(Mutex::new(Observed::default()));
        let stop = Arc::new(AtomicBool::new(false));
        let mut tasks = vec![tokio::spawn(monitor(
            origins.clone(),
            publisher.tls.client.clone(),
            publisher.tls.origin.clone(),
            observed.clone(),
            t0,
            stop.clone(),
        ))];
        for index in 0..wallets {
            let wallet = Wallet {
                name: ["fresh-0", "sticky-0", "fresh-1", "sticky-1"][index],
                sticky: index % 2 == 1,
                binding: None,
                sessions: Default::default(),
            };
            tasks.push(tokio::spawn(run_wallet(
                wallet,
                origins.public.clone(),
                chain.anchors.clone(),
                observed.clone(),
                t0,
                stop.clone(),
            )));
        }
        let mut stack = Self {
            _root: root,
            t0,
            chain,
            node,
            publisher,
            hosts,
            observed,
            stop,
            tasks,
        };
        stack
            .until("the first publication", PREPARATION_BUDGET, |o| {
                o.first(0, "publisher", |s| {
                    matches!(s, Seen::Publisher { eligible: true, .. })
                })
            })
            .await;
        for host in &mut stack.hosts {
            host.ready().await;
        }
        stack
    }
    fn now(&self) -> u64 {
        self.t0.elapsed().as_millis() as u64
    }
    fn logs(&self) -> String {
        let tail = |text: String| text[text.len().saturating_sub(4000)..].to_string();
        let mut logs = format!(
            "publisher:\n{}",
            tail(std::fs::read_to_string(&self.publisher.log).unwrap_or_default())
        );
        for host in &self.hosts {
            logs += &format!("\nhost:\n{}", tail(host.log()));
        }
        logs
    }
    /// Polls `f` until it yields, failing on a wrong answer, a stopped
    /// process or the budget.
    async fn until<T>(
        &mut self,
        what: &str,
        budget: Duration,
        f: impl Fn(&Observed) -> Option<T>,
    ) -> T {
        let deadline = Instant::now() + budget;
        loop {
            {
                let observed = self.observed.lock().unwrap();
                if let Some(wrong) = observed
                    .probes
                    .iter()
                    .find(|p| matches!(p.outcome, Outcome::Wrong(_)))
                {
                    panic!("wrong answer: {wrong:?}");
                }
                if let Some(value) = f(&observed) {
                    return value;
                }
            }
            if let Some(status) = self.publisher.child.try_wait().unwrap() {
                panic!("publisher exited with {status}\n{}", self.logs());
            }
            for host in &mut self.hosts {
                host.assert_running();
            }
            if Instant::now() >= deadline {
                self.retain("failure", json!(what));
                panic!("{what} not observed within {budget:?}\n{}", self.logs());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    async fn wait_until(&mut self, at: u64) {
        while self.now() < at {
            let remaining = at - self.now();
            tokio::time::sleep(Duration::from_millis(remaining.min(200))).await;
            self.until("progress", Duration::from_secs(1), |_| Some(()))
                .await;
        }
    }
    /// Answers sent at or after `from` and refusals among them.
    fn refusals_since(&self, from: u64) -> (usize, usize) {
        let observed = self.observed.lock().unwrap();
        let sent = observed.probes.iter().filter(|p| p.sent >= from);
        let (mut answered, mut refused) = (0, 0);
        for probe in sent {
            match probe.outcome {
                Outcome::Answered => answered += 1,
                _ => refused += 1,
            }
        }
        (answered, refused)
    }
    /// Every answer was bound to a block that was canonical at most one
    /// authority lifetime before the request was sent.
    fn assert_legitimate(&self) {
        let observed = self.observed.lock().unwrap();
        let anchors = self.chain.anchors.lock().unwrap();
        for probe in &observed.probes {
            if probe.outcome != Outcome::Answered {
                continue;
            }
            let anchor = &anchors[probe.anchor.as_ref().unwrap()];
            assert!(
                anchor.canonical.iter().any(|(from, until)| {
                    *from <= probe.done
                        && until.is_none_or(|until| probe.sent < until + LIFETIME_MS)
                }),
                "answered from an invalidated block: {probe:?}, canonical {:?}",
                anchor.canonical
            );
        }
    }
    /// Timestamps (ms since the change at `changed`) of each layer adopting
    /// the publication anchored at `anchor`, and the answers around it.
    fn recovery(&self, label: &str, changed: u64, anchor: &str) -> serde_json::Value {
        let observed = self.observed.lock().unwrap();
        let published = observed
            .timeline
            .iter()
            .find_map(|(at, source, seen)| match seen {
                Seen::Publisher {
                    eligible: true,
                    manifest: Some(manifest),
                    anchor: Some(seen),
                    generation,
                } if *at >= changed && source == "publisher" && seen == anchor => {
                    Some((*at, manifest.clone(), *generation))
                }
                _ => None,
            });
        let Some((published, manifest, generation)) = published else {
            return json!({"label": label, "error": "no replacement publication observed"});
        };
        let withdrawn = observed.first(changed, "publisher", |s| {
            matches!(
                s,
                Seen::Publisher {
                    eligible: false,
                    ..
                }
            )
        });
        let workers = (0..2)
            .map(|index| {
                observed.first(changed, &format!("worker-{index}"), |s| {
                    matches!(s, Seen::Worker { active: Some(a), ready: true } if *a == manifest)
                })
            })
            .collect::<Option<Vec<_>>>()
            .map(|ready| ready.into_iter().max().unwrap());
        let router = observed.first(
            changed,
            "router",
            |s| matches!(s, Seen::Router { active: Some(a), ready: true, .. } if *a == manifest),
        );
        let ingress = observed.first(changed, "ingress", |s| {
            matches!(s, Seen::Ingress { anchor: Some(a), generation: g } if a == anchor && *g == generation)
        });
        let first = observed.first_answer(anchor, changed).map(|p| p.done);
        let last_before = first.and_then(|first| {
            observed
                .probes
                .iter()
                .filter(|p| {
                    p.outcome == Outcome::Answered
                        && p.done < first
                        && p.anchor.as_deref() != Some(anchor)
                })
                .map(|p| p.done)
                .max()
        });
        let publications = observed
            .timeline
            .iter()
            .filter_map(|(at, _, seen)| match seen {
                Seen::Publisher {
                    eligible: true,
                    manifest: Some(manifest),
                    anchor: Some(seen),
                    ..
                } if *at >= changed && seen == anchor => Some(manifest),
                _ => None,
            })
            .collect::<std::collections::BTreeSet<_>>()
            .len();
        let at = |time: Option<u64>| time.map(|time| time as i64 - changed as i64);
        let alert = first.map(|first| alert_model(&observed.probes, changed, first));
        json!({
            "label": label,
            "generation": generation,
            "publications": publications,
            "ms_after_change": {
                "withdrawal": at(withdrawn),
                "replacement_published": at(Some(published)),
                "workers_ready": at(workers),
                "router_active": at(router),
                "ingress_active": at(ingress),
                "first_public_answer": at(first),
                "last_prior_answer": at(last_before),
            },
            "adoption_ms": first.map(|first| first - published),
            "request_outage_ms": first.zip(last_before).map(|(first, last)| first - last),
            "modeled_query_5xx_alert": alert,
        })
    }
    /// One publication per change: work reserved again while the replacement
    /// prepared must not advance consumers to an identical generation.
    fn assert_single_publication(&self, result: &serde_json::Value) {
        assert_eq!(result["publications"], 1, "{result}");
    }
    fn report(&self, case: &str, results: serde_json::Value) {
        let report = json!({"case": case, "results": results});
        eprintln!("{}", serde_json::to_string_pretty(&report).unwrap());
        self.retain(case, report);
    }
    /// With `WALLET_PIR_RECOVERY_REPORT` set, keeps the report, the timeline,
    /// every probe and the process logs there.
    fn retain(&self, case: &str, report: serde_json::Value) {
        let Some(dir) = std::env::var_os("WALLET_PIR_RECOVERY_REPORT") else {
            return;
        };
        let dir = Path::new(&dir).join(case);
        std::fs::create_dir_all(&dir).unwrap();
        let observed = self.observed.lock().unwrap();
        let timeline = observed
            .timeline
            .iter()
            .map(|(at, source, seen)| json!([at, source, format!("{seen:?}")]))
            .collect::<Vec<_>>();
        let probes = observed
            .probes
            .iter()
            .map(|p| {
                json!([
                    p.wallet,
                    p.sent,
                    p.done,
                    p.generation,
                    format!("{:?}", p.outcome)
                ])
            })
            .collect::<Vec<_>>();
        let document = json!({"report": report, "timeline": timeline, "probes": probes});
        std::fs::write(
            dir.join("report.json"),
            serde_json::to_vec_pretty(&document).unwrap(),
        )
        .unwrap();
        std::fs::copy(&self.publisher.log, dir.join("publisher.log")).unwrap();
        for (index, host) in self.hosts.iter().enumerate() {
            std::fs::write(dir.join(format!("host-{index}.log")), host.log()).unwrap();
        }
    }
    async fn shutdown(mut self) {
        self.stop.store(true, Ordering::Release);
        for task in self.tasks.drain(..) {
            task.await.unwrap();
        }
        for host in self.hosts.drain(..) {
            host.stop().await;
        }
    }
}

/// The default `query_5xx` rule, applied to one wallet's requests between the
/// change at `start` and the first answer after recovery at `end`: more than
/// 5% 5xx among at least 10 responses in the trailing five minutes, sampled
/// every 15 s, cleared by two consecutive healthy samples. Traffic outside
/// that span is taken as successful at the observed rate. This models the
/// rule from measured outcomes; it is not a deployed observation.
fn alert_model(probes: &[Probe], start: u64, end: u64) -> serde_json::Value {
    const WINDOW: u64 = 300_000;
    const SAMPLE: u64 = 15_000;
    let observed: Vec<_> = probes
        .iter()
        .filter(|p| p.wallet == "fresh-0" && p.done >= start && p.done <= end)
        .map(|p| {
            (
                p.done,
                matches!(p.outcome, Outcome::Refused(_, status) if status >= 500),
            )
        })
        .collect();
    let first = observed.first().map_or(start, |p| p.0);
    let rate = observed.len() as f64 / (end - first).max(1) as f64;
    let (mut fired, mut healthy, mut cleared) = (None, 0, None);
    let mut sample = first + SAMPLE;
    while sample <= end + WINDOW + 2 * SAMPLE {
        let from = sample.saturating_sub(WINDOW);
        let inside: Vec<_> = observed
            .iter()
            .filter(|p| p.0 > from && p.0 <= sample)
            .collect();
        let errors = inside.iter().filter(|p| p.1).count() as f64;
        let outside = first.saturating_sub(from).min(WINDOW) + sample.saturating_sub(end.max(from));
        let requests = inside.len() as f64 + rate * outside as f64;
        if requests >= 10.0 && errors / requests > 0.05 {
            fired.get_or_insert(sample);
            healthy = 0;
            cleared = None;
        } else if fired.is_some() {
            healthy += 1;
            if healthy == 2 {
                cleared.get_or_insert(sample);
            }
        }
        sample += SAMPLE;
    }
    let relative = |at: Option<u64>| at.map(|at| at as i64 - end as i64);
    json!({
        "fires_ms_after_first_answer": relative(fired),
        "clears_ms_after_first_answer": relative(cleared),
    })
}

/// A one-block reorganization withdraws authority and serves only the
/// replacement; a second reorganization that returns to the original block
/// does the same; a node outage and a publisher edge outage refuse wallets
/// after one authority lifetime and recover on demand once they end.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(
    not(feature = "qualification-tests"),
    ignore = "deployed geometry: qualification-tests"
)]
async fn reorganizations_and_source_outages_never_answer_invalidated_records() {
    let _serial = DEPLOYED.lock().await;
    let mut chain = ChainFixture::new(Instant::now());
    let tx = Transactions::new(&chain.shared);
    let tip = ACTIVATION_HEIGHT + 1;
    chain.put(
        ACTIVATION_HEIGHT,
        &[support::fixture()],
        &[Fixture(0), Fixture(1)],
        0,
    );
    let original = [
        tx.same_block.clone(),
        tx.without_outputs.clone(),
        tx.with_outputs.clone(),
    ];
    let original_kinds = [
        mixed(0, false),
        mixed(1, false),
        mixed(0, true),
        mixed(1, true),
    ];
    let first = chain.put(tip, &original, &original_kinds, 1);
    let mut stack = Stack::start(chain, 2).await;
    stack
        .until("answers for the original block", PREPARATION_BUDGET, |o| {
            o.first_answer(&first, 0).map(|_| ())
        })
        .await;
    let mut results = Vec::new();

    // One-block reorganization. The node holds the replacement block's
    // contents until every wallet request has been refused for a while, so
    // the withdrawn interval is controlled by the test.
    stack.chain.shared.lock().unwrap().hold = Some(tip);
    let changed = stack.now();
    let replacement = stack.chain.put(
        tip,
        &[
            tx.same_block.clone(),
            tx.with_outputs.clone(),
            tx.without_outputs.clone(),
        ],
        &[
            mixed(0, true),
            mixed(1, true),
            mixed(0, false),
            mixed(1, false),
        ],
        2,
    );
    stack
        .until("withdrawal", WITHDRAWAL_BUDGET, |o| {
            o.first(changed, "publisher", |s| {
                matches!(
                    s,
                    Seen::Publisher {
                        eligible: false,
                        ..
                    }
                )
            })
        })
        .await;
    let closed = changed + LIFETIME_MS;
    stack.wait_until(closed + 3_000).await;
    let (answered, refused) = stack.refusals_since(closed);
    assert_eq!(answered, 0, "withdrawn publication answered");
    assert!(refused >= 10, "too few probes while withdrawn: {refused}");
    stack.chain.shared.lock().unwrap().hold = None;
    let published = stack
        .until("the replacement publication", PREPARATION_BUDGET, |o| {
            o.first(changed, "publisher", |s| {
                matches!(s, Seen::Publisher { eligible: true, anchor: Some(a), .. } if *a == replacement)
            })
        })
        .await;
    stack
        .until("answers for the replacement", ADOPTION_BUDGET, |o| {
            o.first_answer(&replacement, published).map(|_| ())
        })
        .await;
    let result = stack.recovery("one-block reorganization", changed, &replacement);
    stack.assert_single_publication(&result);
    results.push(result);

    // Repeated reorganization: another replacement, then a return to the
    // original block as soon as the publisher has withdrawn for it.
    let changed = stack.now();
    let shorter = stack.chain.put(
        tip,
        &[tx.same_block.clone(), tx.without_outputs.clone()],
        &[mixed(0, false), mixed(1, false)],
        3,
    );
    stack
        .until("second withdrawal", WITHDRAWAL_BUDGET, |o| {
            o.first(changed, "publisher", |s| {
                matches!(
                    s,
                    Seen::Publisher {
                        eligible: false,
                        ..
                    }
                )
            })
        })
        .await;
    let returned = stack.now();
    assert_eq!(stack.chain.put(tip, &original, &original_kinds, 1), first);
    let published = stack
        .until("the restored publication", PREPARATION_BUDGET, |o| {
            o.first(returned, "publisher", |s| {
                matches!(s, Seen::Publisher { eligible: true, anchor: Some(a), .. } if *a == first)
            })
        })
        .await;
    stack
        .until("answers for the restored block", ADOPTION_BUDGET, |o| {
            o.first_answer(&first, published).map(|_| ())
        })
        .await;
    let mut result = stack.recovery("repeated reorganization", changed, &first);
    result["returned_ms_after_change"] = json!(returned - changed);
    result["answers_for_the_short_lived_block"] = json!(stack
        .observed
        .lock()
        .unwrap()
        .probes
        .iter()
        .filter(|p| p.outcome == Outcome::Answered && p.anchor.as_deref() == Some(&shorter))
        .count());
    stack.assert_single_publication(&result);
    results.push(result);

    // Source outages: the node, then the publisher's TLS edge, refuse every
    // request. Wallets are refused once the last lease ends, and serving
    // resumes on demand when the source returns.
    let generation = stack
        .observed
        .lock()
        .unwrap()
        .probes
        .iter()
        .rev()
        .find(|p| p.outcome == Outcome::Answered)
        .unwrap()
        .generation;
    for (label, outage) in [("node outage", true), ("publisher edge outage", false)] {
        let down = stack.now();
        if outage {
            stack.node.down.store(true, Ordering::Release);
        } else {
            stack.publisher.refuse_consumers(true);
        }
        stack.wait_until(down + OUTAGE.as_millis() as u64).await;
        let (answered, refused) = stack.refusals_since(down + LIFETIME_MS);
        assert_eq!(
            answered, 0,
            "{label}: answered without a fresh source check"
        );
        assert!(refused >= 10, "{label}: too few probes: {refused}");
        let restored = stack.now();
        if outage {
            stack.node.down.store(false, Ordering::Release);
        } else {
            stack.publisher.refuse_consumers(false);
        }
        let answer = stack
            .until(label, OUTAGE_RECOVERY_BUDGET, |o| {
                o.probes
                    .iter()
                    .find(|p| p.sent >= restored && p.outcome == Outcome::Answered)
                    .cloned()
            })
            .await;
        assert_eq!(
            answer.generation, generation,
            "{label}: an outage needs no republication"
        );
        let last_before = stack
            .observed
            .lock()
            .unwrap()
            .probes
            .iter()
            .filter(|p| p.outcome == Outcome::Answered && p.done < down + LIFETIME_MS)
            .map(|p| p.done)
            .max();
        let alert = alert_model(&stack.observed.lock().unwrap().probes, down, answer.done);
        results.push(json!({
            "label": label,
            "outage_ms": restored - down,
            "first_answer_ms_after_restore": answer.done - restored,
            "request_outage_ms": last_before.map(|last| answer.done - last),
            "modeled_query_5xx_alert": alert,
        }));
    }
    stack.wait_until(stack.now() + 2_000).await;
    stack.assert_legitimate();
    stack.report("reorganizations_and_source_outages", json!(results));
    stack.shutdown().await;
}

/// Seven publications advance the serving generation past the five that
/// workers, routers and ingresses retain, while wallets keep requests in
/// flight across every handover. Router retention diagnostics stay bounded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[cfg_attr(
    not(feature = "qualification-tests"),
    ignore = "deployed geometry: qualification-tests"
)]
async fn publication_advances_past_retained_generations_under_live_wallets() {
    let _serial = DEPLOYED.lock().await;
    let mut chain = ChainFixture::new(Instant::now());
    let mut tip = ACTIVATION_HEIGHT;
    let fixture = [support::fixture()];
    let fixture_kinds = [Fixture(0), Fixture(1)];
    let first = chain.put(tip, &fixture, &fixture_kinds, 0);
    let mut stack = Stack::start(chain, 4).await;
    let serving = stack
        .until("answers for the first block", PREPARATION_BUDGET, |o| {
            o.first_answer(&first, 0).map(|probe| probe.done)
        })
        .await;
    let mut results = Vec::new();
    for appended in 1..=enhance_shared::protocol::RETAINED_GENERATIONS as u8 + 1 {
        tip += 1;
        let changed = stack.now();
        // Identical contents need a distinct nonce for a distinct block hash.
        let anchor = stack.chain.put(tip, &fixture, &fixture_kinds, appended);
        let published = stack
            .until("the next publication", PREPARATION_BUDGET, |o| {
                o.first(changed, "publisher", |s| {
                    matches!(s, Seen::Publisher { eligible: true, anchor: Some(a), .. } if *a == anchor)
                })
            })
            .await;
        stack
            .until("answers for the next publication", ADVANCE_BUDGET, |o| {
                o.first_answer(&anchor, published).map(|_| ())
            })
            .await;
        let mut result = stack.recovery("append", changed, &anchor);
        stack.assert_single_publication(&result);
        let router = stack.observed.lock().unwrap().last.get("router").cloned();
        result["router_after"] = json!(format!("{router:?}"));
        results.push(result);
    }
    // Wallet outcomes once the first generation served; an ordinary
    // advancement must never refuse them as unavailable.
    let (most_retained, outcomes) = {
        let observed = stack.observed.lock().unwrap();
        let most_retained = observed
            .timeline
            .iter()
            .filter_map(|(_, _, seen)| match seen {
                Seen::Router { retained, .. } => Some(*retained),
                _ => None,
            })
            .max()
            .unwrap();
        let mut outcomes = BTreeMap::<String, usize>::new();
        for probe in observed.probes.iter().filter(|p| p.sent >= serving) {
            *outcomes.entry(format!("{:?}", probe.outcome)).or_default() += 1;
        }
        (most_retained, outcomes)
    };
    assert!(most_retained < enhance_shared::protocol::RETAINED_GENERATIONS);
    assert!(
        outcomes
            .keys()
            .all(|outcome| outcome == "Answered" || outcome.contains("409")),
        "{outcomes:?}"
    );
    stack.assert_legitimate();
    stack.report(
        "retained_generations",
        json!({"publications": results, "router_most_retained": most_retained,
            "wallet_outcomes": outcomes}),
    );
    stack.shutdown().await;
}
