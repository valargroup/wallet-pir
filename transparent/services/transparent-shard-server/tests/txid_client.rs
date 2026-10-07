//! The wallet txid client (`transparent-txid-client`) against the in-process
//! tiered display deployment, over real HTTP through a blocking test
//! transport that logs, and on request rewrites, every exchange.
#[path = "../examples/support/txdisplay.rs"]
mod txdisplay;

use axum::body::Body;
use axum::extract::{Request, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Router;
use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Duration;
use tower::ServiceExt;
use transparent_events::Txid;
use transparent_shard::display::{self, DisplayMap, DisplaySealParams, DisplayTable, TXID_2K};
use transparent_shard::txid::TransparentDisplayRecord;
use transparent_shard_server::assignment::WorkerRole;
use transparent_shard_server::display::live::{DisplayCommand, DisplayLive, DisplayPublication};
use transparent_shard_server::display::service::DisplayRuntime;
use transparent_shard_server::display::synth::{self, Published, ShardSpec};
use transparent_shard_server::service::{ReadinessMode, ServiceConfig};
use transparent_txid_client::{
    Method, Placement, ProfileCache, ProtocolKind, Route, Tier, TransportError, TxidDisplayClient,
    TxidError, TxidLookup, TxidReply, TxidRequest, TxidTransport,
};
use txdisplay::{DisplayClient, LookupResult};

#[path = "support/display_world.rs"]
mod display_world;
use display_world::*;

/// One exchange as the transport saw it, after any rewrite.
#[derive(Clone, Debug)]
struct Sent {
    method: Method,
    route: Route,
    path: String,
    template: &'static str,
    body: Vec<u8>,
    status: u16,
    reply_bytes: usize,
}

type Intercept = Box<dyn FnMut(&TxidRequest) -> Option<TxidReply>>;
type Tamper = Box<dyn FnMut(&TxidRequest, &mut TxidReply)>;
type Reroute = Box<dyn FnMut(&str) -> String>;

/// A blocking reqwest transport with fault hooks.
struct Http {
    client: reqwest::blocking::Client,
    base: String,
    log: Vec<Sent>,
    /// Answers a request without sending it.
    intercept: Option<Intercept>,
    /// Rewrites a reply before the client sees it.
    tamper: Option<Tamper>,
    /// Rewrites the path that is actually sent.
    reroute: Option<Reroute>,
}

impl Http {
    fn new(base: &str) -> Self {
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

    fn take_log(&mut self) -> Vec<Sent> {
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

/// One deployment and runtime for the whole binary: every test brings its own
/// client and transport, so faults never cross tests.
struct Env {
    rt: tokio::runtime::Runtime,
    world: World,
    profiles: ProfileCache,
}

/// Tests share one server; run them one at a time so admission never sheds
/// one test's queries because of another's.
fn serial() -> std::sync::MutexGuard<'static, ()> {
    static SERIAL: std::sync::Mutex<()> = std::sync::Mutex::new(());
    SERIAL
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn env() -> &'static Env {
    static ENV: OnceLock<Env> = OnceLock::new();
    ENV.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| "warn".into()),
            )
            .with_test_writer()
            .try_init();
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap();
        let world = rt.block_on(World::start());
        Env {
            rt,
            world,
            profiles: ProfileCache::default(),
        }
    })
}

impl Env {
    fn client(&self) -> TxidDisplayClient {
        TxidDisplayClient::with_profiles(self.profiles.clone())
    }

    fn http(&self) -> Http {
        Http::new(&self.world.url)
    }
}

fn never() -> bool {
    false
}

const ABSENT: [u8; 32] = [0xee; 32];

fn routes(log: &[Sent]) -> Vec<Route> {
    log.iter().map(|s| s.route).collect()
}

fn queries<'a>(log: &'a [Sent], table: &str) -> Vec<&'a Sent> {
    log.iter()
        .filter(|s| s.route == Route::Query && s.path.ends_with(&format!("/query/{table}")))
        .collect()
}

fn posts(log: &[Sent]) -> usize {
    log.iter().filter(|s| s.method == Method::Post).count()
}

/// Request and response body bytes of a transcript.
fn body_bytes(log: &[Sent]) -> (usize, usize) {
    (
        log.iter().map(|s| s.body.len()).sum(),
        log.iter().map(|s| s.reply_bytes).sum(),
    )
}

fn found(
    lookup: TxidLookup,
    record: &TransparentDisplayRecord,
) -> transparent_txid_client::Provenance {
    match lookup {
        TxidLookup::Found {
            record: actual,
            provenance,
        } => {
            assert_eq!(&actual, record);
            provenance
        }
        other => panic!("{other:?} for a published record"),
    }
}

/// The two directory queries of a lookup, then its `pages` page queries, all
/// 200 with the fixed upload.
fn assert_queries(log: &[Sent], pages: usize) {
    let posts: Vec<&Sent> = log.iter().filter(|s| s.method == Method::Post).collect();
    assert_eq!(posts.len(), 2 + pages, "{:?}", routes(log));
    assert!(posts[..2]
        .iter()
        .all(|s| s.path.contains("/query/directory-")));
    assert!(posts[2..].iter().all(|s| s.path.ends_with("/query/pages")));
    for post in posts {
        assert_eq!(post.body.len() as u64, QUERY_BYTES);
        assert_eq!(post.status, 200);
    }
}

#[test]
fn inline_found_exact_transcript() {
    let _serial = serial();
    let env = env();
    for (records, height, digest, shard_id, tier) in [
        (
            &env.world.a0_records,
            150,
            &env.world.a0.digest,
            0,
            Tier::Archive,
        ),
        (
            &env.world.r0_records,
            230,
            &env.world.r0.digest,
            1,
            Tier::Recent,
        ),
    ] {
        let mut client = env.client();
        let mut http = env.http();
        let record = &records[40];
        let provenance = found(
            client
                .lookup(&mut http, record.txid.0, height, &never)
                .unwrap(),
            record,
        );
        let log = http.take_log();
        // Cold: init, map, manifest, the bucket's one directory setup, then
        // exactly two directory queries.
        assert_eq!(
            routes(&log),
            [
                Route::Init,
                Route::Map,
                Route::Manifest,
                Route::Setup,
                Route::Query,
                Route::Query
            ]
        );
        assert_queries(&log, 0);
        assert!(log[3].path.ends_with("/setup/directory-0/0"));
        assert_eq!(provenance.shard_id, shard_id);
        assert_eq!(&provenance.manifest_digest, digest);
        assert_eq!(provenance.tier, tier);
        assert_eq!(provenance.revision, 0);
        assert_eq!(Some(provenance.map_sha256.as_str()), client.map_sha256());
        assert_eq!(provenance.map_sha256, env.world.p0.1);
        let (up, down) = body_bytes(&log);
        eprintln!("bandwidth inline cold {tier:?}: up {up} B, down {down} B");
        // Warm: the same two queries only.
        found(
            client
                .lookup(&mut http, record.txid.0, height, &never)
                .unwrap(),
            record,
        );
        let log = http.take_log();
        assert_eq!(routes(&log), [Route::Query, Route::Query]);
        let (up, down) = body_bytes(&log);
        eprintln!("bandwidth inline warm {tier:?}: up {up} B, down {down} B");
    }
}

#[test]
fn paged_found_pages_queries() {
    let _serial = serial();
    let env = env();
    let mut client = env.client();
    let mut http = env.http();
    for (index, pages) in [(41usize, 1usize), (42, 2), (43, 5)] {
        let record = &env.world.a0_records[index];
        found(
            client
                .lookup(&mut http, record.txid.0, 150, &never)
                .unwrap(),
            record,
        );
        let log = http.take_log();
        assert_queries(&log, pages);
        assert_eq!(queries(&log, "pages").len(), pages);
        // The pages setup is fetched once, after the directory queries.
        let setups: Vec<&Sent> = log.iter().filter(|s| s.route == Route::Setup).collect();
        if index == 41 {
            let first_page = log
                .iter()
                .position(|s| s.path.ends_with("/query/pages"))
                .unwrap();
            assert_eq!(log[first_page - 1].route, Route::Setup);
            assert!(log[first_page - 1].path.contains("/setup/pages/0"));
            assert_eq!(setups.len(), 2);
        } else {
            assert!(setups.is_empty());
        }
        let (up, down) = body_bytes(&log);
        eprintln!(
            "bandwidth pages-{pages} {}: up {up} B, down {down} B",
            if index == 41 { "cold" } else { "warm" }
        );
    }
}

#[test]
fn absent_sends_same_transcript_shape() {
    let _serial = serial();
    let env = env();
    let mut client = env.client();
    let mut http = env.http();
    let record = &env.world.a0_records[40];
    found(
        client
            .lookup(&mut http, record.txid.0, 150, &never)
            .unwrap(),
        record,
    );
    let present = http.take_log();
    let mut fresh = env.client();
    assert_eq!(
        fresh.lookup(&mut http, ABSENT, 150, &never).unwrap(),
        TxidLookup::Absent
    );
    let absent = http.take_log();
    assert_eq!(routes(&absent), routes(&present));
    assert_queries(&absent, 0);
    for (a, p) in absent.iter().zip(&present) {
        assert_eq!(a.template, p.template);
        assert_eq!(a.body.len(), p.body.len());
        assert_eq!(a.reply_bytes, p.reply_bytes);
    }
    let (up, down) = body_bytes(&absent);
    eprintln!("bandwidth absent cold: up {up} B, down {down} B");
    // Warm absent: two queries only, as warm found.
    assert_eq!(
        client.lookup(&mut http, ABSENT, 150, &never).unwrap(),
        TxidLookup::Absent
    );
    assert_eq!(routes(&http.take_log()), [Route::Query, Route::Query]);
}

#[test]
fn placement_below_or_above_no_post() {
    let _serial = serial();
    let env = env();
    let mut client = env.client();
    let mut http = env.http();
    let txid = env.world.a0_records[40].txid.0;
    for (height, placement) in [
        (0, Placement::Below),
        (99, Placement::Below),
        (261, Placement::Above),
        (u64::MAX, Placement::Above),
    ] {
        assert_eq!(
            client.lookup(&mut http, txid, height, &never).unwrap(),
            TxidLookup::PlacementUnknown(placement)
        );
    }
    let log = http.take_log();
    assert_eq!(posts(&log), 0);
    // Init and map once; a fresh map is not refetched for a height above it.
    assert_eq!(routes(&log), [Route::Init, Route::Map]);
    // Both edges of the published range are placed.
    for height in [100, 260] {
        assert!(matches!(
            client.lookup(&mut http, txid, height, &never).unwrap(),
            TxidLookup::Found { .. } | TxidLookup::Absent
        ));
    }
}

#[test]
fn unsupported_schema_codec() {
    let _serial = serial();
    let env = env();
    let txid = env.world.a0_records[40].txid.0;
    // A codec this client does not know.
    let mut http = env.http();
    http.tamper = Some(Box::new(|request, reply| {
        if request.route == Route::Init {
            let mut init: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
            init["codec"] = "transparent-txid-display-v9".into();
            reply.body = init.to_string().into_bytes();
        }
    }));
    assert_eq!(
        env.client().lookup(&mut http, txid, 150, &never).unwrap(),
        TxidLookup::Unsupported
    );
    assert_eq!(routes(&http.take_log()), [Route::Init]);
    // A map schema this client does not know, canonical and correctly
    // announced.
    let mut http = env.http();
    http.tamper = Some(Box::new(|request, reply| {
        if request.route == Route::Map {
            let mut map: DisplayMap = serde_json::from_slice(&reply.body).unwrap();
            map.schema = "transparent-txid-display-shard-v2".into();
            reply.body = map.to_bytes();
            reply.map_sha256 = Some(map.sha256());
        }
    }));
    assert_eq!(
        env.client().lookup(&mut http, txid, 150, &never).unwrap(),
        TxidLookup::Unsupported
    );
    assert_eq!(posts(&http.take_log()), 0);
}

fn stale() -> TxidReply {
    TxidReply {
        status: 409,
        body: br#"{"error":"stale"}"#.to_vec(),
        ..TxidReply::default()
    }
}

#[test]
fn stale_409_refresh_retry_once_then_stale() {
    let _serial = serial();
    let env = env();
    let record = &env.world.a0_records[40];
    // One 409: the map is fetched again and the whole lookup retried.
    let mut http = env.http();
    let mut left = 1;
    http.intercept = Some(Box::new(move |request| {
        (request.route == Route::Query && left > 0).then(|| {
            left -= 1;
            stale()
        })
    }));
    let mut client = env.client();
    found(
        client
            .lookup(&mut http, record.txid.0, 150, &never)
            .unwrap(),
        record,
    );
    let log = http.take_log();
    assert_eq!(
        routes(&log),
        [
            Route::Init,
            Route::Map,
            Route::Manifest,
            Route::Setup,
            Route::Query,
            Route::Map,
            Route::Query,
            Route::Query
        ]
    );
    // Every query 409: one retry, then Stale.
    let mut http = env.http();
    http.intercept = Some(Box::new(|request| {
        (request.route == Route::Query).then(stale)
    }));
    assert_eq!(
        client.lookup(&mut http, record.txid.0, 150, &never),
        Err(TxidError::Stale)
    );
    assert_eq!(
        routes(&http.take_log()),
        [Route::Query, Route::Map, Route::Query]
    );
    // A stale manifest is the same.
    let mut http = env.http();
    http.intercept = Some(Box::new(|request| {
        (request.route == Route::Manifest).then(stale)
    }));
    assert_eq!(
        env.client().lookup(&mut http, record.txid.0, 150, &never),
        Err(TxidError::Stale)
    );
    assert_eq!(
        routes(&http.take_log()),
        [
            Route::Init,
            Route::Map,
            Route::Manifest,
            Route::Map,
            Route::Manifest
        ]
    );
}

#[test]
fn overloaded_503_retry_after_no_internal_retry() {
    let _serial = serial();
    let env = env();
    let record = &env.world.a0_records[41];
    let mut client = env.client();
    let mut http = env.http();
    http.intercept = Some(Box::new(|request| {
        request.path().ends_with("/query/pages").then(|| TxidReply {
            status: 503,
            retry_after: Some("7".into()),
            body: br#"{"error":"busy"}"#.to_vec(),
            ..TxidReply::default()
        })
    }));
    assert_eq!(
        client.lookup(&mut http, record.txid.0, 150, &never),
        Err(TxidError::Unavailable {
            retry_after: Some(Duration::from_secs(7))
        })
    );
    let log = http.take_log();
    assert_eq!(queries(&log, "pages").len(), 1);
    assert_eq!(log.last().unwrap().status, 503);
    // Without a usable hint, still no retry.
    http.intercept = Some(Box::new(|request| {
        (request.route == Route::Query).then(|| TxidReply {
            status: 503,
            retry_after: Some("Wed, 21 Oct 2026 07:28:00 GMT".into()),
            ..TxidReply::default()
        })
    }));
    assert_eq!(
        client.lookup(&mut http, record.txid.0, 150, &never),
        Err(TxidError::Unavailable { retry_after: None })
    );
    assert_eq!(routes(&http.take_log()), [Route::Query]);
    // The cache survives: the next lookup only queries.
    http.intercept = None;
    found(
        client
            .lookup(&mut http, record.txid.0, 150, &never)
            .unwrap(),
        record,
    );
    assert_eq!(posts(&http.log), 3);
    assert_eq!(http.take_log().len(), 3);
}

#[test]
fn misdirected_421_refused() {
    let _serial = serial();
    let env = env();
    let record = &env.world.r0_records[40];
    let mut http = env.http();
    // The edge sends archive-tier paths to the archive owner, which refuses
    // the recent revision with 421.
    http.reroute = Some(Box::new(|path| {
        path.replace("/v1/txid/recent/", "/v1/txid/archive/")
    }));
    let mut client = env.client();
    assert_eq!(
        client.lookup(&mut http, record.txid.0, 230, &never),
        Err(TxidError::Refused(421))
    );
    let log = http.take_log();
    assert_eq!(log.last().unwrap().status, 421);
    assert_eq!(log.iter().filter(|s| s.status == 421).count(), 1);
    assert_eq!(posts(&log), 0);
    // Other refusals are not retried either.
    for status in [400u16, 408, 411] {
        let mut http = env.http();
        http.intercept = Some(Box::new(move |request| {
            (request.route == Route::Query).then(|| TxidReply {
                status,
                ..TxidReply::default()
            })
        }));
        assert_eq!(
            client.lookup(&mut http, record.txid.0, 230, &never),
            Err(TxidError::Refused(status))
        );
        assert_eq!(posts(&http.log), 1);
    }
}

/// A lookup of `txid` at `height` through a transport that tampers with
/// replies fails with `kind`, never `Absent`.
fn assert_tampered(txid: [u8; 32], height: u64, kind: ProtocolKind, tamper: Tamper) {
    let env = env();
    let mut http = env.http();
    http.tamper = Some(tamper);
    assert_eq!(
        env.client().lookup(&mut http, txid, height, &never),
        Err(TxidError::Protocol(kind))
    );
}

fn on(route: Route, mut f: impl FnMut(&mut TxidReply) + 'static) -> Tamper {
    Box::new(move |request, reply| {
        if request.route == route {
            f(reply)
        }
    })
}

/// Rewrites one JSON field of a reply.
fn json_field(route: Route, field: &'static str, value: serde_json::Value) -> Tamper {
    on(route, move |reply| {
        let mut json: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
        json[field] = value.clone();
        reply.body = serde_json::to_vec_pretty(&json).unwrap();
    })
}

#[test]
fn tampered_manifest_protocol_never_absent() {
    let _serial = serial();
    let env = env();
    let found = env.world.a0_records[40].txid.0;
    // A field changed: no longer the map entry's manifest.
    assert_tampered(
        found,
        150,
        ProtocolKind::Manifest,
        json_field(Route::Manifest, "records", 1.into()),
    );
    // One byte appended: no longer canonical.
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Manifest,
        on(Route::Manifest, |reply| reply.body.push(b' ')),
    );
}

#[test]
fn tampered_setup_protocol_never_absent() {
    let _serial = serial();
    for (field, value) in [
        ("public_params_sha256", serde_json::json!("00".repeat(32))),
        ("segment", 1.into()),
        ("bucket", 1.into()),
        ("table", "pages".into()),
        ("geometry", "txid-4k".into()),
        ("manifest_digest", "aa".repeat(32).into()),
        ("public_params_epoch", "00".into()),
    ] {
        assert_tampered(
            ABSENT,
            150,
            ProtocolKind::Setup,
            json_field(Route::Setup, field, value),
        );
    }
    // Correct digest of the wrong bytes.
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Setup,
        on(Route::Setup, |reply| {
            let mut json: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
            let params = vec![0u8; 16];
            json["public_params"] = base64_encode(&params).into();
            json["public_params_sha256"] = hex::encode(Sha256::digest(&params)).into();
            reply.body = serde_json::to_vec(&json).unwrap();
        }),
    );
}

fn base64_encode(bytes: &[u8]) -> String {
    use base64::Engine;
    base64::engine::general_purpose::STANDARD.encode(bytes)
}

#[test]
fn tampered_binding_protocol_never_absent() {
    let _serial = serial();
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Binding,
        on(Route::Query, |reply| reply.body[0] ^= 1),
    );
}

#[test]
fn tampered_epoch_protocol_never_absent() {
    let _serial = serial();
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Epoch,
        on(Route::Query, |reply| reply.body[8] ^= 1),
    );
}

#[test]
fn tampered_length_protocol_never_absent() {
    let _serial = serial();
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Length,
        on(Route::Query, |reply| {
            reply.body.pop();
        }),
    );
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Length,
        on(Route::Query, |reply| reply.body.clear()),
    );
}

#[test]
fn tampered_map_canonical_protocol_never_absent() {
    let _serial = serial();
    // Compact rather than pretty, correctly announced.
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::MapCanonical,
        on(Route::Map, |reply| {
            let map: DisplayMap = serde_json::from_slice(&reply.body).unwrap();
            reply.body = serde_json::to_vec(&map).unwrap();
            reply.map_sha256 = Some(hex::encode(Sha256::digest(&reply.body)));
        }),
    );
}

#[test]
fn tampered_map_header_protocol_never_absent() {
    let _serial = serial();
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::MapHeader,
        on(Route::Map, |reply| reply.map_sha256 = Some("00".repeat(32))),
    );
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::MapHeader,
        on(Route::Map, |reply| reply.map_sha256 = None),
    );
}

#[test]
fn cancel_stops_before_next_request() {
    let _serial = serial();
    let env = env();
    let record = &env.world.a0_records[41];
    // Cold paged lookup: init, map, manifest, setup, 2 queries, setup, 1 page.
    for allowed in 0..8 {
        let mut http = env.http();
        let sent = Cell::new(0usize);
        let cancel = || {
            sent.set(sent.get() + 1);
            sent.get() > allowed
        };
        assert_eq!(
            env.client().lookup(&mut http, record.txid.0, 150, &cancel),
            Err(TxidError::Cancelled),
            "{allowed}"
        );
        assert_eq!(http.log.len(), allowed);
    }
}

#[test]
fn routes_match_whitelist_and_no_txid_in_path_or_body() {
    let _serial = serial();
    let env = env();
    let mut client = env.client();
    let mut http = env.http();
    let mut txids = Vec::new();
    for (records, height) in [(&env.world.a0_records, 150), (&env.world.r0_records, 230)] {
        for record in &records[40..44] {
            found(
                client
                    .lookup(&mut http, record.txid.0, height, &never)
                    .unwrap(),
                record,
            );
            txids.push(record.txid);
        }
        client.lookup(&mut http, ABSENT, height, &never).unwrap();
    }
    txids.push(Txid(ABSENT));
    let log = http.take_log();
    assert!(log.len() > 20);
    for sent in &log {
        let route = sent.route;
        assert!(
            matches_template(route.template(), &sent.path),
            "{} !~ {}",
            sent.path,
            route.template()
        );
        assert_eq!(sent.method, route.method());
        assert_eq!(sent.template, route.template());
        // Templates carry no ids: no digit outside the version prefix.
        assert!(!sent.template["/v1".len()..].contains(|c: char| c.is_ascii_digit()));
        for txid in &txids {
            let mut reversed = txid.0;
            reversed.reverse();
            for needle in [txid.0, reversed] {
                assert!(!sent.path.contains(&hex::encode(needle)));
                assert!(!sent.body.windows(32).any(|w| w == needle));
            }
        }
        assert_eq!(sent.body.is_empty(), sent.method == Method::Get);
    }
}

/// Whether `path` is an instance of `template`, each placeholder holding
/// only what it names.
fn matches_template(template: &str, path: &str) -> bool {
    let (t, p): (Vec<&str>, Vec<&str>) = (template.split('/').collect(), path.split('/').collect());
    let digits = |s: &str| !s.is_empty() && s.bytes().all(|b| b.is_ascii_digit());
    t.len() == p.len()
        && t.iter().zip(&p).all(|(t, p)| match *t {
            "{tier}" => *p == "archive" || *p == "recent",
            "{shard}" | "{segment}" => digits(p),
            "{digest}" => {
                p.len() == 64
                    && p.bytes()
                        .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
            }
            "{table}" => DisplayTable::parse(p).is_some(),
            literal => literal == *p,
        })
}

#[test]
fn caches_reused_second_lookup_only_posts() {
    let _serial = serial();
    let env = env();
    let mut client = env.client();
    let mut http = env.http();
    let records = &env.world.a0_records;
    found(
        client
            .lookup(&mut http, records[43].txid.0, 150, &never)
            .unwrap(),
        &records[43],
    );
    let cold = http.take_log();
    assert_eq!(posts(&cold), 7);
    assert!(cold.iter().any(|s| s.route == Route::Init));
    for (index, pages) in [(40usize, 0usize), (41, 1), (42, 2), (43, 5)] {
        found(
            client
                .lookup(&mut http, records[index].txid.0, 150, &never)
                .unwrap(),
            &records[index],
        );
        let log = http.take_log();
        assert!(
            log.iter().all(|s| s.method == Method::Post),
            "{:?}",
            routes(&log)
        );
        assert_queries(&log, pages);
    }
    // The other shard needs its own manifest and setups, not init or map.
    let record = &env.world.r0_records[40];
    found(
        client
            .lookup(&mut http, record.txid.0, 230, &never)
            .unwrap(),
        record,
    );
    assert_eq!(
        routes(&http.take_log()),
        [Route::Manifest, Route::Setup, Route::Query, Route::Query]
    );
}

#[test]
fn differential_vs_reference_txdisplay() {
    let _serial = serial();
    let env = env();
    let reference = env.world.client();
    let mut client = env.client();
    let mut http = env.http();
    let mut compared = 0;
    let coincident = env.world.r0_records.last().unwrap().txid;
    let mut cases: Vec<(Txid, u64)> = Vec::new();
    for record in &env.world.a0_records {
        cases.push((record.txid, 150));
    }
    for record in &env.world.r0_records {
        cases.push((record.txid, 230));
    }
    cases.extend([
        (Txid(ABSENT), 150),
        (Txid(ABSENT), 230),
        (coincident, 255),
        (coincident, 99),
        (coincident, 261),
        // Published, but in the other shard than its height names.
        (env.world.a0_records[40].txid, 230),
    ]);
    for (txid, height) in cases {
        let expected = env
            .rt
            .block_on(reference.lookup(txid, Some(height)))
            .unwrap();
        let actual = client.lookup(&mut http, txid.0, height, &never).unwrap();
        let log = http.take_log();
        match (&expected.result, &actual) {
            (LookupResult::Found(a), TxidLookup::Found { record, provenance }) => {
                assert_eq!(a, record);
                assert_eq!(expected.shard_id, Some(provenance.shard_id));
                assert_eq!(
                    expected.digest.as_deref(),
                    Some(provenance.manifest_digest.as_str())
                );
                assert_eq!(
                    expected.map_sha256.as_deref(),
                    Some(provenance.map_sha256.as_str())
                );
            }
            (LookupResult::Absent, TxidLookup::Absent)
            | (LookupResult::PlacementUnknown, TxidLookup::PlacementUnknown(_))
            | (LookupResult::Unsupported, TxidLookup::Unsupported) => {}
            (e, a) => panic!("reference {e:?}, client {a:?}"),
        }
        // The same private queries, row for row and table for table.
        let rows: Vec<String> = expected.queries.iter().map(|q| q.table.clone()).collect();
        let sent: Vec<String> = log
            .iter()
            .filter(|s| s.route == Route::Query)
            .map(|s| s.path.rsplit('/').next().unwrap().to_string())
            .collect();
        let mut rows_sorted = rows.clone();
        rows_sorted.sort();
        let mut sent_sorted = sent.clone();
        sent_sorted.sort();
        assert_eq!(rows_sorted, sent_sorted);
        compared += 1;
    }
    assert_eq!(compared, 44 + 45 + 6);
}

#[test]
fn setup_seed_golden_matches_server() {
    let _serial = serial();
    use transparent_shard_server::display::kind;
    use transparent_shard_server::shardset::setup_seed;
    let golden = [
        (
            "txid-2k",
            DisplayTable::Directory(0),
            0x57a7_3ced_5e98_120a_u64,
        ),
        ("txid-2k", DisplayTable::Pages, 0x6231_4f48_472b_6e15),
        ("txid-4k", DisplayTable::Directory(3), 0x28bc_e699_56cc_2d7e),
        ("txid-4k", DisplayTable::Pages, 0x95df_53a3_e525_1353),
    ];
    for (name, table, seed) in golden {
        let geometry = display::display_by_name(name).unwrap();
        assert_eq!(setup_seed(geometry, kind(table)), seed);
        assert_eq!(display::setup_seed(geometry, table.kind()), seed);
        assert_eq!(kind(table).as_str(), table.kind().as_str());
        assert_eq!(kind(table).rows(geometry), table.kind().rows(geometry));
        assert_eq!(
            kind(table).row_bytes(geometry),
            table.kind().row_bytes(geometry)
        );
    }
    // And what the running server publishes.
    let env = env();
    let init: serde_json::Value = env.rt.block_on(env.world.get("/v1/txid/init")).1;
    let geometry = init["geometries"]
        .as_array()
        .unwrap()
        .iter()
        .find(|g| g["name"] == TXID_2K.name)
        .unwrap();
    assert_eq!(
        geometry["txdirectory"]["setup_seed"],
        0x57a7_3ced_5e98_120a_u64
    );
    assert_eq!(geometry["txpages"]["setup_seed"], 0x6231_4f48_472b_6e15_u64);
}

/// A mainnet transaction at height 3,410,000 with one transparent input and
/// two P2PKH outputs, against the production service.
#[test]
#[ignore = "contacts the production txid display service"]
fn txid_live_lookup() {
    let _serial = serial();
    let base = std::env::var("TXID_LIVE_URL")
        .unwrap_or_else(|_| "https://transparent-pir.valargroup.dev".to_string());
    let mut http = Http::new(&base);
    let mut client = TxidDisplayClient::new();
    let txid =
        txdisplay::parse_txid("fd4667e1a9b427715992cd12b3cdabeb2cfe7623e0e61c8d489f9b0b9a8effbf")
            .unwrap();
    let lookup = client.lookup(&mut http, txid.0, 3_410_000, &never).unwrap();
    let TxidLookup::Found { record, provenance } = lookup else {
        panic!("{lookup:?}");
    };
    assert_eq!(record.txid, txid);
    assert!(!record.coinbase);
    let values: Vec<u64> = record.outputs.iter().map(|o| o.value).collect();
    assert_eq!(values, [1_816_444, 93_460_672_472]);
    assert!(record
        .outputs
        .iter()
        .all(|o| o.script.len() == 25 && o.script[..3] == [0x76, 0xa9, 0x14]));
    assert_eq!(provenance.tier, Tier::Archive);
    let log = http.take_log();
    assert_queries(&log, 0);
    let (up, down) = body_bytes(&log);
    eprintln!("live cold lookup: {provenance:?}, up {up} B, down {down} B");
    // Absent at the same height: the same two queries.
    assert_eq!(
        client.lookup(&mut http, ABSENT, 3_410_000, &never).unwrap(),
        TxidLookup::Absent
    );
    assert_eq!(routes(&http.take_log()), [Route::Query, Route::Query]);
}

#[test]
fn refresh_map_observes_a_new_publication() {
    let _serial = serial();
    let env = env();
    // A deployment of its own: this test publishes, the shared one never does.
    let world = env.rt.block_on(World::start());
    let mut http = Http::new(&world.url);
    let mut client = env.client();
    let hex32 = |digest: [u8; 32]| hex::encode(digest);

    let first = client.refresh_map(&mut http, &never).unwrap();
    assert_eq!(hex32(first), world.p0.1);
    assert_eq!(client.map_sha256(), Some(world.p0.1.as_str()));
    assert_eq!(routes(&http.take_log()), [Route::Map]);
    let added = synth::record(30, 1, 25, 0);
    assert_eq!(
        client.lookup(&mut http, added.txid.0, 265, &never).unwrap(),
        TxidLookup::PlacementUnknown(Placement::Above)
    );
    // Unchanged publication: the same digest again.
    assert_eq!(client.refresh_map(&mut http, &never).unwrap(), first);

    // The recent shard grows to 270 with one more record.
    let mut records = world.r0_records.clone();
    records.push(added.clone());
    let grown = synth::write_shard(
        world.root.path(),
        &spec(1, 200, 270, false, &world.a0.digest),
        &records,
    )
    .unwrap();
    let p1 = synth::write_candidate(
        world.root.path(),
        &params(1),
        &[world.a0.clone(), grown.clone()],
        "grown",
    )
    .unwrap();
    env.rt.block_on(world.recent.publish(&p1.0, &p1.1));
    http.take_log();

    let second = client.refresh_map(&mut http, &never).unwrap();
    assert_ne!(second, first);
    assert_eq!(hex32(second), p1.1);
    assert_eq!(client.map_sha256(), Some(p1.1.as_str()));
    assert_eq!(routes(&http.take_log()), [Route::Map]);
    // The refreshed map is the one lookups use: no further map fetch.
    let provenance = found(
        client.lookup(&mut http, added.txid.0, 265, &never).unwrap(),
        &added,
    );
    assert_eq!(provenance.map_sha256, p1.1);
    assert_eq!(provenance.manifest_digest, grown.digest);
    assert!(!routes(&http.take_log()).contains(&Route::Map));
    env.rt.block_on(async move { drop(world) });
}
