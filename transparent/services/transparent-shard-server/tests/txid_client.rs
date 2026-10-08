//! The wallet txid client (`transparent-txid-client`) against the in-process
//! tiered display deployment, over real HTTP through a blocking test
//! transport that logs, and on request rewrites, every exchange.
//!
//! Entries are fixed-size, so every lookup that reaches a shard sends the
//! same transcript: found or absent, a complete entry or one with omissions.

use sha2::{Digest, Sha256};
use std::cell::Cell;
use std::path::Path;
use std::sync::OnceLock;
use std::time::Duration;
use transparent_events::Txid;
use transparent_shard::display::{self, DisplayRecentMap, DisplayTable, TXID_2K};
use transparent_shard::txid::{self, flags, AddressKind, DisplayEntry, DisplayRecord, Tag};
use transparent_shard_server::display::synth::{self, Published};
use transparent_txid_client::{
    Method, Placement, ProtocolKind, Route, Tier, TxidDisplayClient, TxidError, TxidLookup,
    TxidReply,
};

#[path = "support/display_world.rs"]
mod display_world;
use display_world::*;

/// One deployment and runtime for the whole binary: every test brings its own
/// client and transport, so faults never cross tests.
struct Env {
    rt: tokio::runtime::Runtime,
    world: World,
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
        Env { rt, world }
    })
}

impl Env {
    fn client(&self) -> TxidDisplayClient {
        TxidDisplayClient::with_profiles(profiles())
    }

    fn http(&self) -> Http {
        Http::new(&self.world.url)
    }
}

fn never() -> bool {
    false
}

const ABSENT: [u8; 32] = [0xee; 32];

/// SHA-256 of the recent map the fixture publication serves.
fn recent_sha256(world: &World) -> String {
    recent_sha256_of(&world.p0.0)
}

/// SHA-256 of the recent map a publication directory holds.
fn recent_sha256_of(publication: &Path) -> String {
    let bytes = std::fs::read(publication.join(display::split::RECENT_MAP_FILE)).unwrap();
    hex::encode(Sha256::digest(bytes))
}

/// The class records of a fixture shard, in [`classes`] order.
fn classes_of(records: &[DisplayRecord]) -> &[DisplayRecord] {
    &records[CLASSES..CLASSES + 6]
}

/// The facts each class must carry, beyond equality with the publisher's
/// entry: the omission flags and address kinds a wallet displays from.
fn check_class(class: usize, entry: &DisplayEntry) {
    let kind = |slot: usize| entry.outputs[slot].map(|o| o.address.kind);
    match class {
        // Regular: two inputs of one script to one address.
        0 => {
            assert!(entry.is_complete(), "{entry:?}");
            assert_eq!((entry.input_count, entry.output_count), (2, 1));
            assert_eq!(entry.source.kind, AddressKind::P2pkh);
            assert_eq!((kind(0), kind(1)), (Some(AddressKind::P2pkh), None));
            assert_eq!(entry.flags(), 0);
        }
        // Unshield: no transparent source, and none is omitted.
        1 => {
            assert!(entry.is_complete(), "{entry:?}");
            assert_eq!(entry.input_count, 0);
            assert_eq!(entry.source.kind, AddressKind::Absent);
            assert_eq!(entry.flags(), flags::SHIELDED_COMPONENTS);
        }
        // More than two outputs: the first two and the count.
        2 => {
            assert!(!entry.is_complete());
            assert!(entry.more_than_two_outputs());
            assert_eq!(entry.output_count, 5);
            assert_eq!(
                (kind(0), kind(1)),
                (Some(AddressKind::P2pkh), Some(AddressKind::P2pkh))
            );
            assert_eq!(entry.flags(), flags::MORE_THAN_TWO_OUTPUTS);
        }
        // Two source scripts, and an output with no address to show.
        3 => {
            assert!(!entry.is_complete());
            assert!(entry.multiple_source_scripts);
            assert_eq!(kind(0), Some(AddressKind::Other));
            assert_eq!(entry.flags(), flags::MULTIPLE_SOURCE_SCRIPTS);
        }
        // Both pools funded it.
        4 => {
            assert!(!entry.is_complete());
            assert_eq!(
                entry.flags(),
                flags::SHIELDED_COMPONENTS | flags::SHIELDED_AND_TRANSPARENT_FUNDING
            );
        }
        // Coinbase: no input, no fee.
        5 => {
            assert!(entry.is_complete(), "{entry:?}");
            assert_eq!((entry.fee, entry.input_count), (0, 0));
            assert_eq!(entry.flags(), flags::COINBASE);
        }
        _ => unreachable!(),
    }
}

#[test]
fn found_exact_transcript() {
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
        // An archive height needs its index chunk; the recent entry is in
        // the recent map itself.
        let chunk: &[Route] = match tier {
            Tier::Archive => &[Route::MapChunk],
            Tier::Recent => &[],
        };
        let mut client = env.client();
        let mut http = env.http();
        let record = &records[CLASSES];
        let provenance = found(
            client
                .lookup(&mut http, record.txid.0, height, &never)
                .unwrap(),
            record,
        );
        let log = http.take_log();
        // Cold: init, the recent map, the index chunk for an archive, the
        // manifest, the bucket's one directory setup, then exactly two
        // directory queries.
        let expected: Vec<Route> = [Route::Init, Route::Map]
            .into_iter()
            .chain(chunk.iter().copied())
            .chain([Route::Manifest, Route::Setup, Route::Query, Route::Query])
            .collect();
        assert_eq!(routes(&log), expected);
        assert_queries(&log, 0);
        assert!(log[log.len() - 3].path.ends_with("/setup/directory-0/0"));
        assert_eq!(provenance.shard_id, shard_id);
        assert_eq!(&provenance.manifest_digest, digest);
        assert_eq!(provenance.tier, tier);
        assert_eq!(provenance.revision, 0);
        assert_eq!(Some(provenance.map_sha256.as_str()), client.map_sha256());
        assert_eq!(provenance.map_sha256, recent_sha256(&env.world));
        let (up, down) = body_bytes(&log);
        eprintln!("bandwidth cold {tier:?}: up {up} B, down {down} B");
        // Warm: the same two queries only.
        found(
            client
                .lookup(&mut http, record.txid.0, height, &never)
                .unwrap(),
            record,
        );
        let log = http.take_log();
        assert_eq!(routes(&log), [Route::Query, Route::Query]);
        assert_queries(&log, 0);
        let (up, down) = body_bytes(&log);
        assert_eq!((up as u64, down as u64), (2 * QUERY_BYTES, 2 * REPLY_BYTES));
        eprintln!("bandwidth warm {tier:?}: up {up} B, down {down} B");
    }
}

/// Every case the format distinguishes comes back as exactly the entry the
/// publisher derived, flags included, through the same transcript as an
/// absent txid, cold and warm, in both tiers.
#[test]
fn every_case_is_the_published_entry_through_the_absent_transcript() {
    let _serial = serial();
    let env = env();
    for (records, height) in [(&env.world.a0_records, 150), (&env.world.r0_records, 230)] {
        let mut warm = env.client();
        let mut warm_http = env.http();
        assert_eq!(
            warm.lookup(&mut warm_http, ABSENT, height, &never).unwrap(),
            TxidLookup::Absent
        );
        let cold_absent = shape(&warm_http.take_log());
        assert_eq!(
            warm.lookup(&mut warm_http, ABSENT, height, &never).unwrap(),
            TxidLookup::Absent
        );
        let warm_absent = warm_http.take_log();
        assert_queries(&warm_absent, 0);
        let warm_absent = shape(&warm_absent);
        assert_eq!(warm_absent.len(), 2);
        for (class, record) in classes_of(records).iter().enumerate() {
            let mut http = env.http();
            let lookup = env
                .client()
                .lookup(&mut http, record.txid.0, height, &never)
                .unwrap();
            let TxidLookup::Found { entry, .. } = &lookup else {
                panic!("{lookup:?} for class {class}");
            };
            check_class(class, entry);
            found(lookup, record);
            assert_eq!(shape(&http.take_log()), cold_absent, "class {class} cold");
            found(
                warm.lookup(&mut warm_http, record.txid.0, height, &never)
                    .unwrap(),
                record,
            );
            assert_eq!(
                shape(&warm_http.take_log()),
                warm_absent,
                "class {class} warm"
            );
        }
    }
}

#[test]
fn absent_sends_same_transcript_shape() {
    let _serial = serial();
    let env = env();
    let mut client = env.client();
    let mut http = env.http();
    let record = &env.world.a0_records[CLASSES];
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
    // Same requests, same body sizes each way, same statuses.
    assert_eq!(shape(&absent), shape(&present));
    let (up, down) = body_bytes(&absent);
    eprintln!("bandwidth absent cold: up {up} B, down {down} B");
    // Warm absent: two queries only, as warm found.
    found(
        client
            .lookup(&mut http, record.txid.0, 150, &never)
            .unwrap(),
        record,
    );
    let present = http.take_log();
    assert_eq!(
        client.lookup(&mut http, ABSENT, 150, &never).unwrap(),
        TxidLookup::Absent
    );
    let absent = http.take_log();
    assert_eq!(routes(&absent), [Route::Query, Route::Query]);
    assert_eq!(shape(&absent), shape(&present));
    assert_eq!(body_bytes(&absent), body_bytes(&present));
}

#[test]
fn placement_below_or_above_no_post() {
    let _serial = serial();
    let env = env();
    let mut client = env.client();
    let mut http = env.http();
    let txid = env.world.a0_records[CLASSES].txid.0;
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
    let txid = env.world.a0_records[CLASSES].txid.0;
    // A codec this client does not know, the earlier variable-length one
    // included.
    for codec in ["transparent-txid-display-v1", "transparent-txid-display-v9"] {
        let mut http = env.http();
        http.tamper = Some(Box::new(move |request, reply| {
            if request.route == Route::Init {
                let mut init: serde_json::Value = serde_json::from_slice(&reply.body).unwrap();
                init["codec"] = codec.into();
                reply.body = init.to_string().into_bytes();
            }
        }));
        assert_eq!(
            env.client().lookup(&mut http, txid, 150, &never).unwrap(),
            TxidLookup::Unsupported
        );
        assert_eq!(routes(&http.take_log()), [Route::Init]);
    }
    // A recent map schema this client does not know, canonical and correctly
    // announced.
    let mut http = env.http();
    http.tamper = Some(Box::new(|request, reply| {
        if request.route == Route::Map {
            let mut map: DisplayRecentMap = serde_json::from_slice(&reply.body).unwrap();
            map.schema = "transparent-txid-display-shard-v3".into();
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
    let record = &env.world.a0_records[CLASSES];
    // One 409: the recent map is fetched again and the whole lookup
    // retried. The index chunk is cached by digest and not fetched again.
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
            Route::MapChunk,
            Route::Manifest,
            Route::Setup,
            Route::Query,
            Route::Map,
            Route::Query,
            Route::Query
        ]
    );
    // The refetch is the recent map alone, the same bytes as the first.
    let maps: Vec<&Sent> = log.iter().filter(|s| s.route == Route::Map).collect();
    assert_eq!(maps[0].reply_bytes, maps[1].reply_bytes);
    eprintln!(
        "bandwidth map refetch after a 409: down {} B",
        maps[1].reply_bytes
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
            Route::MapChunk,
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
    let record = &env.world.a0_records[CLASSES + 2];
    let mut client = env.client();
    let mut http = env.http();
    // The second directory query is shed: the lookup fails rather than
    // deciding from one row.
    let mut queries = 0;
    http.intercept = Some(Box::new(move |request| {
        queries += usize::from(request.route == Route::Query);
        (request.route == Route::Query && queries == 2).then(|| TxidReply {
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
    assert_eq!(posts(&log), 2);
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
    assert_eq!(posts(&http.log), 2);
    assert_eq!(http.take_log().len(), 2);
}

#[test]
fn misdirected_421_refused() {
    let _serial = serial();
    let env = env();
    let record = &env.world.r0_records[CLASSES];
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
    let found = env.world.a0_records[CLASSES].txid.0;
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
        ("table", "directory-1".into()),
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
    // Pretty rather than compact, correctly announced.
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::MapCanonical,
        on(Route::Map, |reply| {
            let map: DisplayRecentMap = serde_json::from_slice(&reply.body).unwrap();
            reply.body = serde_json::to_vec_pretty(&map).unwrap();
            reply.map_sha256 = Some(hex::encode(Sha256::digest(&reply.body)));
        }),
    );
}

#[test]
fn tampered_index_chunk_protocol_never_absent() {
    let _serial = serial();
    // One byte changed, or appended: no longer the digest the map names.
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Map,
        on(Route::MapChunk, |reply| reply.body[10] ^= 1),
    );
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Map,
        on(Route::MapChunk, |reply| reply.body.push(b' ')),
    );
    // A recent map naming a chunk whose entries it does not chain to,
    // correctly announced: the chunk's digest matches, its contents do not.
    assert_tampered(
        ABSENT,
        150,
        ProtocolKind::Map,
        on(Route::Map, |reply| {
            let mut map: DisplayRecentMap = serde_json::from_slice(&reply.body).unwrap();
            map.chunks[0].start_height += 1;
            map.start_height += 1;
            reply.body = map.to_bytes();
            reply.map_sha256 = Some(map.sha256());
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
    let record = &env.world.a0_records[CLASSES + 2];
    // Cold archive lookup: init, map, chunk, manifest, setup, 2 queries.
    for allowed in 0..7 {
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
        for record in classes_of(records) {
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
            // Nor the tag the tables are keyed by.
            let tag = Tag::of(txid).0;
            assert!(!sent.path.contains(&hex::encode(tag)));
            assert!(!sent.body.windows(tag.len()).any(|w| w == tag));
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
            .lookup(&mut http, records[CLASSES + 2].txid.0, 150, &never)
            .unwrap(),
        &records[CLASSES + 2],
    );
    let cold = http.take_log();
    assert_eq!(posts(&cold), 2);
    assert!(cold.iter().any(|s| s.route == Route::Init));
    for record in classes_of(records) {
        found(
            client
                .lookup(&mut http, record.txid.0, 150, &never)
                .unwrap(),
            record,
        );
        let log = http.take_log();
        assert!(
            log.iter().all(|s| s.method == Method::Post),
            "{:?}",
            routes(&log)
        );
        assert_queries(&log, 0);
    }
    // The other shard needs its own manifest and setups, not init or map.
    let record = &env.world.r0_records[CLASSES];
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

/// The entry for `txid` in `shard` as its published table files hold it,
/// read directly: both candidate rows of the txid's bucket in every segment.
fn published_entry(shard: &Published, txid: &Txid) -> Option<DisplayEntry> {
    let manifest = &shard.manifest;
    let geometry = manifest.display_geometry().unwrap();
    let tag = Tag::of(txid);
    let table = DisplayTable::Directory(display::bucket(&tag, manifest.n_buckets));
    let segments = manifest.segments(table).unwrap().len();
    let mut rows = Vec::new();
    for row in display::candidate_rows(
        &tag,
        manifest.shard_id,
        table.bucket(),
        geometry.directory_rows,
    ) {
        for segment in 0..segments {
            let bytes = std::fs::read(shard.dir.join(table.file_name(segment))).unwrap();
            let at = row as usize * txid::ROW_BYTES;
            rows.push(bytes[at..at + txid::ROW_BYTES].to_vec());
        }
    }
    txid::find_entry(&rows, txid).unwrap()
}

/// Every lookup agrees with the published files read directly, and with the
/// publisher's records: what PIR returns is what the tables hold.
#[test]
fn differential_vs_published_tables() {
    let _serial = serial();
    let env = env();
    let world = &env.world;
    let mut client = env.client();
    let mut http = env.http();
    let coincident = world.r0_records.last().unwrap();
    let mut cases: Vec<(Txid, u64, Option<DisplayEntry>)> = Vec::new();
    for record in &world.a0_records {
        cases.push((record.txid, 150, Some(record.entry)));
    }
    for record in &world.r0_records {
        cases.push((record.txid, 230, Some(record.entry)));
    }
    cases.extend([
        (Txid(ABSENT), 150, None),
        (Txid(ABSENT), 230, None),
        (coincident.txid, 255, Some(coincident.entry)),
        (coincident.txid, 99, None),
        (coincident.txid, 261, None),
        // Published, but in the other shard than its height names.
        (world.a0_records[CLASSES].txid, 230, None),
    ]);
    let mut compared = 0;
    for (txid, height, published) in cases {
        let shard = match height {
            100..=199 => Some(&world.a0),
            200..=260 => Some(&world.r0),
            _ => None,
        };
        let actual = client.lookup(&mut http, txid.0, height, &never).unwrap();
        let log = http.take_log();
        let queries = log.iter().filter(|s| s.route == Route::Query).count();
        match (shard, &actual) {
            (Some(shard), TxidLookup::Found { entry, provenance }) => {
                assert_eq!(published_entry(shard, &txid), Some(*entry));
                assert_eq!(published, Some(*entry));
                assert_eq!(provenance.shard_id, shard.manifest.shard_id);
                assert_eq!(provenance.manifest_digest, shard.digest);
                assert_eq!(provenance.map_sha256, recent_sha256(world));
                assert_eq!(queries, 2);
            }
            (Some(shard), TxidLookup::Absent) => {
                assert_eq!(published_entry(shard, &txid), None);
                assert_eq!(published, None);
                assert_eq!(queries, 2);
            }
            (None, TxidLookup::PlacementUnknown(_)) => assert_eq!(queries, 0),
            (shard, actual) => panic!("{:?}: {actual:?}", shard.map(|s| &s.digest)),
        }
        compared += 1;
    }
    assert_eq!(compared, 46 + 47 + 6);
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
        ("txid-4k", DisplayTable::Directory(3), 0x28bc_e699_56cc_2d7e),
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
    // And what the running server publishes: one table kind, no pages.
    let env = env();
    let init: serde_json::Value = env.rt.block_on(env.world.get("/v1/txid/init")).1;
    assert_eq!(init["codec"], txid::CODEC);
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
    assert_eq!(geometry["txdirectory"]["row_bytes"], txid::ROW_BYTES);
    assert!(geometry.get("txpages").is_none(), "{geometry}");
}

/// A txid in internal byte order from its display hex.
fn parse_txid(display_hex: &str) -> Txid {
    let mut bytes: [u8; 32] = hex::decode(display_hex).unwrap().try_into().unwrap();
    bytes.reverse();
    Txid(bytes)
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
    let txid = parse_txid("fd4667e1a9b427715992cd12b3cdabeb2cfe7623e0e61c8d489f9b0b9a8effbf");
    let lookup = client.lookup(&mut http, txid.0, 3_410_000, &never).unwrap();
    let TxidLookup::Found { entry, provenance } = lookup else {
        panic!("{lookup:?}");
    };
    assert_eq!(entry.tag, Tag::of(&txid));
    assert!(!entry.coinbase);
    assert_eq!((entry.input_count, entry.output_count), (1, 2));
    let values: Vec<u64> = entry.outputs.iter().flatten().map(|o| o.value).collect();
    assert_eq!(values, [1_816_444, 93_460_672_472]);
    assert!(entry
        .outputs
        .iter()
        .flatten()
        .all(|o| o.address.kind == AddressKind::P2pkh));
    assert_eq!(provenance.tier, Tier::Archive);
    let log = http.take_log();
    let (up, down) = body_bytes(&log);
    eprintln!("live cold lookup: {provenance:?}, up {up} B, down {down} B");
    found(
        client.lookup(&mut http, txid.0, 3_410_000, &never).unwrap(),
        &DisplayRecord::new(txid, entry).unwrap(),
    );
    let warm = shape(&http.take_log());
    // Absent at the same height: the same two queries.
    assert_eq!(
        client.lookup(&mut http, ABSENT, 3_410_000, &never).unwrap(),
        TxidLookup::Absent
    );
    let absent = http.take_log();
    assert_eq!(routes(&absent), [Route::Query, Route::Query]);
    assert_eq!(shape(&absent), warm);
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
    let p0 = recent_sha256_of(&world.p0.0);
    assert_eq!(hex32(first), p0);
    assert_eq!(client.map_sha256(), Some(p0.as_str()));
    assert_eq!(routes(&http.take_log()), [Route::Map]);
    let added = pair(30, 1, synth::record(30, 1, 25, 0));
    assert_eq!(
        client.lookup(&mut http, added.txid.0, 265, &never).unwrap(),
        TxidLookup::PlacementUnknown(Placement::Above)
    );
    // Unchanged publication: the same digest again.
    assert_eq!(client.refresh_map(&mut http, &never).unwrap(), first);

    // The recent shard grows to 270 with one more record.
    let mut records = world.r0_records.clone();
    records.push(added);
    let grown = write_shard(
        world.root.path(),
        &spec(1, 200, 270, false, &world.a0.digest),
        &records,
    );
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
    let p1_recent = recent_sha256_of(&p1.0);
    assert_eq!(hex32(second), p1_recent);
    assert_eq!(client.map_sha256(), Some(p1_recent.as_str()));
    assert_eq!(routes(&http.take_log()), [Route::Map]);
    // The refreshed map is the one lookups use: no further map fetch.
    let provenance = found(
        client.lookup(&mut http, added.txid.0, 265, &never).unwrap(),
        &added,
    );
    assert_eq!(provenance.map_sha256, p1_recent);
    assert_eq!(provenance.manifest_digest, grown.digest);
    assert!(!routes(&http.take_log()).contains(&Route::Map));
    env.rt.block_on(async move { drop(world) });
}
