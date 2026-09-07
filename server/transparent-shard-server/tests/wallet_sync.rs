//! The POC end to end: a wallet syncs from a birthday against a real shard
//! server, over real HTTP, with real PIR, and its recovered ledger is compared
//! exactly against an independent traversal of the same events.
//!
//! This is the test the whole thing is for. Everything else checks a piece.
//!
//! Equality is exact — every UTXO and every spend — not just the balance. Two
//! errors can sum to the right number, and a balance check would pass a
//! reconstruction that had lost a receive and a spend of equal value.

use std::collections::BTreeMap;
use std::path::Path;
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
use transparent_filter::{
    filter_hash, BlockHash, ScriptBytes, SealParameters, ShardMap, ShardMapEntry,
};
use transparent_shard::build::build_shard;
use transparent_shard::layout::{Geometry, RECENT_4K, RECENT_8K};
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry, SCHEMA,
};
use transparent_shard_server::service::{router, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS};
use transparent_wallet::client::Table;
use transparent_wallet::ledger::Ledger;
use transparent_wallet::sync::{sync, GeometryParams, ServiceGeometry};
use transparent_wallet::transport::{BoxError, FilterSource, ShardTransport};

const GENESIS: &str = transparent_filter::MAINNET_GENESIS_DISPLAY;
const FIRST: u64 = 3_428_143;
const SPAN: u64 = 200;
const SHARDS: u64 = 4;

fn genesis() -> BlockHash {
    BlockHash::from_display_hex(GENESIS).unwrap()
}

fn hash_at(height: u64) -> BlockHash {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&height.to_le_bytes());
    BlockHash::from_internal_bytes(bytes)
}

fn script(tag: u32) -> ScriptBytes {
    let mut bytes = vec![0x76, 0xa9, 0x14];
    bytes.extend_from_slice(&tag.to_le_bytes());
    bytes.extend_from_slice(&[0u8; 16]);
    bytes.extend_from_slice(&[0x88, 0xac]);
    ScriptBytes::new(bytes)
}

fn txid(tag: u64) -> Txid {
    let mut bytes = [0u8; 32];
    bytes[..8].copy_from_slice(&tag.to_le_bytes());
    Txid(bytes)
}

/// A synthetic chain covering the cases that matter: a fully spent script, an
/// unspent one active in two shards, a history long enough to need pages, and
/// unrelated activity so the wallet's scripts are not alone in the filters.
fn chain() -> Vec<Vec<(ScriptBytes, TransparentEvent)>> {
    let mut per_shard: Vec<Vec<(ScriptBytes, TransparentEvent)>> =
        (0..SHARDS).map(|_| Vec::new()).collect();
    let mut push = |height: u64, s: ScriptBytes, event: TransparentEvent| {
        per_shard[((height - FIRST) / SPAN) as usize].push((s, event));
    };

    push(
        FIRST + 5,
        script(1),
        TransparentEvent::Receive(ReceiveEvent {
            height: (FIRST + 5) as u32,
            txid: txid(100),
            transaction_index: 0,
            output_index: 0,
            value: 50_000,
            coinbase: false,
        }),
    );
    push(
        FIRST + 2 * SPAN + 10,
        script(1),
        TransparentEvent::Spend(SpendEvent {
            height: (FIRST + 2 * SPAN + 10) as u32,
            spending_txid: txid(200),
            transaction_index: 0,
            input_index: 0,
            spent_txid: txid(100),
            spent_output_index: 0,
        }),
    );

    for (n, height) in [FIRST + SPAN + 3, FIRST + 3 * SPAN + 7].iter().enumerate() {
        push(
            *height,
            script(2),
            TransparentEvent::Receive(ReceiveEvent {
                height: *height as u32,
                txid: txid(300 + n as u64),
                transaction_index: 1,
                output_index: 0,
                value: 7_000 + n as u64,
                coinbase: false,
            }),
        );
    }

    let long = transparent_shard::EVENTS_PER_PAGE + transparent_shard::INLINE_EVENTS + 5;
    for i in 0..long {
        let height = FIRST + SPAN + 20 + u64::from(i) % 100;
        push(
            height,
            script(3),
            TransparentEvent::Receive(ReceiveEvent {
                height: height as u32,
                txid: txid(1_000 + u64::from(i)),
                transaction_index: 2,
                output_index: 0,
                value: 11,
                coinbase: false,
            }),
        );
    }

    for shard in 0..SHARDS {
        for tag in 100..400u32 {
            let height = FIRST + shard * SPAN + u64::from(tag % 50);
            push(
                height,
                script(tag),
                TransparentEvent::Receive(ReceiveEvent {
                    height: height as u32,
                    txid: txid(u64::from(tag) * 1_000 + shard),
                    transaction_index: 3,
                    output_index: 0,
                    value: 1,
                    coinbase: false,
                }),
            );
        }
    }
    per_shard
}

/// Writes a publishable shard set to `dir`, as the publisher would.
fn publish(dir: &Path, per_shard: &[Vec<(ScriptBytes, TransparentEvent)>]) -> ShardMap {
    publish_with(dir, per_shard, |_| &RECENT_8K)
}

/// Writes a publishable shard set whose shards may name different geometries.
///
/// The two-tier set the deployment plan describes is this: archive geometry
/// below the recent cutoff, recent geometry from it. Nothing else about the
/// publication changes, which is the property worth testing — a wallet must
/// cross the boundary without being told it is there.
fn publish_with(
    dir: &Path,
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    geometry_for: impl Fn(u64) -> &'static Geometry,
) -> ShardMap {
    let mut entries = Vec::new();
    let mut parent_digest = String::new();
    for (shard_id, events) in per_shard.iter().enumerate() {
        let shard_id = shard_id as u64;
        let geometry = geometry_for(shard_id);
        let start = FIRST + shard_id * SPAN;
        let end = start + SPAN - 1;
        let built = build_shard(
            shard_id,
            start,
            end,
            genesis(),
            hash_at(end),
            transparent_filter::RANGE_PROFILE,
            geometry,
            events,
        )
        .expect("build");

        let manifest = ShardManifest {
            schema: SCHEMA.to_string(),
            profile: transparent_filter::RANGE_PROFILE.to_string(),
            geometry: geometry.name.to_string(),
            network: transparent_filter::NETWORK.to_string(),
            genesis_hash: GENESIS.to_string(),
            shard_id,
            start_height: start,
            end_height: end,
            parent_block_hash: hash_at(start - 1).to_display_hex(),
            terminal_block_hash: hash_at(end).to_display_hex(),
            parent_manifest_digest: parent_digest.clone(),
            sealed: shard_id + 1 < SHARDS,
            revision: 0,
            supersedes: String::new(),
            seal: ManifestSeal {
                scripts_target: 8_192,
                scripts_capacity: 16_384,
                page_rows_target: 2_048,
                page_rows_capacity: 4_096,
            },
            layout: ManifestLayout {
                max_script_bytes: transparent_shard::MAX_SCRIPT_BYTES as u32,
                inline_events: transparent_shard::INLINE_EVENTS,
                events_per_page: transparent_shard::EVENTS_PER_PAGE,
                page_row_header_bytes: transparent_shard::PAGE_ROW_HEADER_BYTES as u32,
                page_entry_header_bytes: transparent_shard::PAGE_ENTRY_HEADER_BYTES as u32,
                directory_choices: transparent_shard::build::DIRECTORY_CHOICES as u32,
            },
            filter_hash: filter_hash(built.filter.as_slice()).to_display_hex(),
            directory_segments: built
                .directory
                .iter()
                .map(|segment| TableGeometry {
                    rows: geometry.directory_rows,
                    row_bytes: geometry.directory_row_bytes as u32,
                    sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment)),
                })
                .collect(),
            page_segments: built
                .pages
                .iter()
                .map(|segment| TableGeometry {
                    rows: geometry.page_rows,
                    row_bytes: geometry.page_row_bytes as u32,
                    sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment)),
                })
                .collect(),
            occupancy: ManifestOccupancy {
                scripts: built.scripts,
                page_rows: built.page_rows,
                fragments: built.fragments,
                events: built.events,
                blocks: SPAN,
                txids: 0,
                excluded_scripts: built.excluded_scripts,
            },
        };

        let digest = manifest.digest();
        let shard_dir = dir.join(&digest);
        std::fs::create_dir_all(&shard_dir).unwrap();
        std::fs::write(shard_dir.join("manifest.json"), manifest.canonical_bytes()).unwrap();
        std::fs::write(shard_dir.join("filter.bin"), built.filter.as_slice()).unwrap();
        for (index, segment) in built.directory.iter().enumerate() {
            std::fs::write(shard_dir.join(format!("directory.{index}.bin")), segment).unwrap();
        }
        for (index, segment) in built.pages.iter().enumerate() {
            std::fs::write(shard_dir.join(format!("pages.{index}.bin")), segment).unwrap();
        }

        entries.push(ShardMapEntry {
            shard_id,
            geometry: geometry.name.to_string(),
            start_height: start,
            end_height: end,
            parent_block_hash: manifest.parent_block_hash.clone(),
            terminal_block_hash: manifest.terminal_block_hash.clone(),
            filter_hash: manifest.filter_hash.clone(),
            scripts: built.scripts,
            page_rows: built.page_rows,
            txids: 0,
            directory_segments: built.directory_segments(),
            page_segments: built.page_segments(),
            manifest_digest: digest.clone(),
            revision: 0,
            sealed: manifest.sealed,
        });
        parent_digest = digest;
    }

    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: entries
            .iter()
            .map(|entry| {
                (
                    entry.geometry.clone(),
                    SealParameters {
                        max_scripts: 8_192,
                        max_page_rows: 2_048,
                        max_txids: 0,
                    },
                )
            })
            .collect(),
        shards: entries,
    };
    std::fs::write(
        dir.join("shards.json"),
        serde_json::to_vec_pretty(&map).unwrap(),
    )
    .unwrap();
    map
}

/// Filters read from the published files, charged at their real size.
///
/// A separate source from the private transport on purpose: a wallet must not
/// fetch public bytes from the same place it makes private requests.
struct PublishedFilters {
    filters: BTreeMap<u64, Vec<u8>>,
    map: Vec<u8>,
}

impl PublishedFilters {
    fn load(dir: &Path, map: &ShardMap) -> Self {
        let mut filters = BTreeMap::new();
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if !path.is_dir() {
                continue;
            }
            let raw = std::fs::read(path.join("manifest.json")).unwrap();
            let manifest: ShardManifest = serde_json::from_slice(&raw).unwrap();
            filters.insert(
                manifest.shard_id,
                std::fs::read(path.join("filter.bin")).unwrap(),
            );
        }
        Self {
            filters,
            map: serde_json::to_vec(map).unwrap(),
        }
    }
}

impl FilterSource for PublishedFilters {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        Ok((self.map.clone(), self.map.len() as u64))
    }

    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self.filters.get(&shard_id).ok_or("no such shard")?.clone();
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }
}

/// Blocking HTTP against the running shard service.
struct HttpShards {
    base: String,
    client: reqwest::blocking::Client,
}

impl ShardTransport for HttpShards {
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

    fn setup(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self
            .client
            .get(format!(
                "{}/v1/shards/{shard_id}/revisions/{revision}/setup/{}/{segment}",
                self.base,
                table.as_str()
            ))
            .send()?
            .error_for_status()?
            .bytes()?
            .to_vec();
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
        Ok(self
            .client
            .post(format!(
                "{}/v1/shards/{shard_id}/revisions/{revision}/query/{}",
                self.base,
                table.as_str()
            ))
            .body(body.to_vec())
            .send()?
            .error_for_status()?
            .bytes()?
            .to_vec())
    }
}

/// Parses the service's init document into what the wallet checks it against.
///
/// The service publishes one entry per geometry it holds, so a two-tier set
/// yields two and a single-tier set one. The wallet re-derives each scheme from
/// the dimensions published beside it and refuses any that does not reproduce.
fn parse_init(raw: &[u8]) -> ServiceGeometry {
    let init: serde_json::Value = serde_json::from_slice(raw).unwrap();
    ServiceGeometry {
        schema: init["schema"].as_str().unwrap().to_string(),
        geometries: init["geometries"]
            .as_array()
            .expect("init publishes geometries")
            .iter()
            .map(|entry| GeometryParams {
                name: entry["name"].as_str().unwrap().to_string(),
                directory_rows: entry["directory_rows"].as_u64().unwrap(),
                directory_row_bytes: entry["directory_row_bytes"].as_u64().unwrap() as u32,
                directory_scheme: serde_json::from_value(entry["directory_scheme"].clone())
                    .unwrap(),
                directory_setup_seed: entry["directory_setup_seed"].as_u64().unwrap(),
                page_rows: entry["page_rows"].as_u64().unwrap(),
                page_row_bytes: entry["page_row_bytes"].as_u64().unwrap() as u32,
                pages_scheme: serde_json::from_value(entry["pages_scheme"].clone()).unwrap(),
                pages_setup_seed: entry["pages_setup_seed"].as_u64().unwrap(),
            })
            .collect(),
    }
}

/// The independent result: the same events replayed with no retrieval at all.
fn traverse(
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    wallet: &[ScriptBytes],
    from_shard: usize,
) -> Ledger {
    let mut events: Vec<(Vec<u8>, TransparentEvent)> = Vec::new();
    for shard in per_shard.iter().skip(from_shard) {
        for (s, event) in shard {
            if wallet.iter().any(|w| w.as_slice() == s.as_slice()) {
                events.push((s.as_slice().to_vec(), *event));
            }
        }
    }
    let mut ledger = Ledger::new();
    ledger.replay(&mut events).expect("traversal");
    ledger
}

fn compare(recovered: &Ledger, expected: &Ledger) {
    let mut mine: Vec<_> = recovered.utxos().cloned().collect();
    let mut theirs: Vec<_> = expected.utxos().cloned().collect();
    mine.sort_by_key(|u| (u.txid, u.output_index));
    theirs.sort_by_key(|u| (u.txid, u.output_index));
    assert_eq!(mine, theirs, "UTXO sets differ");

    let mut my_spends = recovered.spends().to_vec();
    let mut their_spends = expected.spends().to_vec();
    my_spends.sort_by_key(|s| (s.spent_txid, s.spent_output_index));
    their_spends.sort_by_key(|s| (s.spent_txid, s.spent_output_index));
    assert_eq!(my_spends, their_spends, "spend sets differ");

    assert_eq!(recovered.confirmed_balance(), expected.confirmed_balance());
    assert_eq!(recovered.history(), expected.history(), "histories differ");
}

/// Starts the service on an ephemeral port and returns its base URL.
async fn serve(dir: &Path) -> String {
    let set = ShardSet::open(dir, DEFAULT_RETAIN_REVISIONS).expect("load");
    let state = ServiceState::build(set, ServiceConfig::default()).expect("state");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, router(state)).await.unwrap();
    });
    format!("http://{addr}")
}

async fn run_sync(
    dir: &Path,
    base: String,
    wallet: Vec<ScriptBytes>,
    birthday: u64,
    map: ShardMap,
) -> transparent_wallet::SyncOutcome {
    let filters = PublishedFilters::load(dir, &map);
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = HttpShards {
            base: base.clone(),
            client: client.clone(),
        };
        // The geometry is taken from init and then re-derived by the client,
        // which refuses it if it does not match this build's pinned constants.
        let raw = client
            .get(format!("{base}/v1/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap();
        let geometry = parse_init(&raw);
        let mut filters = filters;
        sync(
            &map,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            birthday,
        )
        .expect("sync")
    })
    .await
    .unwrap()
}

/// Two of a wallet's own scripts sharing a page row still cost two fetches.
///
/// A packed row hands the wallet other scripts' histories, including sometimes
/// its own. Satisfying the second script from the first response would be free
/// and is forbidden: it would make the number of requests depend on which
/// scripts happen to share a row, which is a fact about strangers' history and
/// about the builder's placement. A server watching request counts could learn
/// from it. The wallet must therefore fetch a row it already holds.
#[tokio::test(flavor = "multi_thread")]
async fn scripts_sharing_a_row_still_cost_a_fetch_each() {
    let dir = tempfile::tempdir().unwrap();
    // Three events each: two inline, one paged, so all three are class 1 and
    // pack into one row together.
    let mut events = Vec::new();
    for tag in 0..3u32 {
        for i in 0..3u32 {
            events.push((
                script(tag),
                TransparentEvent::Receive(ReceiveEvent {
                    height: (FIRST + u64::from(i)) as u32,
                    txid: txid(u64::from(tag) * 10 + u64::from(i)),
                    transaction_index: i as u16,
                    output_index: 0,
                    value: 500 + u64::from(i),
                    coinbase: false,
                }),
            ));
        }
    }
    let per_shard = vec![events];
    let map = publish(dir.path(), &per_shard);
    assert_eq!(
        map.shards[0].page_segments, 1,
        "three short histories share one row in one segment"
    );
    let base = serve(dir.path()).await;

    for scripts in 1..=3usize {
        let wallet: Vec<ScriptBytes> = (0..scripts as u32).map(script).collect();
        let outcome = run_sync(dir.path(), base.clone(), wallet.clone(), FIRST, map.clone()).await;
        let expected = traverse(&per_shard, &wallet, 0);
        compare(&outcome.ledger, &expected);

        // Two directory candidates per script, and one page fetch per script
        // even though one row answers all of them. Asserted per table rather
        // than as a sum, because the sum would also be satisfied by a wallet
        // that deduplicated the page fetches and made up the difference in
        // directory queries — and not deduplicating them is the point.
        assert_eq!(
            outcome.charges.directory.queries,
            (scripts * 2) as u64,
            "{scripts} scripts should cost {} directory queries",
            scripts * 2
        );
        assert_eq!(
            outcome.charges.pages.queries, scripts as u64,
            "a shared row must still be fetched once per script that needs it"
        );
        assert_eq!(outcome.charges.queries(), (scripts * 3) as u64);
    }
}

/// A wallet must refuse a service serving a layout it does not read.
///
/// The rows carry no version of their own — a fixed-width row has nowhere to
/// put one without spending space on every row — so nothing about a v4 page row
/// makes it self-evidently not a v5 one. Fed to the packed decoder, its first
/// four bytes are a script length and two script bytes, which read as an entry
/// count in the billions and are rejected by luck rather than by design; the
/// reverse direction is not reliably rejected at all. So the schema is checked
/// before any row is decoded, and this is that check.
#[tokio::test(flavor = "multi_thread")]
async fn a_service_serving_another_schema_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;
    let wallet = vec![script(1)];
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    let filters = PublishedFilters::load(dir.path(), &map);

    let error = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = HttpShards {
            base: base.clone(),
            client: client.clone(),
        };
        let raw = client
            .get(format!("{base}/v1/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap();
        let mut geometry = parse_init(&raw);
        assert_eq!(geometry.schema, transparent_shard::SCHEMA);
        geometry.schema = "transparent-shard-v4".to_string();
        let mut filters = filters;
        sync(
            &map,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            FIRST,
        )
        .err()
        .expect("a foreign schema must be refused")
    })
    .await
    .unwrap();

    assert!(
        matches!(error, transparent_wallet::SyncError::Schema { .. }),
        "expected a schema refusal, got {error}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_wallet_syncs_from_its_birthday_and_matches_an_independent_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let tail = map.shards.last().expect("a tail").clone();
    let base = serve(dir.path()).await;

    // Script 999 is derived but never used: a wallet always carries scripts it
    // has no activity for, and they must cost filter checks and nothing more.
    let wallet = vec![script(1), script(2), script(3), script(999)];
    let outcome = run_sync(dir.path(), base, wallet.clone(), FIRST, map).await;

    let expected = traverse(&per_shard, &wallet, 0);
    compare(&outcome.ledger, &expected);
    assert!(outcome.ledger.unresolved().is_empty());

    // The fixture must actually exercise what it claims to.
    assert!(expected.confirmed_balance() > 0);
    assert_eq!(expected.spends().len(), 1);
    // Expressed against the geometry rather than a fixed number: the fixture's
    // long history is sized to exceed one page, and what matters is that it
    // still does, whatever the page width is.
    let one_page = (transparent_shard::EVENTS_PER_PAGE + transparent_shard::INLINE_EVENTS) as usize;
    assert!(
        expected.utxos().count() > one_page,
        "the long history should span more than one page ({} of {one_page})",
        expected.utxos().count()
    );

    assert_eq!(outcome.covered_through, FIRST + SHARDS * SPAN - 1);
    // The last shard is a provisional tail revision. Coverage through it is
    // current but not settled, and it is recorded with the revision digest that
    // produced it, so a later revision replaces that range rather than
    // extending it.
    assert!(!tail.sealed, "the fixture's last shard must be the tail");
    assert_eq!(outcome.settled_through, FIRST + (SHARDS - 1) * SPAN - 1);
    assert_eq!(outcome.provisional.len(), 1);
    assert_eq!(outcome.provisional[0].shard_id, tail.shard_id);
    assert_eq!(outcome.provisional[0].revision, tail.revision);
    assert_eq!(outcome.provisional[0].manifest_digest, tail.manifest_digest);
    assert_eq!(
        outcome.charges.filters_checked, SHARDS,
        "every filter in range is downloaded, matched or not"
    );
    assert!(outcome.charges.queries() > 0);
    assert!(outcome.charges.filter_bytes > 0);
    assert!(outcome.charges.setup_bytes() > 0);

    eprintln!(
        "sync: shards {:?} filters {} B, setup {} B, queries {} ({} up, {} down), total {} B",
        outcome.matched_shards,
        outcome.charges.filter_bytes,
        outcome.charges.setup_bytes(),
        outcome.charges.queries(),
        outcome.charges.query_upload(),
        outcome.charges.query_download(),
        outcome.charges.total()
    );
}

/// A shard whose content does not fit one segment of the pinned geometry.
///
/// Packing makes this harder to provoke than it was, and that is the point: a
/// short history no longer costs a row of its own, so thousands of them no
/// longer overrun anything. What still does is history that cannot share — one
/// full fragment plus one event is two rows belonging to one script, because a
/// long history keeps its run to itself. Half the page table's rows' worth of
/// those, plus one, and the table overruns by exactly one row.
///
/// The design requires such a shard to be published rather than refused, so it
/// has to survive a real sync end to end and not merely build.
fn oversized_chain() -> Vec<Vec<(ScriptBytes, TransparentEvent)>> {
    // Two inline, then a full fragment and one more, so the history is long and
    // takes two rows that nothing else may share.
    let per_script = transparent_shard::INLINE_EVENTS + transparent_shard::EVENTS_PER_PAGE + 1;
    let scripts = transparent_shard::PAGE_ROWS as u32 / 2 + 1;
    let mut events = Vec::new();
    for tag in 0..scripts {
        for i in 0..per_script {
            let height = FIRST + u64::from(i) % SPAN;
            events.push((
                script(tag),
                TransparentEvent::Receive(ReceiveEvent {
                    height: height as u32,
                    txid: txid(u64::from(tag) * 1_000 + u64::from(i)),
                    transaction_index: (i % 1_000) as u16,
                    output_index: 0,
                    value: 100 + u64::from(i),
                    coinbase: false,
                }),
            ));
        }
    }
    vec![events]
}

/// The wallet for the oversized fixture: scripts spread across the page space,
/// so at least one of them is located past the first segment's boundary.
fn oversized_wallet() -> Vec<ScriptBytes> {
    let last = transparent_shard::PAGE_ROWS as u32 / 2;
    vec![script(0), script(last / 2), script(last)]
}

/// The availability case, end to end. A shard with two page segments must
/// reconstruct exactly the same ledger as a traversal of the same events, and
/// must cost one published setup per segment rather than one per shard.
#[tokio::test(flavor = "multi_thread")]
async fn a_shard_whose_pages_need_two_segments_syncs_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = oversized_chain();
    let map = publish(dir.path(), &per_shard);
    assert_eq!(
        map.shards[0].page_segments, 2,
        "the fixture must actually overrun one segment"
    );
    assert_eq!(map.shards[0].directory_segments, 1);
    let base = serve(dir.path()).await;

    let wallet = oversized_wallet();
    let outcome = run_sync(dir.path(), base, wallet.clone(), FIRST, map).await;

    let expected = traverse(&per_shard, &wallet, 0);
    compare(&outcome.ledger, &expected);
    assert!(outcome.ledger.unresolved().is_empty());
    let per_script =
        (transparent_shard::INLINE_EVENTS + transparent_shard::EVENTS_PER_PAGE + 1) as usize;
    assert_eq!(
        expected.utxos().count(),
        wallet.len() * per_script,
        "a full fragment and one more, per wallet script"
    );

    // One setup per segment: the directory's one, and the pages' two. That is
    // the cost the segment path adds, and it is what the analysis must carry.
    // Stated per table, because "three setups" alone would also be satisfied by
    // a wallet that split them the other way round.
    assert_eq!(outcome.charges.directory.segments_opened, 1);
    assert_eq!(outcome.charges.pages.segments_opened, 2);
    assert_eq!(outcome.charges.shards_opened(), 3);
    assert_eq!(outcome.covered_through, FIRST + SPAN - 1);
}

/// Every segment must be accounted for before coverage moves. A wallet that
/// accepted a short answer would advance over history it never retrieved and
/// report a balance missing whatever the dropped segment held.
#[tokio::test(flavor = "multi_thread")]
async fn coverage_does_not_advance_when_a_segment_is_missing() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = oversized_chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;

    let filters = PublishedFilters::load(dir.path(), &map);
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    let wallet = oversized_wallet();
    let error = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let raw = client
            .get(format!("{base}/v1/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap();
        let geometry = parse_init(&raw);
        let mut transport = DropsTheLastSegment {
            inner: HttpShards {
                base: base.clone(),
                client,
            },
        };
        let mut filters = filters;
        sync(
            &map,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            FIRST,
        )
        .err()
        .expect("a short answer must not be accepted")
    })
    .await
    .unwrap();

    assert!(
        error.to_string().contains("length"),
        "expected a length refusal, got: {error}"
    );
}

/// Returns only the first segment's body, as a service that quietly stopped
/// serving a segment would.
struct DropsTheLastSegment {
    inner: HttpShards,
}

impl ShardTransport for DropsTheLastSegment {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.init()
    }

    fn setup(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.setup(shard_id, revision, table, segment)
    }

    fn query(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        body: &[u8],
    ) -> Result<Vec<u8>, BoxError> {
        let mut answer = self.inner.query(shard_id, revision, table, body)?;
        if table == Table::Pages {
            answer.truncate(answer.len() / 2);
        }
        Ok(answer)
    }
}

/// A birthday inside the set skips earlier shards entirely, and the result must
/// match a traversal that skips the same events — including the unresolved
/// spend of an output received before the birthday.
#[tokio::test(flavor = "multi_thread")]
async fn a_later_birthday_recovers_only_history_from_that_shard_forward() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;

    let wallet = vec![script(1), script(2)];
    let birthday = map.shards[2].start_height;
    let outcome = run_sync(dir.path(), base, wallet.clone(), birthday, map).await;

    let expected = traverse(&per_shard, &wallet, 2);
    compare(&outcome.ledger, &expected);

    assert_eq!(
        outcome.charges.filters_checked,
        SHARDS - 2,
        "shards before the birthday are not even filtered"
    );
    // Script 1's receive is before the birthday and its spend is after, so the
    // wallet sees money leave an output it never saw arrive. That must be
    // reported, not ignored: ignoring it yields a balance that is simply wrong.
    assert_eq!(outcome.ledger.unresolved().len(), 1);
}

/// An unused wallet checks every filter and retrieves nothing. This is the case
/// the whole arrangement exists to make cheap, so it must not merely work — it
/// must issue no private queries at all.
#[tokio::test(flavor = "multi_thread")]
async fn an_unused_wallet_pays_only_the_public_floor() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;

    let wallet = vec![script(900_001), script(900_002)];
    let outcome = run_sync(dir.path(), base, wallet, FIRST, map).await;

    assert_eq!(outcome.ledger.confirmed_balance(), 0);
    assert!(outcome.ledger.utxos().next().is_none());
    assert!(outcome.matched_shards.is_empty());
    assert_eq!(
        outcome.charges.queries(),
        0,
        "no private work without a match"
    );
    assert_eq!(outcome.charges.setup_bytes(), 0, "no shard is even opened");
    assert_eq!(outcome.charges.filters_checked, SHARDS);
    assert_eq!(
        outcome.charges.total(),
        outcome.charges.public_floor(),
        "an unused wallet pays the floor and nothing else"
    );
}

/// A set whose shards do not share a geometry syncs exactly, and the wallet is
/// never told where the boundary is.
///
/// This is the deployment plan's two-tier shape: archive geometry for old, dense
/// history and a narrower geometry for the recent window. The pair used here is
/// `recent-4k` and `recent-8k` rather than a true archive geometry, because the
/// mechanism under test is *two row counts in one set* and a 32,768-row table
/// costs 117 MB per segment to build for no additional coverage.
///
/// Equality is exact, against the same independent traversal every other sync
/// test uses. A wallet that read one tier at the other's row count would not
/// error: `split_row` would land it on a real row of a real table, and it would
/// recover a plausible, wrong history. That is the failure this is looking for.
#[tokio::test(flavor = "multi_thread")]
async fn a_wallet_syncs_across_a_geometry_boundary_exactly() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    // The first half is the archive tier, the second the recent one. The
    // boundary falls inside the wallet's history on purpose: script 2 is active
    // in two shards, so it is recovered from both tiers in one sync.
    let map = publish_with(dir.path(), &per_shard, |shard_id| {
        if shard_id < SHARDS / 2 {
            &RECENT_4K
        } else {
            &RECENT_8K
        }
    });
    assert_eq!(map.shards[0].geometry, RECENT_4K.name);
    assert_eq!(
        map.shards[(SHARDS - 1) as usize].geometry,
        RECENT_8K.name,
        "the set must actually span two geometries"
    );
    assert_eq!(
        map.seal.len(),
        2,
        "each tier publishes the thresholds it was sealed under"
    );

    let base = serve(dir.path()).await;
    let wallet = vec![script(1), script(2), script(3)];
    let outcome = run_sync(dir.path(), base, wallet.clone(), FIRST, map).await;

    let expected = traverse(&per_shard, &wallet, 0);
    compare(&outcome.ledger, &expected);
    assert_eq!(outcome.covered_through, FIRST + SHARDS * SPAN - 1);
}

/// The service declares one parameter set per geometry it holds, not one per
/// shard. That sharing is the entire reason geometries are named from a closed
/// registry, so it is asserted rather than assumed.
#[tokio::test(flavor = "multi_thread")]
async fn init_declares_one_parameter_set_per_geometry() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    publish_with(dir.path(), &per_shard, |shard_id| {
        if shard_id < SHARDS / 2 {
            &RECENT_4K
        } else {
            &RECENT_8K
        }
    });
    let base = serve(dir.path()).await;

    let raw = tokio::task::spawn_blocking(move || {
        reqwest::blocking::get(format!("{base}/v1/shards/init"))
            .unwrap()
            .bytes()
            .unwrap()
            .to_vec()
    })
    .await
    .unwrap();
    let geometry = parse_init(&raw);

    assert_eq!(geometry.geometries.len(), 2, "four shards, two geometries");
    let names: Vec<&str> = geometry
        .geometries
        .iter()
        .map(|entry| entry.name.as_str())
        .collect();
    assert_eq!(names, vec![RECENT_4K.name, RECENT_8K.name]);

    // The dimensions travel with the scheme so the client can check the pair.
    // A service that published one geometry's name over another's row counts
    // would be choosing the geometry for the wallet.
    for entry in &geometry.geometries {
        let registered = transparent_shard::layout::by_name(&entry.name).unwrap();
        assert_eq!(entry.directory_rows, registered.directory_rows);
        assert_eq!(entry.page_rows, registered.page_rows);
    }
    // Different geometries must not share a setup seed, or a client that mixed
    // them up would reproduce the wrong setup without noticing.
    assert_ne!(
        geometry.geometries[0].directory_setup_seed,
        geometry.geometries[1].directory_setup_seed
    );
    assert_ne!(
        geometry.geometries[0].directory_setup_seed,
        geometry.geometries[0].pages_setup_seed
    );
}

/// A shard naming a geometry this build does not know must fail the sync, not
/// be skipped.
///
/// Skipping is the tempting behaviour and the dangerous one: the shard's range
/// would pass out of `covered_through` unread, and the wallet would report a
/// synchronised balance over history it never retrieved. There is no safe way
/// to continue past a range that cannot be decoded.
#[tokio::test(flavor = "multi_thread")]
async fn a_shard_naming_an_unknown_geometry_stops_the_sync() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let mut map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;
    let filters = PublishedFilters::load(dir.path(), &map);

    // A geometry from a future build. The map still has to be well formed, so
    // it carries seal parameters for the name as a real publisher would.
    map.shards[1].geometry = "recent-2k".to_string();
    map.seal.insert(
        "recent-2k".to_string(),
        SealParameters {
            max_scripts: 8_192,
            max_page_rows: 2_048,
            max_txids: 0,
        },
    );
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    let wallet = vec![script(1), script(2), script(3)];

    let error = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = HttpShards {
            base: base.clone(),
            client: client.clone(),
        };
        let raw = client
            .get(format!("{base}/v1/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap();
        let geometry = parse_init(&raw);
        let mut filters = filters;
        sync(
            &map,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            FIRST,
        )
        .err()
        .expect("an unknown geometry must stop the sync")
    })
    .await
    .unwrap();

    assert!(
        matches!(error, transparent_wallet::SyncError::UnknownGeometry(ref name) if name == "recent-2k"),
        "expected an unknown-geometry refusal, got {error}"
    );
}
