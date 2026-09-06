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
use transparent_shard::manifest::{
    ManifestLayout, ManifestOccupancy, ManifestSeal, ShardManifest, TableGeometry, SCHEMA,
};
use transparent_shard_server::service::{router, ServiceState};
use transparent_shard_server::shardset::ShardSet;
use transparent_wallet::client::Table;
use transparent_wallet::ledger::Ledger;
use transparent_wallet::sync::{sync, ServiceGeometry};
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
    let mut entries = Vec::new();
    let mut parent_digest = String::new();
    for (shard_id, events) in per_shard.iter().enumerate() {
        let shard_id = shard_id as u64;
        let start = FIRST + shard_id * SPAN;
        let end = start + SPAN - 1;
        let built = build_shard(
            shard_id,
            start,
            end,
            genesis(),
            hash_at(end),
            transparent_filter::RANGE_PROFILE,
            events,
        )
        .expect("build");

        let manifest = ShardManifest {
            schema: SCHEMA.to_string(),
            profile: transparent_filter::RANGE_PROFILE.to_string(),
            network: transparent_filter::NETWORK.to_string(),
            genesis_hash: GENESIS.to_string(),
            shard_id,
            start_height: start,
            end_height: end,
            parent_block_hash: hash_at(start - 1).to_display_hex(),
            terminal_block_hash: hash_at(end).to_display_hex(),
            parent_manifest_digest: parent_digest.clone(),
            sealed: shard_id + 1 < SHARDS,
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
                directory_choices: transparent_shard::build::DIRECTORY_CHOICES as u32,
            },
            filter_hash: filter_hash(built.filter.as_slice()).to_display_hex(),
            directory: TableGeometry {
                rows: transparent_shard::DIRECTORY_ROWS as u64,
                row_bytes: transparent_shard::DIRECTORY_ROW_BYTES as u32,
                sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&built.directory)),
            },
            pages: TableGeometry {
                rows: transparent_shard::PAGE_ROWS as u64,
                row_bytes: transparent_shard::PAGE_ROW_BYTES as u32,
                sha256: hex::encode(<sha2::Sha256 as sha2::Digest>::digest(&built.pages)),
            },
            occupancy: ManifestOccupancy {
                scripts: built.scripts,
                page_rows: built.page_rows,
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
        std::fs::write(shard_dir.join("directory.bin"), &built.directory).unwrap();
        std::fs::write(shard_dir.join("pages.bin"), &built.pages).unwrap();

        entries.push(ShardMapEntry {
            shard_id,
            start_height: start,
            end_height: end,
            parent_block_hash: manifest.parent_block_hash.clone(),
            terminal_block_hash: manifest.terminal_block_hash.clone(),
            filter_hash: manifest.filter_hash.clone(),
            scripts: built.scripts,
            page_rows: built.page_rows,
            txids: 0,
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
        seal: SealParameters {
            max_scripts: 8_192,
            max_page_rows: 2_048,
            max_txids: 0,
        },
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

    fn setup(&mut self, shard_id: u64, table: Table) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self
            .client
            .get(format!(
                "{}/v1/shards/{shard_id}/setup/{}",
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

    fn query(&mut self, shard_id: u64, table: Table, body: &[u8]) -> Result<Vec<u8>, BoxError> {
        Ok(self
            .client
            .post(format!(
                "{}/v1/shards/{shard_id}/query/{}",
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
    let set = ShardSet::open(dir).expect("load");
    let state = ServiceState::build(set).expect("state");
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
        let init: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        let geometry = ServiceGeometry {
            directory_scheme: serde_json::from_value(init["directory_scheme"].clone()).unwrap(),
            directory_setup_seed: init["directory_setup_seed"].as_u64().unwrap(),
            pages_scheme: serde_json::from_value(init["pages_scheme"].clone()).unwrap(),
            pages_setup_seed: init["pages_setup_seed"].as_u64().unwrap(),
        };
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

#[tokio::test(flavor = "multi_thread")]
async fn a_wallet_syncs_from_its_birthday_and_matches_an_independent_traversal() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
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
    assert!(
        expected.utxos().count() > 100,
        "the long history is present"
    );

    assert_eq!(outcome.covered_through, FIRST + SHARDS * SPAN - 1);
    assert_eq!(
        outcome.charges.filters_checked, SHARDS,
        "every filter in range is downloaded, matched or not"
    );
    assert!(outcome.charges.queries > 0);
    assert!(outcome.charges.filter_bytes > 0);
    assert!(outcome.charges.setup_bytes > 0);

    eprintln!(
        "sync: shards {:?} filters {} B, setup {} B, queries {} ({} up, {} down), total {} B",
        outcome.matched_shards,
        outcome.charges.filter_bytes,
        outcome.charges.setup_bytes,
        outcome.charges.queries,
        outcome.charges.query_upload,
        outcome.charges.query_download,
        outcome.charges.total()
    );
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
        outcome.charges.queries, 0,
        "no private work without a match"
    );
    assert_eq!(outcome.charges.setup_bytes, 0, "no shard is even opened");
    assert_eq!(outcome.charges.filters_checked, SHARDS);
    assert_eq!(
        outcome.charges.total(),
        outcome.charges.public_floor(),
        "an unused wallet pays the floor and nothing else"
    );
}
