//! The POC end to end: a wallet syncs from a birthday against a real shard
//! server, over real HTTP, with real PIR, and its recovered ledger is compared
//! exactly against an independent traversal of the same events.
//!
//! This is the test the whole thing is for. Everything else checks a piece.
//!
//! Equality is exact — every UTXO and every spend — not just the balance. Two
//! errors can sum to the right number, and a balance check would pass a
//! reconstruction that had lost a receive and a spend of equal value.

#[path = "common/prepared.rs"]
mod prepared;
use prepared::prepared_shard;

#[path = "support/revision.rs"]
mod revision;

/// Page shards each directory shard holds: two, so every walk crosses a
/// page-shard boundary.
const CHILDREN: u32 = 2;

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
use transparent_filter::{BlockHash, ScriptBytes, SealParameters, ShardMap};
use transparent_shard::build::{BuiltChild, BuiltDirectory};
use transparent_shard::layout::{Geometry, DECOUPLED_2K_8K, DECOUPLED_32K_32K};
use transparent_shard::manifest::ShardManifest;
use transparent_shard_server::service::{router, ServiceConfig, ServiceState};
use transparent_shard_server::shardset::{ShardSet, DEFAULT_RETAIN_REVISIONS};
use transparent_wallet::client::Table;
use transparent_wallet::http::{HttpOptions, HttpShardTransport};
use transparent_wallet::ledger::Ledger;
use transparent_wallet::sync::{sync, GeometryParams, ServiceGeometry};
use transparent_wallet::transport::{
    refusal, BoxError, FilterSource, ShardReply, ShardRequest, ShardTransport,
};

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
            metadata: None,
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
            metadata: None,
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
                metadata: None,
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
                metadata: None,
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
                    metadata: None,
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
    publish_with(dir, per_shard, |_| &DECOUPLED_2K_8K, 0, "")
}

/// Writes a publishable shard set whose shards may name different geometries.
///
/// The two-tier set the deployment plan describes is this: archive geometry
/// below the recent cutoff, recent geometry from it. Nothing else about the
/// publication changes, which is the property worth testing — a wallet must
/// cross the boundary without being told it is there.
///
/// `tail_revision` and `tail_supersedes` apply to the unsealed last shard
/// alone, because it is the only one that can be republished: sealed content is
/// immutable, so a sealed shard that changed would be a different set.
fn publish_with(
    dir: &Path,
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    geometry_for: impl Fn(u64) -> &'static Geometry,
    tail_revision: u32,
    tail_supersedes: &str,
) -> ShardMap {
    publish_choosing(
        dir,
        per_shard,
        geometry_for,
        tail_revision,
        tail_supersedes,
        |_, _| None,
    )
}

/// As [`publish_with`], with `choice` deciding each shard's published
/// directory choice field from its id and build: `None` publishes none.
fn publish_choosing(
    dir: &Path,
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    geometry_for: impl Fn(u64) -> &'static Geometry,
    tail_revision: u32,
    tail_supersedes: &str,
    choice: impl Fn(u64, &BuiltDirectory) -> Option<String>,
) -> ShardMap {
    publish_profiled(
        dir,
        per_shard,
        geometry_for,
        tail_revision,
        tail_supersedes,
        choice,
        transparent_filter::RANGE_PROFILE,
    )
}

/// A published row whose content a dishonest publisher corrupts, chosen from
/// each shard as built: its table and its row in that table's logical space.
type BadRow<'a> = &'a dyn Fn(u64, &BuiltDirectory, &[BuiltChild]) -> Option<(Table, u64)>;

/// A published row a dishonest publisher replaces, chosen from each shard as
/// built: its table, its row in that table's logical space, and the bytes
/// published in its place.
type RewrittenRow<'a> =
    &'a dyn Fn(u64, &BuiltDirectory, &[BuiltChild]) -> Option<(Table, u64, Vec<u8>)>;

/// As [`publish_profiled`] under the default profile, with every byte of
/// the row `bad` names inverted before publication. The manifest describes
/// the corrupted tables, so the service serves them as it would any other:
/// what a publisher aiming bad content at chosen rows does.
fn publish_corrupting(
    dir: &Path,
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    bad: BadRow<'_>,
) -> ShardMap {
    publish_rewriting(dir, per_shard, &|shard_id, built, children| {
        let (table, row) = bad(shard_id, built, children)?;
        let mut bytes = match table {
            Table::Directory => built.directory_row(row).to_vec(),
            Table::Pages(child) => children[child as usize]
                .page_row(built.geometry, row)
                .to_vec(),
        };
        bytes.iter_mut().for_each(|byte| *byte ^= 0xa5);
        Some((table, row, bytes))
    })
}

/// As [`publish_corrupting`], with the row `bad` names replaced by the
/// bytes it gives: content that decodes, but says what the publisher chose.
fn publish_rewriting(
    dir: &Path,
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    bad: RewrittenRow<'_>,
) -> ShardMap {
    publish_with_bad_row(
        dir,
        per_shard,
        |_| &DECOUPLED_2K_8K,
        0,
        "",
        |_, _| None,
        transparent_filter::RANGE_PROFILE,
        bad,
    )
}

/// As [`publish_choosing`], under the named range-filter profile.
fn publish_profiled(
    dir: &Path,
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    geometry_for: impl Fn(u64) -> &'static Geometry,
    tail_revision: u32,
    tail_supersedes: &str,
    choice: impl Fn(u64, &BuiltDirectory) -> Option<String>,
    profile: &str,
) -> ShardMap {
    publish_with_bad_row(
        dir,
        per_shard,
        geometry_for,
        tail_revision,
        tail_supersedes,
        choice,
        profile,
        &|_, _, _| None,
    )
}

#[allow(clippy::too_many_arguments)]
fn publish_with_bad_row(
    dir: &Path,
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    geometry_for: impl Fn(u64) -> &'static Geometry,
    tail_revision: u32,
    tail_supersedes: &str,
    choice: impl Fn(u64, &BuiltDirectory) -> Option<String>,
    profile: &str,
    bad: RewrittenRow<'_>,
) -> ShardMap {
    let mut entries = Vec::new();
    let mut parent_digest = String::new();
    for (shard_id, events) in per_shard.iter().enumerate() {
        let shard_id = shard_id as u64;
        let geometry = geometry_for(shard_id);
        let start = FIRST + shard_id * SPAN;
        let end = start + SPAN - 1;
        let sealed = shard_id + 1 < SHARDS;
        let prepared = prepared_shard(
            shard_id,
            start,
            end,
            genesis(),
            &hash_at,
            profile,
            geometry,
            CHILDREN,
            0,
            sealed,
            events,
        );
        let built = &prepared.0;
        let mut children = prepared.1.clone();
        let mut directory = built.directory.clone();
        let bad_row = bad(shard_id, built, &children);
        match &bad_row {
            Some((Table::Directory, row, bytes)) => {
                let (segment, within) =
                    transparent_shard::layout::split_row(*row, geometry.directory_rows);
                let width = geometry.directory_row_bytes;
                let at = within as usize * width;
                directory[segment as usize][at..at + width].copy_from_slice(bytes);
            }
            Some((Table::Pages(child), row, bytes)) => {
                let width = geometry.page_row_bytes;
                let at = *row as usize * width;
                children[*child as usize].pages[at..at + width].copy_from_slice(bytes);
            }
            None => {}
        }
        let children = &children;
        let spec = revision::Revision {
            shard_id,
            start,
            end,
            geometry,
            sealed,
            revision: if sealed { 0 } else { tail_revision },
            supersedes: if sealed { "" } else { tail_supersedes },
            parent_digest: &parent_digest,
            children: CHILDREN,
            genesis: genesis(),
            hash: &hash_at,
        };
        let mut manifest = revision::manifest(&spec, built, children);
        manifest.profile = profile.to_string();
        manifest.directory_choice = choice(shard_id, built);
        for (described, segment) in manifest.directory_segments.iter_mut().zip(&directory) {
            described.sha256 = hex::encode(<sha2::Sha256 as sha2::Digest>::digest(segment));
        }
        let (_, entry) = revision::write_manifest(dir, &spec, manifest, built, children);
        if matches!(bad_row, Some((Table::Directory, ..))) {
            for (index, segment) in directory.iter().enumerate() {
                std::fs::write(
                    dir.join(&entry.manifest_digest)
                        .join(format!("directory.{index}.bin")),
                    segment,
                )
                .unwrap();
            }
        }
        let digest = entry.manifest_digest.clone();
        entries.push(entry);
        parent_digest = digest;
    }

    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: profile.to_string(),
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
        recuts: Vec::new(),
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
            map: serde_json::to_vec(&map).unwrap(),
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
            .get(format!("{}/v2/shards/init", self.base))
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
                "{}/v2/shards/{shard_id}/revisions/{revision}/manifest",
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
                "{}/v2/shards/{shard_id}/revisions/{revision}/setup/{}/{segment}",
                self.base,
                table.name()
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
                "{}/v2/shards/{shard_id}/revisions/{revision}/query/{}",
                self.base,
                table.name()
            ))
            .body(body.to_vec())
            .send()?;
        checked(response, shard_id, revision)
    }
}

/// Reads a reply, turning a refusal the wallet can act on into one.
///
/// The body and the `retry-after` header are read before the status is thrown
/// away: `error_for_status` keeps only the code, and the map digest a stale
/// refusal carries lives in the body.
fn checked(
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
                directory_scheme_dq44: serde_json::from_value(
                    entry["directory_scheme_dq44"].clone(),
                )
                .unwrap(),
                directory_setup_seed: entry["directory_setup_seed"].as_u64().unwrap(),
                page_rows: entry["page_rows"].as_u64().unwrap(),
                page_row_bytes: entry["page_row_bytes"].as_u64().unwrap() as u32,
                pages_scheme: serde_json::from_value(entry["pages_scheme"].clone()).unwrap(),
                pages_scheme_dq44: serde_json::from_value(entry["pages_scheme_dq44"].clone())
                    .unwrap(),
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
    run_sync_with_init(dir, base, wallet, birthday, map, |_| {}).await
}

/// [`run_sync`] with the init document edited before the wallet reads it.
async fn run_sync_with_init(
    dir: &Path,
    base: String,
    wallet: Vec<ScriptBytes>,
    birthday: u64,
    map: ShardMap,
    edit: fn(&mut serde_json::Value),
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
            .get(format!("{base}/v2/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap();
        let mut init: serde_json::Value = serde_json::from_slice(&raw).unwrap();
        edit(&mut init);
        let geometry = parse_init(&serde_json::to_vec(&init).unwrap());
        let mut filters = filters;
        sync(
            &map,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            birthday,
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
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
                    metadata: None,
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
        map.shards[0].page_rows, 1,
        "three short histories share one row in one page shard"
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
            .get(format!("{base}/v2/shards/init"))
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
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
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

/// A wallet sends the 8-bit profile's 28-bit queries, the only width the
/// history tables publish, and a 44-bit scheme a service advertises beside
/// one (here a copy of the published scheme) changes nothing it sends.
#[tokio::test(flavor = "multi_thread")]
async fn a_wallet_sends_the_published_28_bit_queries() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;
    let wallet = vec![script(1), script(2), script(3)];
    let expected = traverse(&per_shard, &wallet, 0);

    // The directory has 2,048 rows, and every page shard here holds under
    // 2,048 rows, so its queries select that 2,048-row prefix: one length.
    let rows = DECOUPLED_2K_8K.directory_rows as usize;
    let native = transparent_native::profile_for("directory");
    let upload = 8 + native.request_len_bits(rows, 28);
    let plain = run_sync(dir.path(), base.clone(), wallet.clone(), FIRST, map.clone()).await;
    let advertised = run_sync_with_init(dir.path(), base, wallet, FIRST, map, |init| {
        for entry in init["geometries"].as_array_mut().unwrap() {
            let entry = entry.as_object_mut().unwrap();
            assert!(entry.get("directory_scheme_dq44").is_none());
            let directory = entry["directory_scheme"].clone();
            let pages = entry["pages_scheme"].clone();
            entry.insert("directory_scheme_dq44".into(), directory);
            entry.insert("pages_scheme_dq44".into(), pages);
        }
    })
    .await;
    for outcome in [&plain, &advertised] {
        compare(&outcome.ledger, &expected);
        assert!(outcome.charges.queries() > 0);
        assert_eq!(
            outcome.charges.query_upload(),
            outcome.charges.queries() * upload as u64,
            "every query is {upload} bytes"
        );
    }
    assert_eq!(plain.charges.queries(), advertised.charges.queries());
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

/// Local outpoints must survive actual PIR, including a shared tail row and
/// an inline pair. Sharing that row must not suppress the other script's query.
#[tokio::test(flavor = "multi_thread")]
async fn compact_local_outpoints_and_shared_tails_recover_over_native_pir() {
    let mut events = Vec::new();
    for i in 0..50u64 {
        let received = txid(10_000 + i);
        events.push((
            script(1),
            TransparentEvent::Receive(ReceiveEvent {
                metadata: None,
                height: (FIRST + i) as u32,
                transaction_index: 0,
                txid: received,
                output_index: 3,
                value: 123,
                coinbase: false,
            }),
        ));
        events.push((
            script(1),
            TransparentEvent::Spend(SpendEvent {
                metadata: None,
                height: (FIRST + i) as u32,
                transaction_index: 1,
                spending_txid: txid(20_000 + i),
                input_index: 2,
                spent_txid: received,
                spent_output_index: 3,
            }),
        ));
    }
    for i in 0..3u64 {
        events.push((
            script(2),
            TransparentEvent::Receive(ReceiveEvent {
                metadata: None,
                height: (FIRST + i) as u32,
                transaction_index: 2,
                txid: txid(30_000 + i),
                output_index: 0,
                value: 7,
                coinbase: false,
            }),
        ));
    }
    let per_shard = vec![events];
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &per_shard);
    assert_eq!(
        map.shards[0].page_rows, 2,
        "short history shares the long tail"
    );
    let wallet = vec![script(1), script(2)];
    let base = serve(dir.path()).await;
    let outcome = run_sync(dir.path(), base, wallet.clone(), FIRST, map).await;
    compare(&outcome.ledger, &traverse(&per_shard, &wallet, 0));
    assert_eq!(outcome.ledger.spends().len(), 50);
    assert_eq!(outcome.ledger.confirmed_balance(), 21);
    assert!(outcome.ledger.unresolved().is_empty());
    assert_eq!(
        outcome.charges.pages.queries, 3,
        "each script fetches its own pages"
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
                    metadata: None,
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
            .get(format!("{base}/v2/shards/init"))
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
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
        )
        .err()
        .expect("a short answer must not be accepted")
    })
    .await
    .unwrap();

    // A short answer is refused as a request: the service sends one without
    // knowing the row, so it fails the read rather than an entry.
    assert!(
        error.to_string().contains("length"),
        "expected a length refusal, got: {error}"
    );
}

/// Alters one byte of every manifest it relays: what a corrupted or
/// substituted publication looks like from the wallet's side.
struct AltersManifests {
    inner: HttpShards,
}

impl ShardTransport for AltersManifests {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.init()
    }

    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        let (mut bytes, cost) = self.inner.manifest(shard_id, revision)?;
        // Flip a digit inside the terminal block hash: still valid JSON, still
        // a manifest, no longer the one the map named.
        let key = b"\"terminal_block_hash\":\"";
        let at = bytes
            .windows(key.len())
            .position(|window| window == key)
            .expect("the manifest carries a terminal hash")
            + key.len();
        bytes[at] = if bytes[at] == b'0' { b'1' } else { b'0' };
        Ok((bytes, cost))
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
        self.inner.query(shard_id, revision, table, body)
    }
}

/// A manifest that does not digest to what the map names stops the sync
/// before a single private request, and coverage stays where it was.
#[tokio::test(flavor = "multi_thread")]
async fn a_wallet_refuses_a_manifest_that_does_not_digest_to_the_map() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;

    let filters = PublishedFilters::load(dir.path(), &map);
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    let wallet = vec![script(1), script(2), script(3)];
    let (error, queries) = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let raw = client
            .get(format!("{base}/v2/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap();
        let geometry = parse_init(&raw);
        let mut transport = AltersManifests {
            inner: HttpShards {
                base: base.clone(),
                client: client.clone(),
            },
        };
        let mut filters = filters;
        let error = sync(
            &map,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            FIRST,
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
        )
        .err()
        .expect("an altered manifest must not be accepted");
        // Nothing private was asked of the service.
        let metrics = client
            .get(format!("{base}/metrics"))
            .send()
            .unwrap()
            .text()
            .unwrap();
        // Series carry labels, so match the name and read the last field.
        let queries: u64 = metrics
            .lines()
            .find(|line| line.starts_with("transparent_shard_queries_total"))
            .and_then(|line| line.rsplit(' ').next())
            .and_then(|value| value.parse().ok())
            .unwrap();
        (error, queries)
    })
    .await
    .unwrap();

    assert!(
        matches!(
            error,
            transparent_wallet::SyncError::ManifestMismatch {
                field: "digest",
                ..
            }
        ),
        "expected a digest mismatch, got: {error}"
    );
    assert_eq!(
        queries, 0,
        "no private query is made for an unverified shard"
    );
}

/// Every matched shard's manifest is fetched and verified exactly once, and
/// the manifests a sync reads agree with the map field by field: the genuine
/// service passes the same checks the altered one fails.
#[tokio::test(flavor = "multi_thread")]
async fn a_genuine_manifest_is_verified_once_per_matched_shard() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;
    let wallet = vec![script(1), script(2), script(3)];
    let outcome = run_sync(dir.path(), base, wallet.clone(), FIRST, map).await;
    assert_eq!(
        outcome.charges.manifests_checked,
        outcome.matched_shards.len() as u64
    );
    assert!(outcome.charges.manifest_bytes > 0);
    compare(&outcome.ledger, &traverse(&per_shard, &wallet, 0));
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

    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.manifest(shard_id, revision)
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
        if matches!(table, Table::Pages(_)) {
            answer.truncate(answer.len() / 2);
        }
        Ok(answer)
    }
}

/// A birthday inside the set skips earlier shards entirely, and the result must
/// match a traversal that skips the same events — including the unresolved
/// spend of an output received before the birthday.
#[tokio::test(flavor = "multi_thread")]
async fn a_birthday_that_omits_a_receive_cannot_report_complete() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;

    let wallet = vec![script(1), script(2)];
    let birthday = map.shards[2].start_height;
    let filters = PublishedFilters::load(dir.path(), &map);
    let outcome = tokio::task::spawn_blocking(move || {
        use transparent_wallet::{
            Anchor, MemoryStore, ScriptEntry, ScriptOrigin, StaticChain, StaticScripts,
            WalletStore, WorkLimits,
        };
        let mut store = MemoryStore::new();
        let mut transport =
            transparent_wallet::http::HttpShardTransport::new(base, &Default::default()).unwrap();
        let geometry = transport.geometry().unwrap();
        let mut filters = filters;
        let mut provider = StaticScripts(
            [script(1), script(2)]
                .iter()
                .map(|s| ScriptEntry {
                    script: s.as_slice().to_vec(),
                    origin: ScriptOrigin::Derived,
                    required_from: birthday,
                })
                .collect(),
        );
        let tip = map.shards.last().unwrap();
        let result = transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &StaticChain::from_map(&map),
            &mut provider,
            &mut filters,
            &mut transport,
            &WorkLimits::UNLIMITED,
            &Anchor {
                height: tip.end_height,
                hash: tip.terminal_block_hash.clone(),
            },
        )
        .unwrap();
        assert!(matches!(
            result.completion,
            transparent_wallet::Completion::Incomplete {
                reason: transparent_wallet::IncompleteReason::UnresolvedSpends,
                ..
            }
        ));
        assert!(store.anchor().unwrap().is_none());
        result
    })
    .await
    .unwrap();

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
/// `decoupled-2k-8k` and `decoupled-2k-8k` rather than a true archive geometry, because the
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
    let map = publish_with(
        dir.path(),
        &per_shard,
        |shard_id| {
            if shard_id < SHARDS / 2 {
                &DECOUPLED_32K_32K
            } else {
                &DECOUPLED_2K_8K
            }
        },
        0,
        "",
    );
    assert_eq!(map.shards[0].geometry, DECOUPLED_32K_32K.name);
    assert_eq!(
        map.shards[(SHARDS - 1) as usize].geometry,
        DECOUPLED_2K_8K.name,
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
    publish_with(
        dir.path(),
        &per_shard,
        |shard_id| {
            if shard_id < SHARDS / 2 {
                &DECOUPLED_32K_32K
            } else {
                &DECOUPLED_2K_8K
            }
        },
        0,
        "",
    );
    let base = serve(dir.path()).await;

    let raw = tokio::task::spawn_blocking(move || {
        reqwest::blocking::get(format!("{base}/v2/shards/init"))
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
    assert_eq!(names, vec![DECOUPLED_2K_8K.name, DECOUPLED_32K_32K.name]);

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
            .get(format!("{base}/v2/shards/init"))
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
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
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

/// Serves set A's public bytes until the map is refetched, then set B's.
///
/// Not fault injection: this is a wallet holding a cached map whose publisher
/// has moved on, which is the ordinary condition a tail republication creates.
/// The refusal that drives the recovery comes from the real service, against a
/// real published set.
struct TwoSetFilters {
    before: PublishedFilters,
    after: PublishedFilters,
    refreshed: bool,
}

impl FilterSource for TwoSetFilters {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        self.refreshed = true;
        self.after.shard_map()
    }

    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        if self.refreshed {
            self.after.filter(shard_id)
        } else {
            self.before.filter(shard_id)
        }
    }
}

/// A wallet whose cached map names a tail revision the service has replaced
/// refreshes the map and re-derives that shard, rather than failing the sync.
///
/// The two published sets differ only in the unsealed tail: set B holds a later
/// revision of it, carrying one more event for a script the wallet owns, and
/// does not hold set A's revision at all. So the wallet's first tail request is
/// refused with a real 409 from real service code.
///
/// What makes the assertions worth making is the shape of the chain: script 1
/// is received in shard 0 and spent in shard 2, and script 2 gains an event in
/// the replaced tail. A recovery that dropped what it had already read would
/// leave that spend unresolved, and one that kept the abandoned attempt's
/// results would miss the new event. Only re-deriving the tail over retained
/// earlier history reproduces the traversal exactly.
#[tokio::test(flavor = "multi_thread")]
async fn a_wallet_holding_a_replaced_tail_revision_recovers_by_refreshing_the_map() {
    replaced_tail_recovers(false, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filter_replaced_after_the_map_was_read_refreshes_before_matching() {
    replaced_tail_recovers(true, 1).await;
}

/// The same recoveries with requests sent concurrently across shards: the
/// withdrawn tail's refusal or mismatched filter reaches the walk in its place,
/// and nothing read ahead survives the refresh.
#[tokio::test(flavor = "multi_thread")]
async fn a_replaced_tail_recovers_with_concurrent_requests() {
    replaced_tail_recovers(false, 4).await;
    replaced_tail_recovers(true, 4).await;
}

async fn replaced_tail_recovers(filter_race: bool, concurrency: usize) {
    let before_dir = tempfile::tempdir().unwrap();
    let after_dir = tempfile::tempdir().unwrap();

    let mut before_chain = chain();
    let map_before = publish(before_dir.path(), &before_chain);

    // The replacement: the same range, one more event, published as revision 1
    // superseding the revision the wallet cached.
    let tail = (SHARDS - 1) as usize;
    let height = FIRST + (SHARDS - 1) * SPAN + 42;
    before_chain[tail].push((
        script(if filter_race { 999 } else { 2 }),
        TransparentEvent::Receive(ReceiveEvent {
            metadata: None,
            height: height as u32,
            txid: txid(9_999),
            transaction_index: 4,
            output_index: 0,
            value: 4_242,
            coinbase: false,
        }),
    ));
    let after_chain = before_chain;
    let stale_digest = map_before.shards[tail].manifest_digest.clone();
    let map_after = publish_with(
        after_dir.path(),
        &after_chain,
        |_| &DECOUPLED_2K_8K,
        1,
        &stale_digest,
    );
    assert_ne!(
        map_after.shards[tail].manifest_digest, stale_digest,
        "the replacement must be a different revision, or there is nothing to recover from"
    );

    // Only the newer set is served, so the cached revision is genuinely gone
    // rather than merely superseded and still answerable.
    let base = serve(after_dir.path()).await;

    if filter_race {
        assert_ne!(
            map_before.shards[tail].filter_hash,
            map_after.shards[tail].filter_hash
        );
    }
    let wallet = vec![script(1), script(2), script(3), script(999)];
    let expected_wallet = wallet.clone();
    let filters = TwoSetFilters {
        before: PublishedFilters::load(before_dir.path(), &map_before),
        after: PublishedFilters::load(after_dir.path(), &map_after),
        refreshed: filter_race,
    };
    let first_filters = if filter_race {
        &filters.after
    } else {
        &filters.before
    };
    let expected_filter_bytes = first_filters
        .filters
        .values()
        .map(|bytes| bytes.len() as u64)
        .sum::<u64>()
        + filters.after.filters[&(SHARDS - 1)].len() as u64;
    let map_bytes = serde_json::to_vec(&map_before).unwrap().len() as u64;
    let outcome = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = shards_at(&base, &client, concurrency);
        let raw = client
            .get(format!("{base}/v2/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap()
            .to_vec();
        let geometry = parse_init(&raw);
        let mut filters = filters;
        sync(
            &map_before,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            FIRST,
            &transparent_wallet::StaticChain::from_map(&map_before),
            &transparent_wallet::Anchor {
                height: map_before.shards.last().unwrap().end_height,
                hash: map_before
                    .shards
                    .last()
                    .unwrap()
                    .terminal_block_hash
                    .clone(),
            },
        )
        .expect("the sync must recover, not fail")
    })
    .await
    .unwrap();

    let expected = traverse(&after_chain, &expected_wallet, 0);
    compare(&outcome.ledger, &expected);

    assert_eq!(
        outcome.map_refreshes, 1,
        "exactly one refresh should resolve a single replaced revision"
    );
    assert!(
        outcome.ledger.unresolved().is_empty(),
        "history read before the refusal must survive the recovery"
    );
    assert_eq!(
        outcome.ledger.spends().len(),
        1,
        "the spend in shard 2 of an output received in shard 0 must still resolve"
    );
    assert_eq!(
        outcome.covered_through, map_after.shards[tail].end_height,
        "coverage must reach the end of the revision that replaced the refused one"
    );
    assert_eq!(
        outcome.provisional.len(),
        1,
        "only the unsealed tail is provisional"
    );
    assert_eq!(
        outcome.provisional[0].manifest_digest, map_after.shards[tail].manifest_digest,
        "the provisional record must name the revision actually read, never the refused one"
    );
    assert_eq!(outcome.provisional[0].revision, 1);
    // Both the rejected attempt and the verified replacement cost bytes;
    // mismatched bytes must never become a checked or persisted filter.
    assert_eq!(outcome.charges.filter_bytes, expected_filter_bytes);
    assert_eq!(
        outcome.charges.filters_checked,
        SHARDS + u64::from(!filter_race),
        "a mismatched filter is charged in bytes but never counted as checked"
    );
    assert!(
        outcome.charges.map_bytes > map_bytes,
        "the refreshed map is charged too"
    );
}

/// Refuses a fixed number of queries as an overloaded service would, then
/// forwards.
struct OverloadedFor {
    inner: HttpShards,
    remaining: u32,
    retry_after: Option<String>,
    /// Reported to the sync; above 1 it looks ahead and hands this transport
    /// batches, which the default sends one at a time through `query`, so
    /// batched queries are refused like any other.
    concurrency: usize,
}

impl ShardTransport for OverloadedFor {
    fn concurrency(&self) -> usize {
        self.concurrency
    }

    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.init()
    }

    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.manifest(shard_id, revision)
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
        if self.remaining > 0 {
            self.remaining -= 1;
            // Classified the way the HTTP transport classifies it, from the
            // status and the `retry-after` the service would have sent — so a
            // 503 that names no delay falls through to an ordinary failure
            // here exactly as it would against the real service.
            let body = br#"{"error":"no cache capacity is free","retry":"retry shortly"}"#;
            return Err(
                refusal(503, self.retry_after.as_deref(), body, shard_id, revision)
                    .unwrap_or_else(|| "HTTP 503".into()),
            );
        }
        self.inner.query(shard_id, revision, table, body)
    }
}

/// A service that is briefly out of cache capacity is waited for, not given up
/// on — and the wallet still reconstructs exactly.
///
/// The refusal says nothing about the map, so this must be recovered without
/// refetching one: an overload that spent a map refresh would burn the budget
/// that exists for a different problem.
#[tokio::test(flavor = "multi_thread")]
async fn an_overloaded_service_is_retried_and_the_sync_still_reconstructs_exactly() {
    overloaded_then_recovers(1).await;
}

/// The same, with the refusals landing on batched queries: a refused batched
/// query is made again by the walk, which backs off and retries as before.
#[tokio::test(flavor = "multi_thread")]
async fn an_overload_during_concurrent_requests_is_retried_and_reconstructs_exactly() {
    overloaded_then_recovers(4).await;
}

async fn overloaded_then_recovers(concurrency: usize) {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;

    let wallet = vec![script(1), script(2)];
    let expected_wallet = wallet.clone();
    let filters = PublishedFilters::load(dir.path(), &map);
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    let outcome = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = OverloadedFor {
            inner: HttpShards {
                base: base.clone(),
                client: client.clone(),
            },
            remaining: 2,
            retry_after: Some("0".into()),
            concurrency,
        };
        let raw = client
            .get(format!("{base}/v2/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap()
            .to_vec();
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
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
        )
        .expect("an overload is a wait, not a failure")
    })
    .await
    .unwrap();

    compare(&outcome.ledger, &traverse(&per_shard, &expected_wallet, 0));
    assert_eq!(
        outcome.map_refreshes, 0,
        "an overload says nothing about the map and must not spend a refresh"
    );
}

/// A service that stays at its limit is reported as such, and coverage stops
/// where it was rather than skipping the range it could not read.
#[tokio::test(flavor = "multi_thread")]
async fn an_unrelenting_overload_stops_the_sync_without_advancing_coverage() {
    unrelenting_overload_stops(1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unrelenting_overload_stops_a_concurrent_sync_too() {
    unrelenting_overload_stops(4).await;
}

async fn unrelenting_overload_stops(concurrency: usize) {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;

    let wallet = vec![script(1), script(2)];
    let filters = PublishedFilters::load(dir.path(), &map);
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    let error = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = OverloadedFor {
            inner: HttpShards {
                base: base.clone(),
                client: client.clone(),
            },
            remaining: u32::MAX,
            retry_after: Some("0".into()),
            concurrency,
        };
        let raw = client
            .get(format!("{base}/v2/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap()
            .to_vec();
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
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
        )
        .err()
        .expect("an unrelenting overload must not be reported as a completed sync")
    })
    .await
    .unwrap();

    assert!(
        matches!(error, transparent_wallet::SyncError::Overloaded { .. }),
        "expected a named overload, got: {error}"
    );
}

/// A 503 that names no delay is not the capacity refusal.
///
/// The service answers `/v1/ready` and an empty set with 503 too, and neither
/// is fixed by waiting. Retrying those would spin against a condition that no
/// backoff resolves, so only the flavour carrying `retry-after` is retryable.
#[tokio::test(flavor = "multi_thread")]
async fn a_503_that_names_no_delay_is_not_retried() {
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;

    let wallet = vec![script(1), script(2)];
    let filters = PublishedFilters::load(dir.path(), &map);
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    let error = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = OverloadedFor {
            inner: HttpShards {
                base: base.clone(),
                client: client.clone(),
            },
            remaining: 1,
            retry_after: None,
            concurrency: 1,
        };
        let raw = client
            .get(format!("{base}/v2/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap()
            .to_vec();
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
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
        )
        .err()
        .expect("an unrecognised 503 must not be silently retried into success")
    })
    .await
    .unwrap();

    assert!(
        !matches!(error, transparent_wallet::SyncError::Overloaded { .. }),
        "a 503 without a delay must not be treated as the capacity refusal"
    );
}

/// When a refresh cannot produce a live revision, the sync stops and says so.
///
/// Here the public map source is itself stale, so refetching returns the same
/// map that named the revision the service refused. The two disagree with each
/// other, and asking again would loop, so this must be reported at once rather
/// than spending the refresh budget discovering the same thing four times.
///
/// The outcome is a named refusal, not a completed sync: a wallet that reported
/// coverage over the range it could not read would be claiming a balance it had
/// not earned.
#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_that_cannot_help_stops_the_sync_rather_than_looping() {
    unhelpful_refresh_stops(false, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_filter_with_an_unchanged_map_is_never_accepted() {
    unhelpful_refresh_stops(true, 1).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_refresh_that_cannot_help_stops_a_concurrent_sync_too() {
    unhelpful_refresh_stops(false, 4).await;
    unhelpful_refresh_stops(true, 4).await;
}

/// The shard service at `concurrency`: the test's own sequential adapter at
/// 1, the reference HTTP transport otherwise.
fn shards_at(
    base: &str,
    client: &reqwest::blocking::Client,
    concurrency: usize,
) -> Box<dyn ShardTransport> {
    if concurrency == 1 {
        Box::new(HttpShards {
            base: base.to_string(),
            client: client.clone(),
        })
    } else {
        Box::new(
            HttpShardTransport::new(base, &HttpOptions::default())
                .unwrap()
                .with_concurrency(concurrency),
        )
    }
}

async fn unhelpful_refresh_stops(corrupt_filter: bool, concurrency: usize) {
    let before_dir = tempfile::tempdir().unwrap();
    let after_dir = tempfile::tempdir().unwrap();

    let mut before_chain = chain();
    let map_before = publish(before_dir.path(), &before_chain);

    let tail = (SHARDS - 1) as usize;
    let height = FIRST + (SHARDS - 1) * SPAN + 42;
    before_chain[tail].push((
        script(2),
        TransparentEvent::Receive(ReceiveEvent {
            metadata: None,
            height: height as u32,
            txid: txid(9_998),
            transaction_index: 4,
            output_index: 0,
            value: 11,
            coinbase: false,
        }),
    ));
    let stale_digest = map_before.shards[tail].manifest_digest.clone();
    publish_with(
        after_dir.path(),
        &before_chain,
        |_| &DECOUPLED_2K_8K,
        1,
        &stale_digest,
    );

    // The newer set is served, but the filter source still publishes the older
    // map — a publisher the wallet reads that has not caught up.
    let base = serve(after_dir.path()).await;

    let wallet = vec![script(1), script(2)];
    let mut filters = PublishedFilters::load(before_dir.path(), &map_before);
    if corrupt_filter {
        filters.filters.get_mut(&(SHARDS - 1)).unwrap().push(0);
    }
    let map_bytes = serde_json::to_vec(&map_before).unwrap().len() as u64;
    let error = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = shards_at(&base, &client, concurrency);
        let raw = client
            .get(format!("{base}/v2/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap()
            .to_vec();
        let geometry = parse_init(&raw);
        let mut filters = filters;
        sync(
            &map_before,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            FIRST,
            &transparent_wallet::StaticChain::from_map(&map_before),
            &transparent_wallet::Anchor {
                height: map_before.shards.last().unwrap().end_height,
                hash: map_before
                    .shards
                    .last()
                    .unwrap()
                    .terminal_block_hash
                    .clone(),
            },
        )
        .err()
        .expect("a withdrawn revision no refresh can replace must not read as success")
    })
    .await
    .unwrap();

    match error {
        transparent_wallet::SyncError::StaleRevision {
            shard_id,
            refreshes,
            ..
        } => {
            assert_eq!(
                shard_id,
                SHARDS - 1,
                "the tail is the shard that was refused"
            );
            assert_eq!(
                refreshes, 1,
                "one refresh is enough to learn the map cannot help; the budget must not spin"
            );
        }
        other => panic!("expected a named stale revision, got: {other}"),
    }
}

/// Publishes the choice table the builder made for every shard.
fn tabled(_: u64, built: &BuiltDirectory) -> Option<String> {
    built
        .choice
        .as_ref()
        .map(transparent_shard::manifest::encode_directory_choice)
}

/// Wallet scripts with activity in `shard`, which is what its filter matches.
fn active_in(
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    shard: usize,
    wallet: &[ScriptBytes],
) -> u64 {
    wallet
        .iter()
        .filter(|script| per_shard[shard].iter().any(|(s, _)| s == *script))
        .count() as u64
}

/// A published choice table costs one directory query per matched script
/// instead of two, and changes nothing else: the same directory rows, the
/// same page queries, and exactly the same ledger.
#[tokio::test(flavor = "multi_thread")]
async fn a_choice_table_sends_one_directory_query_per_matched_script() {
    let per_shard = chain();
    let wallet = vec![script(1), script(2), script(3), script(999)];

    let two = tempfile::tempdir().unwrap();
    let two_map = publish(two.path(), &per_shard);
    let one = tempfile::tempdir().unwrap();
    let one_map = publish_choosing(one.path(), &per_shard, |_| &DECOUPLED_2K_8K, 0, "", tabled);
    for (a, b) in two_map.shards.iter().zip(&one_map.shards) {
        assert_ne!(a.manifest_digest, b.manifest_digest);
        assert_eq!(
            std::fs::read(two.path().join(&a.manifest_digest).join("directory.0.bin")).unwrap(),
            std::fs::read(one.path().join(&b.manifest_digest).join("directory.0.bin")).unwrap(),
            "the table changes the manifest, never the rows"
        );
    }

    let two_base = serve(two.path()).await;
    let one_base = serve(one.path()).await;
    let without = run_sync(two.path(), two_base, wallet.clone(), FIRST, two_map).await;
    let with = run_sync(one.path(), one_base, wallet.clone(), FIRST, one_map).await;

    compare(&with.ledger, &traverse(&per_shard, &wallet, 0));
    compare(&with.ledger, &without.ledger);
    assert_eq!(with.covered_through, without.covered_through);
    assert_eq!(with.settled_through, without.settled_through);

    let pairs: u64 = (0..per_shard.len())
        .map(|shard| active_in(&per_shard, shard, &wallet))
        .sum();
    assert!(
        pairs > 1,
        "the fixture must match several script-shard pairs"
    );
    assert_eq!(without.charges.directory.queries, 2 * pairs);
    assert_eq!(with.charges.directory.queries, pairs);
    assert_eq!(with.charges.pages.queries, without.charges.pages.queries);
    assert!(with.charges.query_upload() < without.charges.query_upload());
    // The table travels in the manifest, which is where its bytes are charged.
    assert!(with.charges.manifest_bytes > without.charges.manifest_bytes);
}

/// With tables on sealed shards only, the tail keeps both candidate queries.
/// The count per matched script stays fixed per shard either way.
#[tokio::test(flavor = "multi_thread")]
async fn a_tail_without_a_table_keeps_two_directory_queries() {
    let per_shard = chain();
    let wallet = vec![script(1), script(2), script(3), script(999)];
    let dir = tempfile::tempdir().unwrap();
    let map = publish_choosing(
        dir.path(),
        &per_shard,
        |_| &DECOUPLED_2K_8K,
        0,
        "",
        |id, built| (id + 1 < SHARDS).then(|| tabled(id, built)).flatten(),
    );
    let base = serve(dir.path()).await;
    let outcome = run_sync(dir.path(), base, wallet.clone(), FIRST, map).await;
    compare(&outcome.ledger, &traverse(&per_shard, &wallet, 0));

    let tail = (SHARDS - 1) as usize;
    let sealed: u64 = (0..tail)
        .map(|shard| active_in(&per_shard, shard, &wallet))
        .sum();
    let in_tail = active_in(&per_shard, tail, &wallet);
    assert!(in_tail > 0, "the fixture must match the tail");
    assert_eq!(outcome.charges.directory.queries, sealed + 2 * in_tail);
}

/// One query per script, whichever scripts they are, including scripts that
/// share a directory row.
#[tokio::test(flavor = "multi_thread")]
async fn a_choice_table_costs_one_query_per_script_whatever_the_script() {
    let dir = tempfile::tempdir().unwrap();
    let mut events = Vec::new();
    for tag in 0..3u32 {
        for i in 0..3u32 {
            events.push((
                script(tag),
                TransparentEvent::Receive(ReceiveEvent {
                    metadata: None,
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
    let map = publish_choosing(dir.path(), &per_shard, |_| &DECOUPLED_2K_8K, 0, "", tabled);
    let base = serve(dir.path()).await;
    for scripts in 1..=3usize {
        let wallet: Vec<ScriptBytes> = (0..scripts as u32).map(script).collect();
        let outcome = run_sync(dir.path(), base.clone(), wallet.clone(), FIRST, map.clone()).await;
        compare(&outcome.ledger, &traverse(&per_shard, &wallet, 0));
        assert_eq!(outcome.charges.directory.queries, scripts as u64);
        assert_eq!(outcome.charges.pages.queries, scripts as u64);
    }
}

/// A choice table that does not decode, or that indexes a different number
/// of scripts than the directory holds, is refused. Rows store tags, so a
/// flipped bit is not recoverable from the row; the builder checks routes
/// while it still has the raw scripts.
#[test]
fn a_server_refuses_a_choice_table_that_does_not_match() {
    use transparent_shard::manifest::encode_directory_choice;
    use transparent_shard::ChoiceTable;

    let short = |id: u64, _: &BuiltDirectory| {
        Some(encode_directory_choice(
            &ChoiceTable::build(id, &[(&[0x51][..], 0)]).unwrap(),
        ))
    };
    let garbage = |_: u64, _: &BuiltDirectory| Some("not base64!".to_string());

    let per_shard = chain();
    for (name, choose) in [
        (
            "short",
            &short as &dyn Fn(u64, &BuiltDirectory) -> Option<String>,
        ),
        ("garbage", &garbage),
    ] {
        let dir = tempfile::tempdir().unwrap();
        publish_choosing(dir.path(), &per_shard, |_| &DECOUPLED_2K_8K, 0, "", choose);
        let error = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS)
            .err()
            .unwrap_or_else(|| panic!("a {name} table was loaded"));
        assert!(error.to_string().contains("choice"), "{name}: {error}");
    }

    let dir = tempfile::tempdir().unwrap();
    publish_choosing(dir.path(), &per_shard, |_| &DECOUPLED_2K_8K, 0, "", tabled);
    ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).expect("a genuine table loads");
}

/// What a sync asked the shard service for, as the service would see it.
#[derive(Clone, Debug, Default)]
struct Log {
    /// Stage and route of every request that reached the service.
    requests: Vec<(&'static str, String)>,
    /// Batches handed to the transport.
    batches: u64,
    /// Sequential waits: one per request made alone, and one per wave of a
    /// batch, a wave being as many requests as the transport keeps in flight.
    rounds: u64,
    /// Route and body length of every request that reached the service, in
    /// order: everything about a request the service sees but its content.
    sent: Vec<(String, usize)>,
}

fn body_len(request: &ShardRequest<'_>) -> usize {
    match request {
        ShardRequest::Query { body, .. } => body.len(),
        _ => 0,
    }
}

/// What [`Recording`] does with a batch.
#[derive(Clone, Copy, Debug)]
enum Batches {
    /// Sends it.
    Pass,
    /// Sends none of it.
    Unsent,
    /// Refuses its first request as overloaded, without sending it, and sends
    /// none of the rest.
    RefuseFirst,
}

/// Records every request that reaches the service, alone or in a batch, and
/// optionally stands in for a batch that sends nothing.
struct Recording<T> {
    inner: T,
    log: Arc<Mutex<Log>>,
    batches: Batches,
}

impl<T: ShardTransport> Recording<T> {
    fn single(&self, request: ShardRequest<'_>) {
        let mut log = self.log.lock().unwrap();
        log.requests.push((request.stage(), request.route()));
        log.sent.push((request.route(), body_len(&request)));
        log.rounds += 1;
    }
}

impl<T: ShardTransport> ShardTransport for Recording<T> {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.init()
    }
    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        self.single(ShardRequest::Manifest { shard_id, revision });
        self.inner.manifest(shard_id, revision)
    }
    fn setup(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError> {
        self.single(ShardRequest::Setup {
            shard_id,
            revision,
            table,
            segment,
        });
        self.inner.setup(shard_id, revision, table, segment)
    }
    fn query(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        body: &[u8],
    ) -> Result<Vec<u8>, BoxError> {
        self.single(ShardRequest::Query {
            shard_id,
            revision,
            table,
            body,
        });
        self.inner.query(shard_id, revision, table, body)
    }
    fn concurrency(&self) -> usize {
        self.inner.concurrency()
    }
    fn batch(&mut self, requests: &[ShardRequest<'_>]) -> Vec<Option<ShardReply>> {
        self.log.lock().unwrap().batches += 1;
        match self.batches {
            Batches::Pass => {}
            // Nothing reaches the service.
            Batches::Unsent => return (0..requests.len()).map(|_| None).collect(),
            // Nothing reaches the service: the first request is refused as an
            // overloaded service would refuse it, and the rest are never sent.
            Batches::RefuseFirst => {
                let body = br#"{"error":"no cache capacity is free","retry":"retry shortly"}"#;
                return requests
                    .iter()
                    .enumerate()
                    .map(|(i, request)| {
                        let (shard_id, revision) = match *request {
                            ShardRequest::Manifest { shard_id, revision }
                            | ShardRequest::Setup {
                                shard_id, revision, ..
                            }
                            | ShardRequest::Query {
                                shard_id, revision, ..
                            } => (shard_id, revision),
                        };
                        (i == 0).then(|| {
                            Err(refusal(503, Some("0"), body, shard_id, revision).unwrap())
                        })
                    })
                    .collect();
            }
        }
        let replies = self.inner.batch(requests);
        let mut log = self.log.lock().unwrap();
        let sent = replies.iter().filter(|reply| reply.is_some()).count() as u64;
        log.rounds += sent.div_ceil(self.inner.concurrency() as u64);
        for (request, reply) in requests.iter().zip(&replies) {
            if reply.is_some() {
                log.requests.push((request.stage(), request.route()));
                log.sent.push((request.route(), body_len(request)));
            }
        }
        replies
    }
}

/// One sync through the reference HTTP transport at `concurrency`, recorded.
async fn run_recorded(
    dir: &Path,
    base: String,
    wallet: Vec<ScriptBytes>,
    map: ShardMap,
    concurrency: usize,
    batches: Batches,
) -> (transparent_wallet::SyncOutcome, Log) {
    let filters = PublishedFilters::load(dir, &map);
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    let log = Arc::new(Mutex::new(Log::default()));
    let recorded = log.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        let mut transport = Recording {
            inner: HttpShardTransport::new(&base, &HttpOptions::default())
                .unwrap()
                .with_concurrency(concurrency),
            log: recorded,
            batches,
        };
        let geometry = transport.inner.geometry().unwrap();
        let mut filters = filters;
        sync(
            &map,
            map_bytes,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            FIRST,
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
        )
        .expect("sync")
    })
    .await
    .unwrap();
    let log = log.lock().unwrap().clone();
    (outcome, log)
}

fn sorted(requests: &[(&'static str, String)]) -> Vec<(&'static str, String)> {
    let mut requests = requests.to_vec();
    requests.sort();
    requests
}

/// Requests sent concurrently across matched shards are the sequential
/// walk's requests, sent sooner: the same ledger, the same charges, and the
/// same requests per stage and per shard revision, over publications with and
/// without choice tables, across a geometry boundary, and for wallets matching
/// one shard or several.
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_requests_change_nothing_but_when_they_are_sent() {
    let per_shard = chain();
    let wallets: Vec<Vec<ScriptBytes>> = vec![
        vec![script(1), script(2), script(3), script(999)],
        vec![script(1)],
        vec![script(2), script(3)],
        vec![script(1), script(2), script(100), script(101)],
    ];
    let tabled_but_tail =
        |id: u64, built: &BuiltDirectory| (id + 1 < SHARDS).then(|| tabled(id, built)).flatten();
    let two_tiers = |shard_id: u64| {
        if shard_id < SHARDS / 2 {
            &DECOUPLED_32K_32K
        } else {
            &DECOUPLED_2K_8K
        }
    };
    let mut publications: Vec<(&str, tempfile::TempDir, ShardMap)> = Vec::new();
    let dir = tempfile::tempdir().unwrap();
    let map = publish(dir.path(), &per_shard);
    publications.push(("no tables", dir, map));
    let dir = tempfile::tempdir().unwrap();
    let map = publish_choosing(dir.path(), &per_shard, |_| &DECOUPLED_2K_8K, 0, "", tabled);
    publications.push(("tables", dir, map));
    let dir = tempfile::tempdir().unwrap();
    let map = publish_choosing(
        dir.path(),
        &per_shard,
        |_| &DECOUPLED_2K_8K,
        0,
        "",
        tabled_but_tail,
    );
    publications.push(("sealed tables", dir, map));
    let dir = tempfile::tempdir().unwrap();
    let map = publish_with(dir.path(), &per_shard, two_tiers, 0, "");
    publications.push(("two geometries", dir, map));

    for (name, dir, map) in &publications {
        let base = serve(dir.path()).await;
        for wallet in &wallets {
            let (sequential, reference) = run_recorded(
                dir.path(),
                base.clone(),
                wallet.clone(),
                map.clone(),
                1,
                Batches::Pass,
            )
            .await;
            let (concurrent, log) = run_recorded(
                dir.path(),
                base.clone(),
                wallet.clone(),
                map.clone(),
                4,
                Batches::Pass,
            )
            .await;
            compare(&sequential.ledger, &traverse(&per_shard, wallet, 0));
            compare(&concurrent.ledger, &sequential.ledger);
            assert_eq!(concurrent.charges, sequential.charges, "{name}");
            assert_eq!(concurrent.matched_shards, sequential.matched_shards);
            assert_eq!(
                concurrent.unproductive_matches,
                sequential.unproductive_matches
            );
            assert_eq!(concurrent.covered_through, sequential.covered_through);
            assert_eq!(concurrent.provisional, sequential.provisional);
            assert_eq!(
                sorted(&log.requests),
                sorted(&reference.requests),
                "{name}: the concurrent walk must make exactly the sequential walk's requests"
            );
            assert_eq!(reference.batches, 0, "concurrency 1 is the sequential walk");
            if sequential.matched_shards.len() > 1 {
                assert!(
                    log.batches > 0,
                    "{name}: several matched shards are batched"
                );
                assert!(
                    log.rounds < reference.rounds,
                    "{name}: {} rounds concurrently, {} in sequence",
                    log.rounds,
                    reference.rounds
                );
            }
            eprintln!(
                "{name}, {} scripts, {} matched shards: {} requests, {} rounds in sequence, {} \
                 at concurrency 4",
                wallet.len(),
                sequential.matched_shards.len(),
                reference.requests.len(),
                reference.rounds,
                log.rounds
            );
        }
    }
}

/// A batch that sends nothing, or stops at a refused first request, leaves
/// the rest to the walk, which makes them in sequence with its ordinary
/// handling: the refusal is waited out where the sequential walk would have
/// met it, and the result, the charges and what reached the service are the
/// sequential walk's.
#[tokio::test(flavor = "multi_thread")]
async fn a_failed_batch_falls_back_to_the_sequential_walk() {
    let per_shard = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish_choosing(dir.path(), &per_shard, |_| &DECOUPLED_2K_8K, 0, "", tabled);
    let base = serve(dir.path()).await;
    let wallet = vec![script(1), script(2), script(3), script(999)];
    let (sequential, reference) = run_recorded(
        dir.path(),
        base.clone(),
        wallet.clone(),
        map.clone(),
        1,
        Batches::Pass,
    )
    .await;
    for batches in [Batches::Unsent, Batches::RefuseFirst] {
        let (fallen_back, log) = run_recorded(
            dir.path(),
            base.clone(),
            wallet.clone(),
            map.clone(),
            4,
            batches,
        )
        .await;
        compare(&fallen_back.ledger, &traverse(&per_shard, &wallet, 0));
        assert_eq!(fallen_back.charges, sequential.charges, "{batches:?}");
        assert_eq!(fallen_back.map_refreshes, 0);
        assert!(log.batches > 0, "{batches:?}: the batch was attempted");
        assert_eq!(
            log.requests, reference.requests,
            "{batches:?}: the walk made every request itself, in the sequential order"
        );
    }
}

/// As [`serve`], with every request held for `delay` before it is answered,
/// standing in for a network round trip. Requests in flight together wait
/// together, as they would on a real link.
async fn serve_delayed(dir: &Path, delay: std::time::Duration) -> String {
    let set = ShardSet::open(dir, DEFAULT_RETAIN_REVISIONS).expect("load");
    let state = ServiceState::build(set, ServiceConfig::default()).expect("state");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let app = router(state).layer(axum::middleware::from_fn(
        move |request: axum::extract::Request, next: axum::middleware::Next| async move {
            tokio::time::sleep(delay).await;
            next.run(request).await
        },
    ));
    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    format!("http://{addr}")
}

/// A local indication of what cross-shard concurrency buys under a round
/// trip: sync time at several concurrencies with every shard service request
/// delayed by 100 ms. Filters are local here, so this isolates the shard
/// service requests. Not a measurement of any deployment.
///
/// `cargo test --release -p transparent-shard-server --test wallet_sync
/// concurrency_under_emulated_delay -- --ignored --nocapture`
#[tokio::test(flavor = "multi_thread")]
#[ignore = "timing indication, about a minute"]
async fn concurrency_under_emulated_delay() {
    let per_shard = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish_choosing(dir.path(), &per_shard, |_| &DECOUPLED_2K_8K, 0, "", tabled);
    let base = serve_delayed(dir.path(), std::time::Duration::from_millis(100)).await;
    let wallet = vec![script(1), script(2), script(3), script(100), script(101)];
    for concurrency in [1usize, 2, 4, 8] {
        let mut seconds = Vec::new();
        let mut rounds = 0;
        let mut requests = 0;
        for _ in 0..5 {
            let started = std::time::Instant::now();
            let (outcome, log) = run_recorded(
                dir.path(),
                base.clone(),
                wallet.clone(),
                map.clone(),
                concurrency,
                Batches::Pass,
            )
            .await;
            seconds.push(started.elapsed().as_secs_f64());
            compare(&outcome.ledger, &traverse(&per_shard, &wallet, 0));
            rounds = log.rounds;
            requests = log.requests.len();
        }
        seconds.sort_by(f64::total_cmp);
        eprintln!(
            "concurrency {concurrency}: {requests} requests, {rounds} rounds, p50 {:.2} s (min \
             {:.2}, max {:.2})",
            seconds[seconds.len() / 2],
            seconds[0],
            seconds[seconds.len() - 1]
        );
    }
}

/// A set published under the lower-precision range profile syncs exactly, with
/// the same private work as the original profile, and smaller filters.
#[tokio::test(flavor = "multi_thread")]
async fn a_set_under_the_v2_range_profile_syncs_exactly_with_smaller_filters() {
    let per_shard = chain();
    let wallet = vec![script(1), script(2), script(3), script(999)];

    let v1 = tempfile::tempdir().unwrap();
    let v1_map = publish(v1.path(), &per_shard);
    let v2 = tempfile::tempdir().unwrap();
    let v2_map = publish_profiled(
        v2.path(),
        &per_shard,
        |_| &DECOUPLED_2K_8K,
        0,
        "",
        tabled,
        transparent_filter::RANGE_PROFILE_V2.name,
    );
    assert_eq!(v2_map.profile, transparent_filter::RANGE_PROFILE_V2.name);

    let v1_base = serve(v1.path()).await;
    let v2_base = serve(v2.path()).await;
    let old = run_sync(v1.path(), v1_base, wallet.clone(), FIRST, v1_map).await;
    let new = run_sync(v2.path(), v2_base, wallet.clone(), FIRST, v2_map).await;
    compare(&new.ledger, &traverse(&per_shard, &wallet, 0));
    compare(&new.ledger, &old.ledger);
    assert!(new.charges.filter_bytes < old.charges.filter_bytes);
    assert_eq!(new.charges.pages.queries, old.charges.pages.queries);
}

/// A map naming a range profile this build does not know stops the sync
/// before any filter is matched.
#[tokio::test(flavor = "multi_thread")]
async fn an_unknown_range_profile_stops_the_sync_before_matching() {
    let per_shard = chain();
    let dir = tempfile::tempdir().unwrap();
    let mut map = publish(dir.path(), &per_shard);
    map.profile = "zcash-transparent-range-v99".into();
    let filters = PublishedFilters::load(
        dir.path(),
        &publish(tempfile::tempdir().unwrap().path(), &per_shard),
    );
    let base = serve(dir.path()).await;
    let wallet = vec![script(1)];
    let error = tokio::task::spawn_blocking(move || {
        let client = reqwest::blocking::Client::new();
        let mut transport = HttpShards {
            base: base.clone(),
            client: client.clone(),
        };
        let raw = client
            .get(format!("{base}/v2/shards/init"))
            .send()
            .unwrap()
            .bytes()
            .unwrap();
        let geometry = parse_init(&raw);
        let mut filters = filters;
        sync(
            &map,
            0,
            &geometry,
            &mut filters,
            &mut transport,
            &wallet,
            FIRST,
            &transparent_wallet::StaticChain::from_map(&map),
            &transparent_wallet::Anchor {
                height: map.shards.last().unwrap().end_height,
                hash: map.shards.last().unwrap().terminal_block_hash.clone(),
            },
        )
        .err()
        .expect("an unknown profile must be refused")
    })
    .await
    .unwrap();
    assert!(
        matches!(error, transparent_wallet::SyncError::UnknownProfile(ref name) if name == "zcash-transparent-range-v99"),
        "{error}"
    );
}

/// Alters what one shard's service sends: a byte of every directory reply, or
/// its manifest. Unlike a corrupt published row, neither can be aimed at a
/// row: the service makes both without knowing which row a query selects.
#[derive(Clone, Copy, Debug)]
enum Alter {
    /// Inverts this byte of every directory reply: 0 is in the revision
    /// binding, 8 in the parameter epoch.
    ReplyByte(usize),
    Manifest,
    /// Refuses the shard's first page query as an overloaded service does,
    /// once, without sending it: a later round than the directory's.
    RefusePage,
}

/// Which shard [`Alters`] alters, and how.
type Corruption = (u64, Alter);

/// A transport passing [`Alter`] on one shard's replies.
struct Alters<T> {
    inner: T,
    corruption: Option<Corruption>,
    /// Whether [`Alter::RefusePage`] has refused its query.
    refused: bool,
}

impl<T> Alters<T> {
    /// The overload refusal for this query, if [`Alter::RefusePage`] makes
    /// one of it.
    fn refusal(&mut self, shard_id: u64, revision: &str, table: Table) -> Option<BoxError> {
        let refuses =
            matches!(self.corruption, Some((shard, Alter::RefusePage)) if shard == shard_id);
        if !refuses || self.refused || !matches!(table, Table::Pages(_)) {
            return None;
        }
        self.refused = true;
        let body = br#"{"error":"no cache capacity is free","retry":"retry shortly"}"#;
        refusal(503, Some("0"), body, shard_id, revision)
    }

    fn reply(&self, shard_id: u64, table: Table, mut reply: Vec<u8>) -> Vec<u8> {
        if let Some((shard, Alter::ReplyByte(byte))) = self.corruption {
            if shard == shard_id && table == Table::Directory {
                reply[byte] ^= 0xff;
            }
        }
        reply
    }

    fn manifest(&self, shard_id: u64, mut raw: Vec<u8>) -> Vec<u8> {
        if let Some((shard, Alter::Manifest)) = self.corruption {
            if shard == shard_id {
                let middle = raw.len() / 2;
                raw[middle] ^= 1;
            }
        }
        raw
    }
}

impl<T: ShardTransport> ShardTransport for Alters<T> {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        self.inner.init()
    }

    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        let (raw, cost) = self.inner.manifest(shard_id, revision)?;
        Ok((Alters::manifest(self, shard_id, raw), cost))
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
        if let Some(refused) = self.refusal(shard_id, revision, table) {
            return Err(refused);
        }
        let answer = self.inner.query(shard_id, revision, table, body)?;
        Ok(self.reply(shard_id, table, answer))
    }

    fn concurrency(&self) -> usize {
        self.inner.concurrency()
    }

    fn batch(&mut self, requests: &[ShardRequest<'_>]) -> Vec<Option<ShardReply>> {
        // A refused request ends the batch, as a refusal from the service
        // does: what came before it is sent, nothing after it.
        let mut refused = None;
        for (index, request) in requests.iter().enumerate() {
            if let ShardRequest::Query {
                shard_id,
                revision,
                table,
                ..
            } = *request
            {
                if let Some(error) = self.refusal(shard_id, revision, table) {
                    refused = Some((index, error));
                    break;
                }
            }
        }
        let sent = refused.as_ref().map_or(requests.len(), |(index, _)| *index);
        let mut replies = self.inner.batch(&requests[..sent]);
        if let Some((_, error)) = refused {
            replies.push(Some(Err(error)));
            replies.resize_with(requests.len(), || None);
        }
        requests
            .iter()
            .zip(replies)
            .map(|(request, reply)| match (*request, reply) {
                (
                    ShardRequest::Query {
                        shard_id, table, ..
                    },
                    Some(Ok((answer, cost))),
                ) => Some(Ok((self.reply(shard_id, table, answer), cost))),
                (ShardRequest::Manifest { shard_id, .. }, Some(Ok((raw, cost)))) => {
                    Some(Ok((Alters::manifest(self, shard_id, raw), cost)))
                }
                (_, reply) => reply,
            })
            .collect()
    }
}

/// What a recorded [`sync_into`](transparent_wallet::sync_into) against one
/// store returned and sent.
type Recorded = (
    Result<transparent_wallet::SyncReport, transparent_wallet::SyncError>,
    Log,
);

/// Syncs `store` once from the set's start to its tip, through the reference
/// HTTP transport at `concurrency`, with `corruption` applied, recorded as the
/// service sees it.
#[allow(clippy::too_many_arguments)]
async fn sync_recorded(
    dir: &Path,
    base: &str,
    map: &ShardMap,
    store: transparent_wallet::MemoryStore,
    wallet: &[ScriptBytes],
    concurrency: usize,
    corruption: Option<Corruption>,
) -> (transparent_wallet::MemoryStore, Recorded) {
    let filters = PublishedFilters::load(dir, map);
    let map = map.clone();
    let base = base.to_string();
    let wallet = wallet.to_vec();
    tokio::task::spawn_blocking(move || {
        use transparent_wallet::{
            Anchor, ScriptEntry, ScriptOrigin, StaticChain, StaticScripts, WorkLimits,
        };
        let mut store = store;
        let mut http = HttpShardTransport::new(&base, &HttpOptions::default())
            .unwrap()
            .with_concurrency(concurrency);
        let geometry = http.geometry().unwrap();
        let log = Arc::new(Mutex::new(Log::default()));
        let mut transport = Recording {
            inner: Alters {
                inner: http,
                corruption,
                refused: false,
            },
            log: log.clone(),
            batches: Batches::Pass,
        };
        let mut filters = filters;
        let mut provider = StaticScripts(
            wallet
                .iter()
                .map(|s| ScriptEntry {
                    script: s.as_slice().to_vec(),
                    origin: ScriptOrigin::Derived,
                    required_from: FIRST,
                })
                .collect(),
        );
        let tip = map.shards.last().unwrap();
        let result = transparent_wallet::sync_into(
            &mut store,
            &map,
            0,
            &geometry,
            &StaticChain::from_map(&map),
            &mut provider,
            &mut filters,
            &mut transport,
            &WorkLimits::UNLIMITED,
            &Anchor {
                height: tip.end_height,
                hash: tip.terminal_block_hash.clone(),
            },
        );
        let log = log.lock().unwrap().clone();
        (store, (result, log))
    })
    .await
    .unwrap()
}

/// What the service saw, as (route, body length) in order, with each
/// shard's revision digest replaced by its id: two publications that differ
/// only in a row's content have different digests, and the same transcript.
fn transcript(log: &Log, map: &ShardMap) -> Vec<(String, usize)> {
    log.sent
        .iter()
        .map(|(route, len)| {
            let mut route = route.clone();
            for entry in &map.shards {
                route = route.replace(&entry.manifest_digest, &format!("shard-{}", entry.shard_id));
            }
            (route, *len)
        })
        .collect()
}

/// Directory queries the service saw for shard `shard_id`.
fn directory_queries(log: &Log, shard_id: u64) -> usize {
    let prefix = format!("/v2/shards/{shard_id}/");
    log.sent
        .iter()
        .filter(|(route, _)| route.starts_with(&prefix) && route.ends_with("/query/directory"))
        .count()
}

/// The directory row of a built shard holding `script`'s entry, and the entry.
fn entry_row(
    built: &BuiltDirectory,
    script: &ScriptBytes,
) -> (u64, transparent_shard::records::DirectoryEntry) {
    let salt = transparent_shard::tag_salt(
        built.shard_id,
        &hash_at(built.end_height),
        built.tag_salt_counter,
    );
    let tag = transparent_shard::script_tag(&salt, script.as_slice());
    let rows = built.geometry.directory_rows * u64::from(built.directory_segments());
    transparent_shard::build::candidate_rows(built.shard_id, script.as_slice(), rows)
        .into_iter()
        .find_map(|row| {
            transparent_shard::records::decode_directory_row(
                built.directory_row(row),
                built.children,
            )
            .unwrap()
            .into_iter()
            .find(|entry| entry.tag == tag)
            .map(|entry| (row, entry))
        })
        .expect("an entry")
}

/// The page row holding the last fragment of `script`'s oldest run in a
/// built shard: the last row its walk reads.
fn last_page_row(
    built: &BuiltDirectory,
    children: &[BuiltChild],
    script: &ScriptBytes,
) -> (u32, u64) {
    let (child, row, head) = oldest_run(built, children, script);
    (child, row + u64::from(head.fragment_count) - 1)
}

/// `script`'s oldest run in a built shard: its page shard, the row of its
/// first fragment, and that fragment.
fn oldest_run(
    built: &BuiltDirectory,
    children: &[BuiltChild],
    script: &ScriptBytes,
) -> (u32, u64, transparent_shard::PageEntry) {
    let (_, entry) = entry_row(built, script);
    let mut next = entry.head();
    let mut last = None;
    while let Some((child, row)) = next {
        let page = &children[usize::from(child)];
        let salt = transparent_shard::page_tag_salt(
            built.shard_id,
            page.index,
            page.end_height,
            &page.terminal_block_hash,
            page.tag_salt_counter,
        );
        let tag = transparent_shard::script_tag(&salt, script.as_slice());
        let head =
            transparent_shard::decode_page_row(page.page_row(built.geometry, u64::from(row)))
                .unwrap()
                .into_iter()
                .find(|entry| entry.tag == tag && entry.ordinal == 0)
                .unwrap();
        next = head.link.previous();
        last = Some((u32::from(child), u64::from(row), head));
    }
    last.expect("a paged script")
}

/// Publishes the chain honestly and again with the row `bad` names
/// corrupted, and serves both.
async fn honest_and_corrupt(
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    bad: BadRow<'_>,
) -> (
    (tempfile::TempDir, ShardMap, String),
    (tempfile::TempDir, ShardMap, String),
) {
    let honest_dir = tempfile::tempdir().unwrap();
    let honest_map = publish(honest_dir.path(), per_shard);
    let corrupt_dir = tempfile::tempdir().unwrap();
    let corrupt_map = publish_corrupting(corrupt_dir.path(), per_shard, bad);
    let honest_base = serve(honest_dir.path()).await;
    let corrupt_base = serve(corrupt_dir.path()).await;
    (
        (honest_dir, honest_map, honest_base),
        (corrupt_dir, corrupt_map, corrupt_base),
    )
}

/// A published directory row whose content does not decode, the one holding
/// script 1's entry in shard 2, fails that entry. Every later shard is still
/// read, the commit lands for every other script, and what the service sees,
/// every route, table and body length in order, is what it sees from an
/// honest sync, in which that entry's history is inline. Sequentially and
/// with concurrent lookahead alike.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_directory_row_fails_one_entry_and_the_sync_finishes() {
    use transparent_wallet::{Completion, FailedEntry, IncompleteReason, MemoryStore};
    let per_shard = chain();
    let ((honest_dir, honest_map, honest_base), (dir, map, base)) =
        honest_and_corrupt(&per_shard, &|shard_id, built, _| {
            (shard_id == 2).then(|| (Table::Directory, entry_row(built, &script(1)).0))
        })
        .await;
    let wallet = vec![script(1), script(2)];
    for concurrency in [1, 4] {
        let (_, (honest, honest_log)) = sync_recorded(
            honest_dir.path(),
            &honest_base,
            &honest_map,
            MemoryStore::new(),
            &wallet,
            concurrency,
            None,
        )
        .await;
        let honest = honest.unwrap();
        assert_eq!(honest.completion, Completion::Complete);
        compare(&honest.ledger, &traverse(&per_shard, &wallet, 0));

        let (store, (report, log)) = sync_recorded(
            dir.path(),
            &base,
            &map,
            MemoryStore::new(),
            &wallet,
            concurrency,
            None,
        )
        .await;
        let report = report.unwrap();
        assert_eq!(
            transcript(&log, &map),
            transcript(&honest_log, &honest_map),
            "concurrency {concurrency}: the service sees the honest transcript"
        );
        assert!(matches!(
            report.completion,
            Completion::Incomplete {
                reason: IncompleteReason::ContentFailed,
                ..
            }
        ));
        assert!(report.failed_entries.contains(&FailedEntry {
            shard_id: 2,
            script: script(1).as_slice().to_vec(),
            failures: 1,
        }));
        assert!(report
            .failed_entries
            .iter()
            .all(|entry| entry.shard_id == 2));
        assert!(directory_queries(&log, 3) > 0, "the shard after is read");
        // Everything but the failed entry was recovered: script 1's spend in
        // shard 2 is all that is missing.
        let mut without = per_shard.clone();
        without[2].retain(|(owner, _)| owner.as_slice() != script(1).as_slice());
        compare(&report.ledger, &traverse(&without, &wallet, 0));
        assert!(report.covered_through < map.shards[2].end_height);
        assert!(transparent_wallet::WalletStore::anchor(&store)
            .unwrap()
            .is_none());
    }
}

/// An entry whose row keeps failing is asked three times, one sync each, and
/// then not again. The count is the store's and nothing resets it; the
/// report keeps listing the entry.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_entry_is_asked_three_times_then_held() {
    use transparent_wallet::{Completion, IncompleteReason, MemoryStore, WalletStore};
    let per_shard = chain();
    let dir = tempfile::tempdir().unwrap();
    let map = publish_corrupting(dir.path(), &per_shard, &|shard_id, built, _| {
        (shard_id == 2).then(|| (Table::Directory, entry_row(built, &script(1)).0))
    });
    let base = serve(dir.path()).await;
    let wallet = vec![script(1), script(2)];
    let mut store = MemoryStore::new();
    for attempt in 1..=5u32 {
        let (kept, (report, log)) =
            sync_recorded(dir.path(), &base, &map, store, &wallet, 1, None).await;
        store = kept;
        let report = report.unwrap();
        let failures = attempt.min(3);
        assert_eq!(
            store.entry_failures(2, script(1).as_slice()).unwrap(),
            failures,
            "attempt {attempt}"
        );
        assert!(report
            .failed_entries
            .iter()
            .any(|entry| entry.script == script(1).as_slice() && entry.failures == failures));
        assert!(matches!(
            report.completion,
            Completion::Incomplete {
                reason: IncompleteReason::ContentFailed,
                ..
            }
        ));
        // Without a choice table a script costs two candidate rows. After
        // the first sync, script 1's entry in shard 2 is all that is owed.
        let asked = directory_queries(&log, 2);
        match attempt {
            1 => assert!(asked >= 2),
            2 | 3 => assert_eq!(asked, 2, "attempt {attempt} asks again"),
            _ => assert_eq!(asked, 0, "attempt {attempt} asks no more"),
        }
    }
}

/// A published page row whose content does not decode, the last one script
/// 3's walk reads, fails that walk alone. Nothing the walk would have sent
/// after depends on it, so the service sees exactly the honest transcript,
/// route and body length in order, sequentially and at concurrency 4. The
/// entry is counted, its walk stays owed, and the next sync asks again.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_page_row_fails_one_walk_with_the_honest_transcript() {
    use transparent_wallet::{Completion, IncompleteReason, MemoryStore, WalletStore};
    let per_shard = chain();
    let ((honest_dir, honest_map, honest_base), (dir, map, base)) =
        honest_and_corrupt(&per_shard, &|shard_id, built, children| {
            (shard_id == 1).then(|| {
                let (child, row) = last_page_row(built, children, &script(3));
                (Table::Pages(child), row)
            })
        })
        .await;
    let wallet = vec![script(1), script(2), script(3)];
    for concurrency in [1, 4] {
        let (_, (honest, honest_log)) = sync_recorded(
            honest_dir.path(),
            &honest_base,
            &honest_map,
            MemoryStore::new(),
            &wallet,
            concurrency,
            None,
        )
        .await;
        assert_eq!(honest.unwrap().completion, Completion::Complete);
        let (store, (report, log)) = sync_recorded(
            dir.path(),
            &base,
            &map,
            MemoryStore::new(),
            &wallet,
            concurrency,
            None,
        )
        .await;
        let report = report.unwrap();
        assert_eq!(
            transcript(&log, &map),
            transcript(&honest_log, &honest_map),
            "concurrency {concurrency}: the service sees the honest transcript"
        );
        assert!(matches!(
            report.completion,
            Completion::Incomplete {
                reason: IncompleteReason::ContentFailed,
                ..
            }
        ));
        assert_eq!(report.failed_entries.len(), 1);
        assert_eq!(report.failed_entries[0].shard_id, 1);
        assert_eq!(report.failed_entries[0].script, script(3).as_slice());
        assert_eq!(store.pending().unwrap().len(), 1, "the walk stays owed");

        let (store, (retried, _)) =
            sync_recorded(dir.path(), &base, &map, store, &wallet, concurrency, None).await;
        assert!(retried.unwrap().failed_entries[0].failures == 2);
        assert_eq!(store.entry_failures(1, script(3).as_slice()).unwrap(), 2);
    }
}

/// [`chain`] with script 4 active in shard 1 on both sides of its page-shard
/// boundary: an older run of several fragments, then a short newer one.
fn chain_with_a_long_oldest_run() -> Vec<Vec<(ScriptBytes, TransparentEvent)>> {
    let mut per_shard = chain();
    let start = FIRST + SPAN;
    let older = 2 * transparent_shard::EVENTS_PER_PAGE + 10;
    for i in 0..older + 3 {
        let height = if i < older {
            start + u64::from(i) % 90
        } else {
            start + 150 + u64::from(i - older)
        };
        per_shard[1].push((
            script(4),
            TransparentEvent::Receive(ReceiveEvent {
                metadata: None,
                height: height as u32,
                txid: txid(50_000 + u64::from(i)),
                transaction_index: 4,
                output_index: 0,
                value: 13,
                coinbase: false,
            }),
        ));
    }
    per_shard
}

/// A published page row that decodes but whose first fragment carries a bad
/// link, on the head of script 4's oldest run in shard 1, which its chain
/// promised no older run. The walk fails there and still owes the run's
/// other fragments: it sends them as dummies of the same length to the same
/// table, and no head after them, as the honest walk sends the fragments and
/// stops. So the padded dummies cross the real HTTP transport and the service
/// sees exactly the honest transcript, sequentially and at concurrency 4.
#[tokio::test(flavor = "multi_thread")]
async fn a_bad_link_owes_dummies_and_keeps_the_honest_transcript() {
    use transparent_shard::{decode_page_row, encode_page_row, page_tag_salt, script_tag, RunLink};
    use transparent_wallet::{Completion, IncompleteReason, MemoryStore, WalletStore};
    let per_shard = chain_with_a_long_oldest_run();
    let honest_dir = tempfile::tempdir().unwrap();
    let honest_map = publish(honest_dir.path(), &per_shard);
    let dir = tempfile::tempdir().unwrap();
    let owed = std::sync::atomic::AtomicU32::new(0);
    let map = publish_rewriting(dir.path(), &per_shard, &|shard_id, built, children| {
        if shard_id != 1 {
            return None;
        }
        let (child, row, head) = oldest_run(built, children, &script(4));
        assert_ne!(
            entry_row(built, &script(4)).1.head(),
            Some((child as u8, row as u32)),
            "a run reached by a link"
        );
        assert_eq!(head.link, RunLink::default(), "the oldest run");
        owed.store(
            head.fragment_count - 1,
            std::sync::atomic::Ordering::Relaxed,
        );
        let page = &children[child as usize];
        let tag = script_tag(
            &page_tag_salt(
                built.shard_id,
                page.index,
                page.end_height,
                &page.terminal_block_hash,
                page.tag_salt_counter,
            ),
            script(4).as_slice(),
        );
        let entries = decode_page_row(page.page_row(built.geometry, row))
            .unwrap()
            .into_iter()
            .map(|entry| {
                if entry.tag == tag && entry.ordinal == 0 {
                    // A link to an older run the chain did not promise.
                    entry
                        .with_link(RunLink {
                            prev_child: 0,
                            prev_row: 1,
                            older_runs: 1,
                        })
                        .unwrap()
                } else {
                    entry
                }
            })
            .collect::<Vec<_>>();
        Some((Table::Pages(child), row, encode_page_row(&entries).unwrap()))
    });
    let owed = owed.into_inner();
    assert!(owed >= 2, "{owed} fragments after the head");
    let honest_base = serve(honest_dir.path()).await;
    let base = serve(dir.path()).await;
    let wallet = vec![script(1), script(2), script(3), script(4)];
    for concurrency in [1, 4] {
        let (_, (honest, honest_log)) = sync_recorded(
            honest_dir.path(),
            &honest_base,
            &honest_map,
            MemoryStore::new(),
            &wallet,
            concurrency,
            None,
        )
        .await;
        assert_eq!(honest.unwrap().completion, Completion::Complete);
        let (store, (report, log)) = sync_recorded(
            dir.path(),
            &base,
            &map,
            MemoryStore::new(),
            &wallet,
            concurrency,
            None,
        )
        .await;
        let report = report.unwrap();
        assert_eq!(
            transcript(&log, &map),
            transcript(&honest_log, &honest_map),
            "concurrency {concurrency}: the service sees the honest transcript"
        );
        assert!(matches!(
            report.completion,
            Completion::Incomplete {
                reason: IncompleteReason::ContentFailed,
                ..
            }
        ));
        assert_eq!(report.failed_entries.len(), 1);
        assert_eq!(report.failed_entries[0].shard_id, 1);
        assert_eq!(report.failed_entries[0].script, script(4).as_slice());
        assert_eq!(store.entry_failures(1, script(4).as_slice()).unwrap(), 1);
    }
}

/// A corrupt directory row fails script 2's entry in shard 1, whose honest
/// history there is inline, and the service then refuses the shard's first
/// page round as overloaded. The retried read asks the directory again for
/// every matched script without a saved walk, as the honest retry does, the
/// failed entry included: the same queries in the same order, so the
/// directory queries of the re-read do not show which row the wallet read.
/// The failure is counted once.
#[tokio::test(flavor = "multi_thread")]
async fn an_overload_after_a_failed_entry_retries_with_the_honest_transcript() {
    use transparent_wallet::{Completion, FailedEntry, IncompleteReason, MemoryStore, WalletStore};
    let per_shard = chain();
    let ((honest_dir, honest_map, honest_base), (dir, map, base)) =
        honest_and_corrupt(&per_shard, &|shard_id, built, _| {
            (shard_id == 1).then(|| (Table::Directory, entry_row(built, &script(2)).0))
        })
        .await;
    let wallet = vec![script(2), script(3)];
    let refuse = Some((1, Alter::RefusePage));
    // Shard 1's directory queries after the refused page query: the re-read.
    let reread = |log: &Log| -> Vec<(String, usize)> {
        let refused = log
            .sent
            .iter()
            .position(|(route, _)| {
                route.starts_with("/v2/shards/1/") && route.contains("/query/pages")
            })
            .expect("a page query");
        log.sent[refused + 1..]
            .iter()
            .filter(|(route, _)| {
                route.starts_with("/v2/shards/1/") && route.ends_with("/query/directory")
            })
            .cloned()
            .collect()
    };
    for concurrency in [1, 4] {
        let (_, (honest, honest_log)) = sync_recorded(
            honest_dir.path(),
            &honest_base,
            &honest_map,
            MemoryStore::new(),
            &wallet,
            concurrency,
            refuse,
        )
        .await;
        assert_eq!(honest.unwrap().completion, Completion::Complete);
        let honest_reread = reread(&honest_log);
        assert!(!honest_reread.is_empty(), "the honest retry asks again");

        let (store, (report, log)) = sync_recorded(
            dir.path(),
            &base,
            &map,
            MemoryStore::new(),
            &wallet,
            concurrency,
            refuse,
        )
        .await;
        let report = report.unwrap();
        let honest_reread: Vec<_> = honest_reread
            .iter()
            .map(|(route, len)| {
                (
                    route.replace(&honest_map.shards[1].manifest_digest, "shard-1"),
                    *len,
                )
            })
            .collect();
        let corrupt_reread: Vec<_> = reread(&log)
            .iter()
            .map(|(route, len)| {
                (
                    route.replace(&map.shards[1].manifest_digest, "shard-1"),
                    *len,
                )
            })
            .collect();
        assert_eq!(
            corrupt_reread, honest_reread,
            "concurrency {concurrency}: the re-read asks what the honest one asks"
        );
        assert_eq!(
            transcript(&log, &map),
            transcript(&honest_log, &honest_map),
            "concurrency {concurrency}: the service sees the honest transcript"
        );
        assert!(matches!(
            report.completion,
            Completion::Incomplete {
                reason: IncompleteReason::ContentFailed,
                ..
            }
        ));
        assert_eq!(
            report.failed_entries,
            vec![FailedEntry {
                shard_id: 1,
                script: script(2).as_slice().to_vec(),
                failures: 1,
            }]
        );
        assert_eq!(store.entry_failures(1, script(2).as_slice()).unwrap(), 1);
    }
}

/// A reply whose revision binding or parameter epoch does not verify fails
/// its request, not an entry: the read stops as it always has, nothing is
/// counted, and a reply made under other parameters than the setup the
/// wallet holds drops that setup, so the next sync fetches it again.
#[tokio::test(flavor = "multi_thread")]
async fn a_reply_failing_its_binding_or_epoch_fails_the_request_not_an_entry() {
    use transparent_wallet::client::ClientError;
    use transparent_wallet::{MemoryStore, SetupKey, SyncError, WalletStore};
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;
    let wallet = vec![script(1), script(2)];

    let (store, (result, _)) = sync_recorded(
        dir.path(),
        &base,
        &map,
        MemoryStore::new(),
        &wallet,
        1,
        Some((2, Alter::ReplyByte(0))),
    )
    .await;
    assert!(
        matches!(result, Err(SyncError::Client(ClientError::Response(_)))),
        "{:?}",
        result.err()
    );
    assert_eq!(store.entry_failures(2, script(1).as_slice()).unwrap(), 0);

    let (store, (result, _)) = sync_recorded(
        dir.path(),
        &base,
        &map,
        MemoryStore::new(),
        &wallet,
        1,
        Some((2, Alter::ReplyByte(8))),
    )
    .await;
    assert!(
        matches!(
            result,
            Err(SyncError::Client(ClientError::SetupMismatch { .. }))
        ),
        "{:?}",
        result.err()
    );
    assert_eq!(store.entry_failures(2, script(1).as_slice()).unwrap(), 0);
    let set_digest = store.set_identity().unwrap().unwrap().digest();
    let key = |shard: usize| SetupKey {
        set_digest: set_digest.clone(),
        revision_digest: map.shards[shard].manifest_digest.clone(),
        table: Table::Directory,
        segment: 0,
    };
    assert!(
        store.setup(&key(0)).unwrap().is_some(),
        "a good setup stays"
    );
    assert_eq!(
        store.setup(&key(2)).unwrap(),
        None,
        "the stale one is dropped"
    );
}

/// A manifest that does not verify still stops the sync: it is the same for
/// every wallet, so refusing it reveals nothing about the wallet's rows.
#[tokio::test(flavor = "multi_thread")]
async fn a_corrupt_manifest_still_stops_the_sync() {
    use transparent_wallet::{MemoryStore, WalletStore};
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;
    let wallet = vec![script(1), script(2)];
    let (store, (result, _)) = sync_recorded(
        dir.path(),
        &base,
        &map,
        MemoryStore::new(),
        &wallet,
        1,
        Some((2, Alter::Manifest)),
    )
    .await;
    assert!(result.is_err(), "a corrupt manifest is refused");
    assert_eq!(store.entry_failures(2, script(1).as_slice()).unwrap(), 0);
}

/// A malformed shard map still stops the sync before anything is read.
#[tokio::test(flavor = "multi_thread")]
async fn a_malformed_shard_map_still_stops_the_sync() {
    use transparent_wallet::{MemoryStore, SyncError};
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    let base = serve(dir.path()).await;
    let mut malformed = map.clone();
    malformed.shards[1].start_height += 1;
    let (_, (result, log)) = sync_recorded(
        dir.path(),
        &base,
        &malformed,
        MemoryStore::new(),
        &[script(1), script(2)],
        1,
        None,
    )
    .await;
    assert!(
        matches!(result, Err(SyncError::Invalid(ref why)) if why.contains("malformed")),
        "{:?}",
        result.err()
    );
    assert!(log.sent.is_empty(), "nothing is read");
}

/// The runtimes a publisher prepares beside the publication in `dir`, all
/// built: nothing is retained to link from.
fn prepare_runtimes(dir: &Path) -> transparent_shard_server::prebuild::HistoryPrepared {
    let set = ShardSet::open(dir, DEFAULT_RETAIN_REVISIONS).expect("load");
    let prepared =
        transparent_shard_server::prebuild::prepare_publication(&set, dir, &[], &|| false)
            .expect("prepare");
    assert!(prepared.failures.is_empty(), "{:?}", prepared.failures);
    assert_eq!(prepared.built(), prepared.files.len());
    assert_eq!(prepared.files.len(), set.warm_targets().len());
    prepared
}

/// Starts the service on an ephemeral port, offered the runtimes prepared
/// beside the publication or, with `offered` false, none; returns its base
/// URL and its serving state, whose counters the tests read.
async fn serve_prepared(dir: &Path, offered: bool) -> (String, ServiceState) {
    let set = ShardSet::open(dir, DEFAULT_RETAIN_REVISIONS).expect("load");
    let shipped = offered
        .then(|| transparent_shard_server::runtime::disk::ShippedRuntimes::new(dir.to_path_buf()));
    let state = ServiceState::build_with_runtimes(set, ServiceConfig::default(), None, shipped)
        .expect("state");
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let serving = state.clone();
    tokio::spawn(async move {
        axum::serve(listener, router(serving)).await.unwrap();
    });
    (format!("http://{addr}"), state)
}

fn counter(
    state: &ServiceState,
    pick: fn(&transparent_shard_server::metrics::Metrics) -> &std::sync::atomic::AtomicU64,
) -> u64 {
    transparent_shard_server::metrics::Metrics::get(pick(state.metrics()))
}

/// A wallet syncing against a worker offered the publisher's runtimes
/// recovers exactly what an independent traversal of the events does, and
/// the worker builds nothing: every runtime the sync touched was loaded
/// from a prepared file and self-checked. Two page shards per directory
/// shard, a history spanning pages, and a shard whose pages need two
/// segments.
#[tokio::test(flavor = "multi_thread")]
async fn a_wallet_syncs_exactly_from_prepared_runtimes_with_no_worker_build() {
    for (fixture, per_shard, wallet) in [
        (
            "four shards",
            chain(),
            vec![script(1), script(2), script(3), script(999)],
        ),
        ("two page segments", oversized_chain(), oversized_wallet()),
    ] {
        let dir = tempfile::tempdir().unwrap();
        let map = publish(dir.path(), &per_shard);
        let prepared = prepare_runtimes(dir.path());
        assert!(
            prepared.files.iter().any(|f| f.table.contains("pages-1"))
                || prepared
                    .files
                    .iter()
                    .filter(|f| f.table.contains("pages"))
                    .count()
                    > 1,
            "{fixture}: page shards are prepared"
        );
        let (base, state) = serve_prepared(dir.path(), true).await;
        let outcome = run_sync(dir.path(), base, wallet.clone(), FIRST, map).await;
        compare(&outcome.ledger, &traverse(&per_shard, &wallet, 0));
        assert!(outcome.ledger.unresolved().is_empty(), "{fixture}");
        assert!(counter(&state, |m| &m.shipped_loads) > 0, "{fixture}");
        assert_eq!(counter(&state, |m| &m.shipped_fallbacks), 0, "{fixture}");
        assert_eq!(
            counter(&state, |m| &m.builds),
            0,
            "{fixture}: no worker build"
        );
    }
}

/// The same queries, sent to a worker loading prepared runtimes and to one
/// building its own, get byte-identical setup and answers, and each answer
/// decodes to the row read straight from the segment file: populated rows,
/// empty rows after them, the first and last rows, and the rows either side
/// of a native block boundary, for every directory and page segment.
#[tokio::test(flavor = "multi_thread")]
async fn prepared_and_built_runtimes_answer_identically_at_populated_empty_and_boundary_rows() {
    use base64::Engine as _;
    use transparent_shard_server::runtime::SharedParams;
    for per_shard in [chain(), oversized_chain()] {
        let dir = tempfile::tempdir().unwrap();
        publish(dir.path(), &per_shard);
        prepare_runtimes(dir.path());
        let (shipped_base, shipped) = serve_prepared(dir.path(), true).await;
        let (built_base, built) = serve_prepared(dir.path(), false).await;
        let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).unwrap();
        let client = reqwest::Client::new();
        let mut checked = 0;
        for shard in set.current() {
            for table in shard.tables() {
                let shared = SharedParams::build(shard.geometry, table.kind()).unwrap();
                let profile = &shared.profile;
                let query_rows = shard.query_rows(table, profile.rows);
                let segments = shard.segments(table);
                let mut public = Vec::new();
                for segment in 0..segments {
                    let mut setups = Vec::new();
                    for base in [&shipped_base, &built_base] {
                        let setup: serde_json::Value = client
                            .get(format!(
                                "{base}/v2/shards/{}/revisions/{}/setup/{}/{segment}",
                                shard.manifest.shard_id,
                                shard.digest,
                                table.name()
                            ))
                            .send()
                            .await
                            .unwrap()
                            .json()
                            .await
                            .unwrap();
                        setups.push(setup);
                    }
                    assert_eq!(setups[0], setups[1], "setup of {}", table.name());
                    public.push(
                        base64::engine::general_purpose::STANDARD
                            .decode(setups[0]["public_params"].as_str().unwrap())
                            .unwrap(),
                    );
                }
                // The rows each segment holds, read independently of any
                // runtime.
                let files: Vec<Vec<u8>> = (0..segments)
                    .map(|segment| {
                        std::fs::read(&shard.segment(table, segment).unwrap().path()).unwrap()
                    })
                    .collect();
                let populated = files
                    .iter()
                    .map(|rows| {
                        rows.chunks(profile.row_bytes)
                            .rposition(|row| row.iter().any(|b| *b != 0))
                            .unwrap_or(0)
                    })
                    .max()
                    .unwrap();
                let block = transparent_native::D;
                let mut rows: Vec<usize> = vec![
                    0,
                    populated,
                    populated + 1,
                    query_rows - 1,
                    block - 1,
                    block,
                ];
                rows.retain(|row| *row < query_rows);
                rows.sort_unstable();
                rows.dedup();
                for row in rows {
                    let (secret, upload) = profile.prepare(row).unwrap();
                    let mut body =
                        transparent_shard::manifest::query_binding(&shard.digest, &table.name())
                            .to_vec();
                    body.extend(upload);
                    let mut answers = Vec::new();
                    for base in [&shipped_base, &built_base] {
                        let response = client
                            .post(format!(
                                "{base}/v2/shards/{}/revisions/{}/query/{}",
                                shard.manifest.shard_id,
                                shard.digest,
                                table.name()
                            ))
                            .body(body.clone())
                            .send()
                            .await
                            .unwrap();
                        assert!(response.status().is_success(), "{}", response.status());
                        answers.push(response.bytes().await.unwrap().to_vec());
                    }
                    assert_eq!(answers[0], answers[1], "{} row {row}", table.name());
                    let each = shared.response_bytes();
                    assert_eq!(answers[0].len(), each * segments as usize);
                    for (segment, answer) in answers[0].chunks(each).enumerate() {
                        let decoded = profile
                            .decode(&secret, &public[segment], &answer[16..])
                            .unwrap();
                        let at = row * profile.row_bytes;
                        assert_eq!(
                            decoded,
                            &files[segment][at..at + profile.row_bytes],
                            "{} segment {segment} row {row}",
                            table.name()
                        );
                    }
                    checked += 1;
                }
            }
        }
        assert!(checked >= 5, "{checked} rows checked");
        assert_eq!(counter(&shipped, |m| &m.builds), 0);
        assert_eq!(counter(&shipped, |m| &m.shipped_fallbacks), 0);
        assert!(counter(&built, |m| &m.builds) > 0);
    }
}

/// A prepared file whose database differs from its segment in a single row
/// is refused by the full database comparison, whichever row the
/// self-check's query happens to sample, and the worker builds that one
/// runtime itself: the wallet still recovers exactly.
#[tokio::test(flavor = "multi_thread")]
async fn a_prepared_runtime_one_row_off_is_refused_and_the_sync_stays_exact() {
    use transparent_shard_server::runtime::disk::ShippedRuntimes;
    use transparent_shard_server::runtime::{SharedParams, TableRuntime};
    use transparent_shard_server::shardset::TableRef;
    let dir = tempfile::tempdir().unwrap();
    let per_shard = chain();
    let map = publish(dir.path(), &per_shard);
    prepare_runtimes(dir.path());
    let set = ShardSet::open(dir.path(), DEFAULT_RETAIN_REVISIONS).unwrap();
    // The first shard's directory, which the sync below queries.
    let shard = set.get(0).unwrap();
    let shared = SharedParams::build(
        shard.geometry,
        transparent_shard_server::shardset::Table::Directory,
    )
    .unwrap();
    let source = shard.segment(TableRef::Directory, 0).unwrap();
    let rows = std::fs::read(source.path()).unwrap();
    let row_bytes = shared.profile.row_bytes;
    let changed = rows
        .chunks(row_bytes)
        .position(|row| row.iter().any(|b| *b != 0))
        .unwrap();
    let mut one_off = rows.clone();
    one_off[changed * row_bytes + 3] ^= 0x10;
    let forged = TableRuntime::build(&shared, &one_off).unwrap();
    // Its own rows decode under it: only comparing the whole database with
    // the segment can tell, and it does at every sampled row.
    forged.self_check(&shared, &one_off).unwrap();
    for _ in 0..16 {
        let refused = forged.self_check(&shared, &rows).unwrap_err();
        assert!(refused.contains("differs from the segment"), "{refused}");
    }
    let key = (
        shard.digest.clone(),
        transparent_shard_server::shardset::Table::Directory,
        0,
    );
    ShippedRuntimes::new(dir.path().to_path_buf())
        .write(&key, &shared, source.sha256(), &forged)
        .unwrap();
    let (base, state) = serve_prepared(dir.path(), true).await;
    let wallet = vec![script(1), script(2), script(3), script(999)];
    let outcome = run_sync(dir.path(), base, wallet.clone(), FIRST, map).await;
    compare(&outcome.ledger, &traverse(&per_shard, &wallet, 0));
    assert_eq!(counter(&state, |m| &m.shipped_fallbacks), 1);
    assert_eq!(
        counter(&state, |m| &m.builds),
        1,
        "only the refused runtime is built"
    );
}
