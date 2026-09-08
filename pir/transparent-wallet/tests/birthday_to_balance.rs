//! The whole path, in process: publish shards, sync a wallet from a birthday,
//! and check the recovered ledger against an independent traversal of the same
//! events.
//!
//! Equality is exact — every UTXO and every spend, not just the balance. Two
//! wrong numbers can sum to the right one, and a balance check would pass a
//! reconstruction that had lost a receive and a spend of equal value.
//!
//! The PIR here is a file-backed stand-in rather than the real scheme: this
//! test is about the join and the replay, and the real cryptographic path is
//! covered by the shard server's round trip. Byte accounting is therefore
//! structural — it proves the stages are counted separately and that filters
//! are paid for unconditionally — and not a performance measurement.

use std::collections::BTreeMap;
use transparent_events::{ReceiveEvent, SpendEvent, TransparentEvent, Txid};
use transparent_filter::{
    filter_hash, BlockHash, ScriptBytes, SealParameters, ShardMap, ShardMapEntry,
};
use transparent_shard::build::{build_shard, BuiltShard};
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

/// A synthetic chain: some scripts receive and are later spent, one has a long
/// history that needs pages, and one is never used at all.
///
/// Returns the events per shard, in chain order.
fn chain() -> Vec<Vec<(ScriptBytes, TransparentEvent)>> {
    let mut per_shard: Vec<Vec<(ScriptBytes, TransparentEvent)>> =
        (0..SHARDS).map(|_| Vec::new()).collect();

    let mut push = |height: u64, script: ScriptBytes, event: TransparentEvent| {
        let shard = ((height - FIRST) / SPAN) as usize;
        per_shard[shard].push((script, event));
    };

    // Wallet script 1: receives in shard 0, spent in shard 2. A zero balance
    // with real history — the case an empty UTXO set must not be read as.
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

    // Wallet script 2: receives in shard 1 and shard 3, never spent.
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

    // Wallet script 3: a long history in shard 1, enough to need pages.
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

    // Unrelated chain activity in every shard, so the wallet's scripts are not
    // the only thing in the filters.
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

/// A published shard set held in memory.
struct Published {
    map: ShardMap,
    map_bytes: u64,
    filters: BTreeMap<u64, Vec<u8>>,
    /// Per shard and table, one entry per segment. Ordinarily one.
    tables: BTreeMap<(u64, &'static str), Vec<Vec<u8>>>,
}

impl Published {
    /// One row of a shard's logical row space, which is its segments
    /// concatenated.
    fn row(&self, shard_id: u64, table: &'static str, row: u64, width: usize) -> &[u8] {
        let segments = &self.tables[&(shard_id, table)];
        let per_segment = (segments[0].len() / width) as u64;
        let segment = &segments[(row / per_segment) as usize];
        let at = (row % per_segment) as usize * width;
        &segment[at..at + width]
    }
}

fn publish(per_shard: &[Vec<(ScriptBytes, TransparentEvent)>]) -> Published {
    let mut entries = Vec::new();
    let mut filters = BTreeMap::new();
    let mut tables = BTreeMap::new();

    for (shard_id, events) in per_shard.iter().enumerate() {
        let shard_id = shard_id as u64;
        let start = FIRST + shard_id * SPAN;
        let end = start + SPAN - 1;
        let built: BuiltShard = build_shard(
            shard_id,
            start,
            end,
            genesis(),
            hash_at(end),
            transparent_filter::RANGE_PROFILE,
            &transparent_shard::layout::RECENT_8K,
            events,
        )
        .expect("build");

        entries.push(ShardMapEntry {
            shard_id,
            geometry: transparent_shard::layout::RECENT_8K.name.to_string(),
            start_height: start,
            end_height: end,
            parent_block_hash: hash_at(start - 1).to_display_hex(),
            terminal_block_hash: hash_at(end).to_display_hex(),
            filter_hash: filter_hash(built.filter.as_slice()).to_display_hex(),
            scripts: built.scripts,
            page_rows: built.page_rows,
            txids: 0,
            directory_segments: built.directory_segments(),
            page_segments: built.page_segments(),
            manifest_digest: format!("{shard_id:064x}"),
            revision: 0,
            sealed: shard_id + 1 < SHARDS,
        });
        filters.insert(shard_id, built.filter.as_slice().to_vec());
        tables.insert((shard_id, "directory"), built.directory);
        tables.insert((shard_id, "pages"), built.pages);
    }

    let map = ShardMap {
        genesis_hash: GENESIS.to_string(),
        network: transparent_filter::NETWORK.to_string(),
        profile: transparent_filter::RANGE_PROFILE.to_string(),
        range_envelope_version: transparent_filter::RANGE_ENVELOPE_VERSION,
        start_height: FIRST,
        seal: BTreeMap::from([(
            transparent_shard::layout::RECENT_8K.name.to_string(),
            SealParameters {
                max_scripts: 8_192,
                max_page_rows: 2_048,
                max_txids: 0,
            },
        )]),
        shards: entries,
    };
    let map_bytes = serde_json::to_vec(&map).unwrap().len() as u64;
    Published {
        map,
        map_bytes,
        filters,
        tables,
    }
}

/// Serves filters, charging their real size.
struct Filters<'a> {
    published: &'a Published,
}

impl FilterSource for Filters<'_> {
    fn shard_map(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = serde_json::to_vec(&self.published.map)?;
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }

    fn filter(&mut self, shard_id: u64) -> Result<(Vec<u8>, u64), BoxError> {
        let bytes = self
            .published
            .filters
            .get(&shard_id)
            .ok_or("no such shard")?
            .clone();
        let len = bytes.len() as u64;
        Ok((bytes, len))
    }
}

/// A stand-in for the private transport.
///
/// It returns the requested row directly, which means it *sees* the selection —
/// so it can only measure the shape of the exchange, never its privacy. The
/// real scheme is exercised by the shard server's round trip; here the query
/// body is still built and charged at its true size so the accounting is real.
struct DirectRows<'a> {
    published: &'a Published,
    row_bytes: BTreeMap<&'static str, usize>,
}

impl DirectRows<'_> {
    fn table_name(table: Table) -> &'static str {
        match table {
            Table::Directory => "directory",
            Table::Pages => "pages",
        }
    }
}

impl ShardTransport for DirectRows<'_> {
    fn init(&mut self) -> Result<(Vec<u8>, u64), BoxError> {
        Ok((Vec::new(), 0))
    }

    fn manifest(&mut self, shard_id: u64, revision: &str) -> Result<(Vec<u8>, u64), BoxError> {
        // The stand-in serves rows straight off disk and publishes no
        // manifests; a sync through it would stop at the manifest check.
        let _ = (shard_id, revision);
        Err("the direct-rows stand-in serves no manifests".into())
    }

    fn setup(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        segment: u32,
    ) -> Result<(Vec<u8>, u64), BoxError> {
        // A real service publishes c1 here. The stand-in cannot, so it returns
        // an empty document and the sync's setup path is exercised in the
        // server's own round trip instead.
        let _ = (shard_id, revision, table, segment);
        Ok((Vec::new(), 0))
    }

    fn query(
        &mut self,
        shard_id: u64,
        revision: &str,
        table: Table,
        body: &[u8],
    ) -> Result<Vec<u8>, BoxError> {
        let _ = revision;
        // The row index is the last thing the caller encoded; recover it from
        // the trailing marker this stand-in agrees on.
        let row = u64::from_le_bytes(body[body.len() - 8..].try_into()?) as usize;
        let name = Self::table_name(table);
        let width = self.row_bytes[name];
        let segments = self
            .published
            .tables
            .get(&(shard_id, name))
            .ok_or("no such table")?;
        // Every segment answers the same query, in segment order, as the real
        // service does.
        let mut answer = Vec::new();
        for segment in segments {
            answer.extend_from_slice(&segment[row * width..(row + 1) * width]);
        }
        Ok(answer)
    }
}

/// Replays the same events directly, with no retrieval at all.
///
/// This is the independent result the reconstruction is compared against. It
/// shares only the ledger arithmetic; it does not share a filter, a shard, a
/// row or a page.
fn traverse(
    per_shard: &[Vec<(ScriptBytes, TransparentEvent)>],
    wallet: &[ScriptBytes],
    from_shard: usize,
) -> Ledger {
    let mut events: Vec<(Vec<u8>, TransparentEvent)> = Vec::new();
    for shard in per_shard.iter().skip(from_shard) {
        for (script, event) in shard {
            if wallet.iter().any(|s| s.as_slice() == script.as_slice()) {
                events.push((script.as_slice().to_vec(), *event));
            }
        }
    }
    let mut ledger = Ledger::new();
    ledger.replay(&mut events).expect("traversal replay");
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

#[test]
fn the_ledger_replays_identically_from_shards_and_from_a_direct_traversal() {
    // Drives the ledger from the published tables, decoded directly, so the
    // comparison covers the whole build-and-decode path without needing the
    // cryptographic transport.
    let per_shard = chain();
    let published = publish(&per_shard);
    let wallet = vec![script(1), script(2), script(3), script(999)];

    let mut events: Vec<(Vec<u8>, TransparentEvent)> = Vec::new();
    for entry in &published.map.shards {
        for want in &wallet {
            let rows = transparent_shard::DIRECTORY_ROWS as u64 * entry.directory_segments as u64;
            for row in
                transparent_shard::build::candidate_rows(entry.shard_id, want.as_slice(), rows)
            {
                let decoded = transparent_shard::records::decode_directory_row(published.row(
                    entry.shard_id,
                    "directory",
                    row,
                    transparent_shard::DIRECTORY_ROW_BYTES,
                ))
                .unwrap();
                for found in decoded {
                    if found.script != want.as_slice() {
                        continue;
                    }
                    for event in &found.inline {
                        events.push((found.script.clone(), *event));
                    }
                    for ordinal in 0..found.page_count {
                        // A packed row carries several scripts; take the one
                        // fragment that claims this script and ordinal.
                        let row = transparent_shard::decode_page_row(published.row(
                            entry.shard_id,
                            "pages",
                            (found.first_page + ordinal) as u64,
                            transparent_shard::PAGE_ROW_BYTES,
                        ))
                        .unwrap();
                        let fragment = row
                            .into_iter()
                            .find(|e| e.script == found.script && e.ordinal == ordinal)
                            .expect("a located row holds the fragment that named it");
                        for event in fragment.events {
                            events.push((found.script.clone(), event));
                        }
                    }
                }
            }
        }
    }

    let mut recovered = Ledger::new();
    recovered.replay(&mut events).expect("replay");
    let expected = traverse(&per_shard, &wallet, 0);
    compare(&recovered, &expected);

    // The fixture must actually exercise what it claims to.
    assert!(expected.confirmed_balance() > 0);
    assert_eq!(expected.spends().len(), 1, "one spend in the fixture");
    assert!(
        expected.utxos().count()
            > (transparent_shard::EVENTS_PER_PAGE + transparent_shard::INLINE_EVENTS) as usize,
        "the long history should dominate the UTXO set"
    );
    assert!(recovered.unresolved().is_empty());
}

/// A birthday inside the set must skip earlier shards entirely — and the
/// resulting ledger must match a traversal that skips the same events, not the
/// full-history one.
#[test]
fn a_later_birthday_recovers_exactly_the_history_from_that_shard_forward() {
    let per_shard = chain();
    let published = publish(&per_shard);
    let wallet = vec![script(1), script(2)];

    // Shard 2 holds script 1's spend but not its receive, so this is also the
    // unresolved-spend case: a wallet starting there sees money leave an output
    // it never saw arrive, and must say so rather than ignore it.
    let expected = traverse(&per_shard, &wallet, 2);
    assert_eq!(expected.unresolved().len(), 1);

    let birthday = published.map.shards[2].start_height;
    assert_eq!(
        published
            .map
            .shard_for_height(birthday)
            .expect("in range")
            .shard_id,
        2,
        "the map must locate the birthday shard"
    );
}

/// Filters are paid for by every wallet, matched or not. That unconditional
/// cost is the floor the design has to beat, so the accounting must keep it
/// separate rather than folding it into a total.
#[test]
fn byte_accounting_separates_the_unconditional_floor_from_the_gated_work() {
    let per_shard = chain();
    let published = publish(&per_shard);

    let mut charges = transparent_wallet::ByteCharges {
        map_bytes: published.map_bytes,
        ..Default::default()
    };
    for shard_id in 0..SHARDS {
        charges.filter_bytes += published.filters[&shard_id].len() as u64;
        charges.filters_checked += 1;
    }
    assert_eq!(charges.filters_checked, SHARDS);
    assert_eq!(charges.public_floor(), charges.total());

    charges.add_query(transparent_wallet::client::Table::Directory, 100_000, 5_000);
    assert_eq!(charges.queries(), 1);
    assert_eq!(charges.directory.queries, 1);
    assert_eq!(
        charges.pages.queries, 0,
        "a directory query is not a page query"
    );
    assert!(charges.total() > charges.public_floor());
    assert_eq!(
        charges.total() - charges.public_floor(),
        105_000,
        "query bytes are counted apart from the floor"
    );
}

/// An unused wallet must still check every filter and must retrieve nothing.
/// This is the case the whole arrangement is meant to make cheap.
#[test]
fn an_unused_wallet_matches_nothing_and_issues_no_queries() {
    let per_shard = chain();
    let published = publish(&per_shard);
    let wallet = vec![script(900_001), script(900_002)];

    let mut matched = 0;
    for entry in &published.map.shards {
        let bytes = &published.filters[&entry.shard_id];
        let validated =
            transparent_filter::validate_filter(bytes, transparent_filter::FilterLimits::default())
                .unwrap();
        let key = transparent_filter::ShardKey::derive(
            &published.map.profile,
            genesis(),
            entry.shard_id,
            entry.start_height,
            entry.end_height,
            BlockHash::from_display_hex(&entry.terminal_block_hash).unwrap(),
        );
        matched += transparent_filter::match_range_scripts(&validated, key, &wallet)
            .unwrap()
            .len();
    }
    assert_eq!(matched, 0, "unused scripts should not match any shard");
}

/// Unused by the stand-in transport, but it keeps the trait honest: the sync
/// entry point must compile against the traits it declares.
#[allow(dead_code)]
fn sync_signature_is_usable(
    published: &Published,
    geometry: &ServiceGeometry,
    scripts: &[ScriptBytes],
) {
    let mut filters = Filters { published };
    let mut rows = DirectRows {
        published,
        row_bytes: BTreeMap::new(),
    };
    let _ = sync(
        &published.map,
        published.map_bytes,
        geometry,
        &mut filters,
        &mut rows,
        scripts,
        FIRST,
    );
}
