# Implementation plan: content-sealed shards for transparent PIR

Date: 2026-09-05. Status: implementation plan for a POC, not a deployed protocol.

This plans a proof of concept for the design in
[the updated recommendation](transparent_pir_updated.md). It answers that
document's open question — what a practical shard is — with a concrete,
buildable shape, and states what the POC would have to measure before any of it
could be believed.

## Why

The deployed implementation publishes one filter per *block*
(`pir/transparent-filter`), and the retrieval service serves one bounded-range
generation at a time (`server/transparent-history-server`). That is the shape
the [mainnet study](transparent_pir_mainnet_study.md) found wanting: probing
unused scripts directly is expensive — 100 unused scripts cost 11.2 MB of PIR —
and [the incremental prototype](transparent_pir_incremental.md) records that
heavy histories and catch-up still fail against a 2.89 MB/day compact-scanning
baseline.

The measured evidence already points at range filters. For the 1,152-block
sample, aggregating filter intervals collapses total filter bytes:

| Interval width | Filter bytes |
|---|---:|
| 1 block | 224,380 |
| 16 blocks | 100,129 |
| 128 blocks | 68,122 |
| 1,024 blocks | 50,499 |

Extrapolated, shards of roughly 1,024 blocks cover Ironwood activation
(3,428,143) to tip in about **1.3 MB of filters**, against roughly **108 MB**
for coverage-matched compact scanning of the same range. Whether that margin
survives paying for the private retrieval it gates is exactly what the POC is
for. It is a screening estimate from measured components, not a result.

## What the POC must produce

A wallet with a birthday at Ironwood activation downloads every range filter,
matches its scripts locally, privately retrieves history only from the shards
that matched, and reconstructs a UTXO set that is **exactly** equal to an
independent traversal of the same chain events — with per-stage byte accounting
good enough to compare against compact scanning.

## What it does not do

Publicly selecting a temporal shard reveals the queried chain range, and
querying a shard only after a filter hit tells the server there is probable
activity in that range. This is the leak named in
[§Timing and shard-selection privacy](transparent_pir_updated.md). The POC
accepts it. No result from this work may be described as hiding whether the
wallet had activity.

PIR query privacy also does not prove the index is complete. A filter negative
advances coverage only under the trusted-indexer assumption, unchanged here.

## Scope

- **Coverage:** Ironwood activation (3,428,143) through tip.
- **Implementation:** Rust throughout. No new Python harness code.
- **Shard boundaries:** content-sealed, published as protocol data.
- **In scope:** byte accounting and exact replay equality; range filters served
  by `transparent-filter-server`; the optional private transaction-detail table.
- **Not yet served:** `GET /v1/filters/shards` and `/v1/filters/shards/range`
  are specified below and validated by the client, but the filter server does
  not expose them; the only `FilterSource` implementations are in tests. Shard
  filter distribution is designed, not shipped.
- **Out of scope:** bounded evaluation-key reuse (`ipir-sp`'s
  `experimental-key-reuse` `QueryPool`). The plan notes where it would land.

## Three findings that shape the design

### 1. The Golomb-Rice parameter does not need retuning

In BIP 158 an element is mapped into `[0, N*M)`, so the per-tested-element
false-positive rate is `1/M` — about 1.27e-6 — *independently of the element
count*. A range filter holding 12,000 scripts has the same per-query
false-positive rate as a one-block filter holding twelve. With 40 shards and 100
wallet scripts, expected false-positive shard selections are
`4000/784931`, about 0.005.

So `P = 19` and `M = 784_931` carry over unchanged from
`pir/transparent-filter/src/profile.rs`. Only the *keying* changes. Filter
*bytes* still grow with the element count, at roughly `(P+1)/8` bytes per
element; that growth is what cross-block deduplication pays for, and it is what
the interval table above measures.

### 2. Uniform geometry gives one shared parameter set

`ipir_sp::params_for_simplepir(rows, item_size_bits)` is a pure function of
geometry. If every shard pins the same `rows` and `row_bytes`, all shards share
one `YpirSchemeParams`, and a client validates parameters and derives its query
setup **once** for every shard it will ever query.

The published `c1` still differs per shard, because it is derived from the
shard's own database, so setup download remains per-shard. But query
construction, packing keys and parameter validation do not. This is the largest
cost lever available, and it is the reason the next finding matters.

### 3. Content-sealed shards, with segments, make that lever unconditional

Pinned geometry only works if no table overflows it. Nothing bounds what a
*height* range contains, so height sharding must size its tables from expected
occupancy and fail closed when a spam burst exceeds it. Sealing on content
removes that for every ordinary range: a shard seals at the block boundary
before it would overflow.

Sealing cannot save the case where one block's own content exceeds a table, and
failing closed there would let a single purchased block halt publication. A
shard therefore holds one or more **segments** of the pinned geometry, and the
builder adds a segment when placement needs one. Ordinary shards have exactly
one segment and are unaffected; the geometry, and therefore the shared parameter
set, is the same either way.

One criterion is not enough, because the three tables are keyed differently:

| Table | Sized by | Why event count alone does not bound it |
|---|---|---|
| filter | distinct scripts | — |
| directory | distinct scripts | — |
| pages | `sum of ceil((n_script - 2) / 186)` | pages are per-script and padded. Many scripts with three events each is the bad case: about 21,845 pages at 65,536 events, against a few hundred expected — a 100x spread |
| transaction detail | distinct txids | — |

So seal on **whichever constraint binds first**, evaluated at a block boundary:

```text
seal shard when  distinct_scripts >= S
              or page_rows       >= P
              or distinct_txids  >= T
```

`S`, `P` and `T` are per-segment capacities, so the predicate is unchanged for
ordinary shards; a block that breaches them on its own is absorbed and the
builder gives that shard a second segment. All three quantities are monotone as
blocks stream in, so this is one incremental pass over the event journal.
Snapping the seal to a block boundary
preserves parent and terminal block-hash binding and leaves reorg handling
unchanged. Shards end up as wide as they can be without overflowing any table.

### Boundaries are published, not re-derived

Content-derived boundaries depend on the indexer's event set. Two operators
disagreeing about a single event would produce different boundaries, and the
disagreement would cascade into every later shard — destroying the cross-operator
digest comparison that `/v1/filters/digests` exists to support, because no two
shards would be comparable at all.

Publishing the height-to-shard map as committed protocol data fixes this. An
operator must reproduce the published boundaries rather than derive its own, so
a disagreement stays localized to one shard's digests, exactly as it would under
height sharding. The same map is what a wallet binary-searches to turn its
birthday height into a first shard.

Two consequences to accept explicitly. `S`, `P` and `T` become schema: changing
them re-shards, and since sealed shards are immutable, new values can in
practice apply only to new ranges. And shard density becomes public — which is
public chain data already, but it should be stated rather than discovered.

## Work

### Phase 0 — Event collection

`extract_elements` in `server/transparent-filter-server/src/extract.rs` already
resolves every non-coinbase input's previous output against Zakura and then
discards the structure, returning deduplicated scripts. The events the PIR
tables need are the thrown-away half of work already being done.

- Add `extract_events`, emitting the `ReceiveEvent` and `SpendEvent` shapes of
  [§Canonical event model](transparent_pir_updated.md), with the stable
  identities `(txid, output_index)` and
  `(spending_txid, input_index, spent_txid, spent_output_index)`.
- Re-express `extract_elements` as a projection over `extract_events`, so a
  filter's element set and the events' keying cannot disagree. A script present
  in the events but absent from its filter is a silent coverage hole.
- Add an append-only `EventStore` beside `FilterStore`, following the same
  crash-safe discipline: content file, fixed-width index, and a small
  `checkpoint.bin` of committed lengths, with rollback truncating the height
  index only.
- Add a standalone `transparent-event-ingest` binary that backfills
  3,428,143 through tip into its own data directory.

Prevout resolution needs Zakura RPC, which is reachable only on the node host,
so the backfill runs there. It is designed to touch nothing the running service
owns: separate binary, separate data directory, read-only against Zakura.

### Phase 1 — Range filters

`build_filter` derives its SipHash keys from a block hash, and its own
documentation forbids merging filters for exactly that reason. This is a new
profile, not a mutation of the existing one.

- Introduce a `FilterKeys` newtype with two producers: the existing
  `BlockHash::filter_keys`, and a new `ShardKey::filter_keys` derived as
  `SHA256(RANGE_PROFILE || genesis || shard_id || start_height || end_height || terminal_block_hash)`,
  truncated to sixteen bytes. Domain separation is what stops a range filter
  validating as a block filter. The shard id is part of the key, so a filter
  cannot be replayed under a different shard identity.
- Key `build_filter` and `matching` on `FilterKeys`: one encoder, two keyings.
  The BIP 158 test vectors and the btcd cross-check keep pinning the block path,
  so the shared encoder stays honest.
- Add `RANGE_PROFILE = "zcash-transparent-range-v1"` and the seal parameters.
  There is deliberately no shard-width constant; widths come from the map.
- Add a `ZTRB` range envelope alongside `ZTFB` rather than bumping the envelope
  version. The per-block envelope is deployed and pinned by a golden fixture,
  and must stay byte-stable. Records carry shard id, start and end heights,
  parent and terminal block hashes, and the filter.
- Replace what per-height contiguity was providing. `check_batch` currently
  enforces exactly one record per height against the wallet's accepted chain; a
  range filter loses that, and content sealing means a wallet cannot recompute
  boundaries arithmetically either. A new `check_range_batch` must instead
  require that each record matches the published shard map the wallet fetched
  and bound by digest; that both parent and terminal block hashes are on the
  wallet's accepted chain at the stated heights; and that shard `i`'s parent
  hash equals shard `i-1`'s terminal hash, so the shard chain is gapless from
  the start height to the tip. The gapless check is what stops an operator
  quietly omitting a range now that widths vary.
- Add `sync_shards` beside `sync_range`. As with `sync_range`, the wallet
  requests the whole contiguous shard span and never only the shards it matched.

### Phase 2 — Range filters and the shard map on the wire

- Store range filters keyed by shard id, reusing the existing filter-store file
  discipline. A sealed shard's filter is immutable; the tail is rebuilt until it
  seals.
- Add endpoints beside the per-block ones, which stay untouched and deployed:
  `GET /v1/filters/shards` for the published height-to-shard map — shard id,
  start and end heights, parent and terminal hashes, filter digest, occupancy
  counts, sealed flag — and `GET /v1/filters/shards/range` for a `ZTRB` batch.
- Extend the service info shape with the range profile, the seal parameters and
  the shard count.

### Phase 3 — Shard builder

Two responsibilities: decide seal boundaries, and build the tables.

Sealing streams the event journal in height order, maintaining distinct scripts,
incremental page rows and distinct txids, and seals at the first block boundary
where any threshold is met. `S`, `P` and `T` are chosen from a first pass over
the collected Ironwood-to-tip events so shards land near 1,024 blocks at current
density — about 12,000 scripts and 63,000 events — while every table's pinned
geometry holds with margin. The realized occupancy distribution is the evidence
for the chosen thresholds, and it replaces the estimate-based sizing a height
scheme would need.

Tables use pinned geometry, identical for every shard and every segment: a
3,584-byte directory row with two inline events and two independently salted
candidate buckets, a 17,920-byte page row at 186 events per page, and a
transaction-detail table sized from the measured per-shard txid distribution.

Placement is a loop, not a fail-closed check: build at one segment, and if any
script cannot be placed or the pages do not fit, rebuild at one more. A fresh
segment's rows are empty, so the loop terminates, and the smallest count that
works is the published one. Rows are addressed over the shard's whole logical
row space, `rows * segment_count`, so occupancy stays even instead of spilling
into a last segment. Overflow must still never trigger a public script, address,
outpoint or transaction lookup — adding a segment is the private fallback that
replaces failing.

A shard's pages are one contiguous space, its segments' page tables concatenated
in order, so a directory entry stays a `(first_page, page_count)` extent whether
or not the extent crosses a segment.

Every record carries exact script bytes. A shard's manifest binds network,
genesis, schema and profile versions, seal parameters, heights, parent and
terminal hashes, the filter digest, its lifecycle state and tail revision, and
each segment's table geometry and digest. Shard
identity is the digest of the canonical manifest, and the published directory
must be named by that digest, so a manifest edited after publication is refused
rather than served under the identity of what it replaced.

### Phase 4 — Sharded retrieval service

Generalize the retrieval service from one generation to an ordered map of
shards, with routes for the shard index, a shared-parameter init, and a query
addressed by shard and table. Keep the per-table setup-seed separation and add
the shard id to the query prefix, so a query decoded against the wrong shard
fails the client's own check rather than returning plausible nonsense.

A multi-segment shard is served by fan-out: one client request per shard and
table, evaluated against every segment, answered with the per-segment results in
segment order. The client must not name a segment, because the segment a script
lands in is a function of the script. Setup is per segment, since `c1` is
derived from each segment's own database.

This phase carries the plan's main risk. The current table runtime holds an
`IPIRServer`, its packing preprocessing and its key images resident, and leaks
its RLWE parameters on the explicit assumption that one generation is loaded
once at startup and never replaced. Forty shards times three tables will not fit
that assumption: the reuse measurements record packing-cache payloads between
100 MB and 2 GB *per geometry*. Mitigation, in order:

1. Reuse the existing on-disk artifact cache and its metadata guard rather than
   inventing one. Sealed shards are immutable, so artifacts are built once.
2. Load shard runtimes lazily behind a bounded cache, and share one set of RLWE
   parameters across shards — legitimate precisely because the geometry is
   uniform, which is what content sealing guarantees for every shard rather than
   most.
3. Measure resident bytes per loaded shard as soon as one shard exists, and let
   that number decide the cache bound and whether the transaction-detail table
   ships at all. A multi-segment shard multiplies that number by its segment
   count, so the bound is over segments, not shards.

If the first two do not bring a shard's resident cost into budget, report the
measured number rather than shipping a service that cannot hold its fleet.

### Phase 5 — Wallet client

The join between filters and private retrieval does not exist in Rust today; it
exists only in the Python harness. This is that join, and the deliverable a
wallet adapter would eventually consume.

Given a birthday height, a script set and an accepted chain: fetch and validate
the shard map and binary-search it for the birthday; download every range filter
from that shard to the tip and validate chain binding, gaplessness and digests;
match locally; for each matched script and shard, privately retrieve both
candidate directory rows from every segment, validate exact script identity, and
privately retrieve the pages the decoded extent locates; privately retrieve
transaction detail for
each discovered txid from the same shard; replay events into an outpoint-indexed
map; and commit ledger state and per-shard coverage atomically, only once every
segment of that shard has been processed.

A tail shard is published as an immutable revision. Coverage taken from one is
provisional: it is stored with the revision digest that produced it and
re-derived when a later revision or the sealed shard appears. A wallet must not
merge a newer revision into an older one's results.

Filters come from the filter service and private queries from the retrieval
service. They must stay separate: a wallet must not learn to fetch public bytes
from the same place it makes private requests, or the two become correlated by
construction.

Page locators must never appear in a plaintext request. An unexplained spend of
an unknown receive is unresolved work and must fail, not be skipped. Budget
exhaustion, a timeout or a malformed response leaves coverage unadvanced.

Byte accounting is per stage — filters, shard map, per-shard setup, directory,
pages, transaction detail — separating upload from download, and charging each
shard's published parameters once per sync rather than once per query.

### Phase 6 — Verification

- **Replay equality.** A reference traversal over the event journal, independent
  of the shard tables, producing the event multiset and UTXO set for each
  synthetic wallet. Require exact event and UTXO equality, not equal balances.
- **Seal determinism.** Rebuilding from the same journal reproduces the
  published boundaries byte for byte, and a build that disagrees with the
  published map is refused rather than published under new boundaries.
- **Availability.** No valid block is ever rejected. A synthetic block whose own
  content exceeds a segment's capacity produces a multi-segment shard, and a
  wallet syncing across it recovers exactly the same ledger, at the segment
  multiple of the bytes.
- **Tail revisions.** A tail is published, extended and sealed; a wallet holding
  the earlier revision re-derives that range rather than merging, and its final
  ledger is unchanged.
- **Cross-shard refusal.** A query built for one shard is refused by another,
  mirroring the existing cross-generation and cross-table refusal tests.
- **Filter tests.** A range-filter golden fixture derived from the spec rather
  than printed from the encoder; a range filter must not validate under block
  keying; a gap in the shard chain must be rejected; and a false-positive count
  over synthetic absent scripts across all shards.
- **Workload sweep.** The study's six shapes — 20 and 100 unused scripts, ten
  small histories, ten median histories, one very large history, and that
  history with a cached prefix — now against a birthday-to-tip restoration.
- **The headline comparison.** Total bytes for a full Ironwood-to-tip
  restoration against the coverage-matched compact-scanning equivalent, broken
  down by stage.

## Sequencing

Phase 0 gates both the realistic measurement and the seal parameters, so it is
the long pole: `S`, `P` and `T` cannot be chosen without the full event set.
Phases 1, 4 and 5 can be built and tested against synthetic shards while the
backfill runs. Phase 4's memory question should be answered with a real
measurement as soon as Phase 3 emits one shard, because it can invalidate the
transaction-detail table.

## What would make this fail

Worth stating in advance, so the POC is capable of returning a negative result:

- Per-shard published setup, multiplied across the shards a restoring wallet
  touches, could exceed what the filter gating saves.
- A wallet whose scripts are active in many shards pays a directory query per
  matched pair; the study's measured cost is about 112 KB per query per
  candidate bucket, and two candidates double it.
- Resident server memory per shard could make a forty-shard fleet impractical on
  the hardware the service actually runs on. A multi-segment shard costs its
  segment count in both memory and per-query work.
- Tail-revision churn could dominate a wallet that syncs often, because each
  superseding revision re-charges the range it re-covers.

The architecture succeeds only when public filters, per-shard setup, directory
queries, page queries, optional transaction-detail queries, retries and cover
traffic together cost less than coverage-matched compact scanning.
