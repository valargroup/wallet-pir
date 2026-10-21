# Transparent txid display PIR: server contract and qualification

A wallet opening a transparent transaction privately fetches one fixed-size
display entry by txid. The entry gives who funded the transaction, where up to
two of its outputs went, the fee, and what the entry leaves out. This page is
the server contract, the lookup protocol and the reproducible demo.

Source and local qualification do not establish deployment or production
capacity. Executable milestone tracking belongs in
[remaining work](../transparent/remaining-work.md).

## Scope

Display v2 (`transparent-txid-display-v2`) replaces the variable-length v1
record, which listed every output and no sender. v2 covers the regular cases
whole and names everything else as an omission:

- a regular send: one recipient plus shielded change, from one address or
  several addresses of one wallet;
- the zcashd style: a recipient and transparent change;
- shielding (transparent to shielded) and unshielding (shielded to one
  transparent output);
- coinbase.

Usage over the six months before the change, from Roman: the median
transaction has one distinct input address and one output address. At the
90th percentile it has one input address and two output addresses.

## Published facts

Every entry is 113 bytes, so every lookup costs the same.

| Field | Bytes | Meaning |
|---|---:|---|
| Tag | 16 | `SHA-256("transparent-txid-display/tag/v2" ‖ txid)[..16]`, txid in internal byte order. All zero marks an empty slot |
| Flags | 2 | Facts and omissions, below. Unknown bits are refused |
| Fee | 8 | Exact whole-transaction fee in zatoshis; zero for coinbase |
| Input count | 4 | Transparent inputs; zero for coinbase |
| Output count | 4 | Every transparent output, including omitted ones |
| Source | 21 | Kind and hash of the first input, in input order, whose **spent** script is P2PKH or P2SH |
| Output 0, output 1 | 29 each | Value, then kind and hash, of the transaction's first two outputs |

Integers are little-endian. Address kinds:
- 0: absent;
- 1: P2PKH;
- 2: P2SH;
- 3: present without an address (P2PK, OP_RETURN, empty or anything else).

For kinds 0 and 3 the hash is zero. An output of kind 3 still carries its
value.

| Flag bit | Meaning |
|---:|---|
| 0 | Coinbase |
| 1 | Has shielded components (any Sprout, Sapling, Orchard or Ironwood component) |
| 2 | Omission: the inputs spend two or more distinct scripts; only the first address-shaped one is given |
| 3 | Omission: more than two outputs; outputs 0 and 1 are given and the count says how many exist |
| 4 | Omission: there are transparent inputs and the shielded pools also contributed net value |

The decoder refuses any entry the encoder would refuse:
- a source is present exactly when there are inputs;
- output slots match `min(output count, 2)`;
- a coinbase entry has no inputs and a zero fee;
- a non-coinbase entry with no inputs has shielded components;
- values stay within `MAX_MONEY`;
- bit 3 agrees with the output count.

| Case | Entry | A wallet shows |
|---|---|---|
| Regular send, shielded change | Source, one output, shielded bit | From, to, amount, fee |
| Several addresses of one wallet | Source and bit 2 | First address; the wallet knows it owns every input |
| zcashd send | Source, two outputs | From, to, change, fee |
| Shielding | Source, no outputs, shielded bit | Shielded from the source |
| Unshielding | No inputs, shielded bit, one or two outputs | From shielded funds |
| Coinbase | Coinbase bit | Mined |
| Exotic | Bit 2 from another wallet, bit 3, bit 4, or a kind-3 slot | Known facts, the named omission, and an explicit public enhancement |

**What the server cannot see.** It cannot tell whether inputs come from
different wallets; it reports only distinct spent scripts. The wallet decides:
- it owns every input: a regular send;
- it owns some but not all inputs: shared funding;
- it owns none: it shows the first source.

Whether a change output was meant to be transparent is not knowable from the
chain and is not encoded.

**Trust.** The entry is a trusted publisher's assertion, like the v1 fee.
- The publisher resolves every input's spent output (script and value) to
  compute the fee and to key spend events. The source and flag 2 come from the
  same resolved outputs (`extract_block`, `transparent-ingest/src/extract.rs`).
- A wallet cannot check a foreign source without the parent transaction. It
  checks the entry only against the inputs and outputs it owns.
- A shielded-only transaction, with neither transparent inputs nor outputs,
  has no entry.

## Source records and durability

A display journal keeps each transaction's **source record**
(`transparent-txid-display-v2x`), not its published entry:
- the v1 record body: whole-transaction metadata and every transparent output
  with its raw script;
- every transparent input in transaction order, with its prevout txid and
  index and the resolved value and raw locking script of the output it spends.
  A coinbase lists no inputs.

Entries are derived from source records when the journal is read
(`TransparentDisplayRecordV2x::display_record`). A change to what an entry
holds is therefore a republish, not a new ingest from the node.

The source payload starts with version byte `0xf2`, then the v1 body
unchanged, then exactly `transparent_input_count` inputs. Each input is
`txid ‖ LEB128 index ‖ LEB128 value ‖ LEB128 script length ‖ script`; the count
is not repeated. Decoding refuses:
- truncation, trailing bytes and non-canonical integers;
- an input list that disagrees with the input count, and duplicate outpoints;
- input totals above `MAX_MONEY`;
- without shielded components, input values that do not equal outputs plus
  the exact fee.

Input scripts are bounded at 2 MB each and the record at 64 MiB. These are
ceilings that stop an ingest loudly, not consensus bounds.

`event-ingest --txid-display` (`--txid-display-inputs` is the same flag) writes
one immutable sidecar per block under `display-v2x/<block hash>.bin`:
- envelope magic `TPIRTX2X`, a 256 MiB block ceiling and a SHA-256 trailer;
- the sidecar is synced and linked before its block is appended to the event
  journal;
- inputs come from the previous outputs extraction already resolves for fees
  and spend events, so no lookup is added;
- each spend event must match its listed input, and every indexable input
  must have one;
- extraction also derives each entry, so a source no entry can be made from
  stops the display ingest. History and filter ingest build no source records,
  so these ceilings never stop them.

Reading a block's entries rechecks its source records against the recovered
event projection.

Earlier v1 sidecars (`display-v1/`) are no longer written. They are still read
for one purpose: the existing history journal keeps the events of scripts above
the journal's 10,000-byte ceiling in them. `events_at` takes those events from
a block's v2x sidecar, or else its v1 sidecar, skipping v1 display records
unread. A v1 journal has no source records and cannot be published as v2.

History publications no longer carry display tables. Three things were
removed; none was in production use:
- `shard-publish --txid-display`;
- the history manifest's `txid_display`;
- the history map's `txid_segments`.

Display is published only by the tiered controller below.

## Lookup: one row of one table per bucket

Every bucket of a display shard is one native table of a display geometry:
`txid-4k8` (4,096 rows), which the one-query site serves since 2026-10-13, or
`txid-2k8` (2,048 rows). Both have 8,192-byte rows, four 2,048-byte blocks at
the 8-bit profile. A row holds 72 entries in ascending tag order, then all-zero empty slots, then 56
zero bytes. A server checks that rows are canonical when it loads them, and a
client checks again when it decodes them.

An entry has exactly one row:
`SHA-256("txid-display/row/v3" ‖ seed ‖ shard id ‖ bucket ‖ tag) mod rows`,
with the seed as 4 bytes and the shard id and bucket as 8 and 4 bytes, little
endian. The publisher tries seeds 0, 1, 2, … below 64 for each bucket and
keeps the smallest that leaves no row over 72 entries, so rebuilding a range
from the journal reproduces its seed and digest. The seed is in the
digest-bound manifest (`buckets[b].placement_seed`) and in the map entry
(`placement_seeds`).

When no seed fits, as after a burst of transactions, the publisher keeps the
seed whose fullest row is emptiest and spills each row's overflow, in tag
order, into the same row of further segments. It logs
`alert=txid_display_overflow`. Segment counts are public in the map, and a
query is answered by every segment of its table.

A table is 61% full either way: 180,000 entries in a `txid-4k8` table, 90,000
in a `txid-2k8` one. Over the chain from genesis, the first seed fit 81 of 94
`txid-4k8` archives (86%; the rest needed two or three seeds) and 182 of 189
`txid-2k8` ones (96%), and no archive spilled into a second segment
([census](../../evidence/txid/txid-display-4k8-2026-10-13/README.md#placement-census)).
Twice the rows give twice the chances for one row to overflow, so the first
seed fits less often.

A lookup always sends the same transcript:
1. the map, manifest and setups, when not cached;
2. exactly one row query, whether the txid is found or absent.

The server, the publisher and the client each check that every entry they
read sits in its bucket and at its one row under that bucket's seed. A row
holding an entry placed elsewhere is refused, so a server cannot move an
entry between rows without the client noticing.

Request and reply sizes depend only on the geometry and segment count: a
28,168 B query at `txid-4k8` (21,000 B at `txid-2k8`) and a 10,256 B reply per
segment. A cold lookup adds the
bucket's setup, 60.1 KB per segment. The server learns the range, tier, bucket and
shard, and the timing. It learns nothing about the transaction's size: there
are no pages and no page-count classes.

Display tables use the 8-bit native profile
([architecture](../transparent/architecture.md#pir-scheme)), at the display geometry's rows
and row width: one 28-bit dithered scheme, no second width. Masks, packing
setup and setup seeds are derived under display's own native schema,
`transparent-txid-display-p8-v1` (`display::NATIVE_SCHEMA`), independent of
the history schema; init publishes it as `native_schema`. Bindings and table
names are display-specific (`directory-{b}`).

## Tiered display publication (proof of concept)

This proof of concept is not adopted. It publishes display tables apart from
history shards, so the anonymity set and query size do not follow history
geometry.

- **Time tiers.** Sealed archive shards are immutable and never rebuilt. They
  are contiguous from the display start. One recent shard is rebuilt per
  block.
  - A seal takes the smallest oldest unsealed range in which every bucket
    holds `archive_target` real entries, once it ends at least `reorg_margin`
    blocks below the tip. Nothing else gates it.
  - The rule is monotone, so bootstrap and incremental sealing produce
    identical shards.
  - The recent shard is a sliding window `[R, tip]`: `R` is the latest height
    at which every recent bucket still holds `recent_window` entries, but
    never above the first unsealed height, so every unsealed block is
    covered, and never below the map's start. It therefore overlaps the
    newest archive or archives, and its parent block is the one before `R`.
    The window's start is not part of any archive's identity.
  - The client picks the shard from the transaction's height: a height at or
    below the last archive's end goes to its archive, anything above to the
    recent shard. Entries of the overlap are in both, but each height has one
    shard.
  - A map, and a client, refuse a recent shard holding fewer than
    `recent_window` entries in a bucket, unless it starts at the map's start.
  - The controller keeps in memory every block from the latest start at
    which the sealed blocks alone fill the window, so a rollback above the
    sealed floor can always move the window back, and reloads from there on
    restart.
  - A bounded window drops the oldest archive from the map.
- **Buckets.** `H("transparent-txid-display/bucket/v2" ‖ tag) mod N` selects a
  bucket, which is a separate table. N is a manifest parameter.
- **Publication.** A separate map (`txid-shards.json`), content-pure sealed
  manifests with absolute shard ids, and a recent revision lineage.
  - The display geometries, `txid-4k8` (4,096 rows × 8,192 B, 28,168 B per
    query upload) and `txid-2k8` (2,048 rows, 21,000 B), live in a
    display-only registry. A root's `display-root.json` fixes its geometry, so
    changing it is a fresh lineage.
  - One table per bucket halves a shard's tables and runtimes compared with v1.
- **Split map.** The map is also published split, so a client's map bytes stay
  bounded as the archive window grows (below).
- **Workers.** The `role` setting of a `pir-query-server`
  `transaction_details` section picks which tables a worker holds:
  - `archive-owner`: the sealed archives;
  - `recent-replica`: the recent shard;
  - `combined`: both.

  The controller stages seals on any worker that holds archives, before a map
  names them. It hands every new map and every reorg invalidation to any
  worker that holds the recent shard. A combined worker gets both, so one
  host can serve the whole publication. The edge route keeps its two
  prefixes, `/v2/txid/archive/*` and `/v2/txid/*`, and both may name the same
  worker.

  A site request runs a second lineage beside the router's: one combined
  worker on a host of its own that also serves its own public hostname, with
  its own coordinator unit names, paths and journal
  ([deployment](../transparent/deployment.md#txid-display-one-query-site-2026-10-12)).

  The deploy request picks the layout. A request without a `recent` host runs
  one combined worker on the archive host. Its optional
  `hosts.former_recent` names the host of a split deployment's recent replica,
  and the `workers` phase then stops and removes that worker's unit there. The
  worker's data on that host is kept.
- **Leakage** is the range, tier, shard, bucket and timing. With
  `archive_target` and `recent_window` both 180,000 and one bucket, as the
  one-query site runs `txid-4k8`, every lookup's anonymity set is at least
  180,000 entries: archives hold 180,000–180,800, and the recent shard 180,000
  plus at most one block, or every unsealed entry when there are more.

Source:
- `transparent-shard/src/display/` (format, placement and seal rule);
- `src/transparent/transparent-publisher/src/tx_detail/` (`txid-display-controller`);
- `src/transparent/transparent-query-server/src/tx_detail/` (serving, run by
  `pir-query-server` with feature `transparent-detail`);
- `tools/transparent` (`txid-control`, `txid-inventory`);
- the `txid-display-*` deploy commands.

The v1 publication has run in production beside history since 2026-10-06
([status](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/docs/transparent/status.md#tiered-txid-display-in-production-2026-10-07)), unaccepted.
The code on `main` no longer builds or reads v1. Serving v2 needs a fresh
journal and lineage; the gates are in [remaining work](../transparent/remaining-work.md).

The display controller composes the shared lifecycle at source acquisition,
candidate preparation and owner activation. Its borrowed controller token keeps
the store and every seal job with their existing owner. Background seal building
and staging remain ordered; preparation selects only the staged contiguous queue
prefix, checks immutable lineage and writes the recent candidate. Owner publication
then delivers invalidations, activates archive owners before recent replicas and
commits the existing active record. Adaptive source/background/retry waits use the
shared boundary wait without cancelling these operations or coalescing seals.
Internal phase timing does not change status formats, readiness or authority.

### Controller status and metrics

`txid-display-controller run --status-listen` serves private JSON status on `/`,
`/v1/status` and `/v1/health`, and Prometheus text on `/metrics`. Workers keep
serving the last activation when the controller stalls or halts, so these are
what show it. Gauges: `txid_display_node_height`, `_published_height`,
`_lag_blocks`, `_oldest_unpublished_seconds`, `_stalled`, `_halted`,
`_consecutive_failures`, `_seal_queue`, `_last_success_timestamp_seconds` and
`_last_seal_timestamp_seconds` (0 until the first since the process started).
Counters: `txid_display_cycles_total` and `_cycle_failures_total`. Histograms:
`txid_display_cycle_seconds` per successful cycle, `_freshness_seconds` per
newly published block from first observed to active, and
`_cycle_stage_seconds{stage}` with `build`, `verify`, `digest`, `write`,
`prebuild` (only with `--ship-runtimes`), `ship`, `prepare` and `activate`.
Display workers add `transparent_display_prepare_seconds` per successful
prepare and `transparent_display_self_check_seconds` per prepare that loaded
shipped runtimes to their own `/metrics`.

### Shipped recent runtimes

Every block the recent replica prepares each table of the new recent revision:
it encodes the database, computes the hint and the two-mask preprocessing. With
`txid-display-controller run --ship-runtimes` (off by default) the controller
does that work once instead and ships the result with the candidate.

- **Controller.** After writing a candidate it builds every table segment of the
  recent revision on the runtime build pool (`TRANSPARENT_BUILD_THREADS`) and
  writes each runtime, in the runtime disk-cache entry format, as a plain file
  at the candidate root: `<sha256(key)>-<identity>.runtime`, beside the revision
  directories. Never inside one, whose files must be exactly its manifest's, and
  never as a subdirectory, which a worker would read as a revision. The cycle
  records `prebuild_ms`, `shipped_bytes`, the files and `prebuild_failures`. A
  failed build is logged and counted, its files are removed, and the candidate
  ships without runtimes. Archives are not shipped: they are built once per seal
  by `stage`.
- **Transport.** The fleet adapter's `rsync` carries the files unchanged. They
  are named by revision, so every block's files are copied in full; collection
  deletes them with their candidate on both sides.
- **Worker.** When a publication directory holds any `.runtime` file, the
  prewarm tries the file for each unsealed table segment before restoring or
  building. It is found by the same identity a disk-cache entry is: revision
  digest, table, segment, geometry, source SHA-256, setup seed, scheme,
  transport and format, so one made for anything else is not found. It is loaded
  read-only under the restore path's slot and memory reservation, keeps its
  preprocessing mapped from the shipped file, and is never saved to the worker's
  own disk cache.
- **Self-check.** After every load the worker reads the segment, verifying its
  SHA-256 as a restore does, and checks the runtime end to end: its encoded
  database must equal the segment coefficient for coefficient, and a fresh
  client query for one randomly sampled populated row must decode, under the
  published masks, to that row. A missing, rejected or failing file is counted
  (`shipped_fallbacks` in the prepare reply,
  `transparent_shard_shipped_fallbacks_total`) and the table is built locally.
  The prepare reply also reports `shipped` and `self_check_ms`.
- **Trust.** The coordinator is trusted, as for Enhance packing state. The file
  checksum detects corruption only. The disk format's own checks already bind
  the published masks to the preprocessing; the self-check adds that the
  database is the segment's and, for the sampled row, that the preprocessing
  answers for that database. Wallets see the same published masks, digests,
  epochs and answers as from a local build, and the wire format is unchanged.

[Pre-deploy bench](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-shipped-runtimes-bench-2026-10-07/README.md);
[production deploy and measurement, 2026-10-08](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-shipped-runtimes-2026-10-08/README.md):
the worker's build CPU and query interference went away as designed, and the
80 MiB copy per block became the block-to-serving tail on the private network.

### Split map

The full map (`GET /v2/txid/shards`, about 583 B per entry) costs about 248 KB at
genesis coverage (426 entries). A client refetches it after every 409, and the
recent rebuild causes one every block. The controller therefore also publishes
the same entries as two kinds of document, derived from the full map
(`transparent-shard/src/display/split.rs`):

- **Recent map** (`GET /v2/txid/map`, `txid-map.json`): the seal parameters,
  the start, the archive count, the first height no archive holds
  (`unsealed_start`), one reference (start height and SHA-256) per index
  chunk, and the recent shard's entry. Compact canonical JSON, served
  with `X-Txid-Map-Sha256` and `Cache-Control: no-cache`.
- **Index chunks** (`GET /v2/txid/map/{base}/{sha256}`,
  `txid-index-<sha256>.json`): the listed archives with absolute shard ids in
  `[base, base + 32)`, where `base` is a multiple of 32. Served with
  `Cache-Control: public, max-age=31536000, immutable`.
  - A seal changes only the newest chunk and a window drop only the oldest;
    every other chunk keeps its digest.
  - An unknown digest is a 409, like an unserved revision.

How the split is produced and served:
- The controller writes each chunk once into `index/<sha256>.json` under the
  display root and hard-links it into every candidate beside the recent map.
  Collection removes chunks no retained candidate links.
- A worker derives the split from the full map itself and refuses a candidate
  whose split files differ from that derivation. It serves candidates written
  before the split from the derivation alone.
- The router gzips the two new routes when the client accepts it.

A chunk names a 32-archive range, coarser than the shard id that queries
already name, so it adds no leakage.

### Wallet client

`wallet-libraries/zakura/transparent-txid-client` is the wallet's client for this
publication: synchronous, with no HTTP stack or async runtime. The tooling
client in `tools/transparent/src/support/txdisplay.rs` remains the
reference it is tested against.

The qualified `transparent-txid-display` release includes `txid-quality-canary`,
the standalone encrypted lookup probe. It checks a retained fixture's transaction
id and mined height against an independently pinned encoded-entry digest. Run
this packaged probe on a trusted validation host so acceptance does not depend
on a binary in an older development checkout. Keep the original fixture digests;
never derive the expected digest from the server response being checked.

- **API.**
  - The wallet implements `TxidTransport::send(TxidRequest) -> TxidReply`. A
    request has a method, an origin-relative `path()`, a loggable route
    `template()` without ids or digests, and a body. `POST` bodies are
    `application/octet-stream` with a declared length. A reply carries the raw
    status, `Retry-After`, `X-Txid-Map-Sha256` and body; the client interprets
    them.
  - `TxidDisplayClient::lookup(transport, txid, mined_height, cancel)` takes
    the txid in internal byte order and the height from the wallet's accepted
    chain. It returns `Found { entry, provenance }`, where provenance is the
    recent map's SHA-256, shard id, revision, manifest digest and tier.
    Otherwise it returns `Absent`, `PlacementUnknown(Below | Above)`,
    `RecentBelowFloor` or `Unsupported`.
  - The client caches native profiles, init, the recent map, index chunks by
    digest, manifests and setups. `refresh_map(transport, cancel)` fetches and
    validates only the recent map.
  - With the `testing` feature, `with_placement_refresh_age` and
    `cached_revisions` let tests make a map stale and inspect the cache, and
    `with_min_recent_floor` lowers the client's recent floor for fixtures
    holding a handful of txids. Wallets do not enable it.
- **Transcript.** Requests are sent one at a time:
  1. init, the recent map, and the index chunk covering an archive height;
  2. the manifest and the bucket's setups, when not cached;
  3. exactly one row query.

  Under the placement and cache rules below, init or the map can be fetched
  again after the map or a chunk, for example `Map, MapChunk, Init, Manifest,
  Setup, Query`. A lookup sends at most 2 map, 3 init and 2 query requests.
  `Absent` sends the same query. Placement, recent floor and support results
  send no query. `cancel` is polled before every request.
- **Placement.** A height below the recent map's `unsealed_start`, one past
  the last archive's end, is looked up in its archive through the index
  chunk covering it; that height and above in the recent shard. A height below the
  map's start or above its tip is
  `PlacementUnknown(Below | Above)` straight from a map the lookup fetched,
  or one less than 30 s old by the wall clock. A lookup holding an older map
  fetches it once more and places the height again on the new map. The wall
  clock is used because a monotonic clock stops while a phone sleeps; a clock
  that went back counts as old.
- **Recent floor.** A query names the shard and bucket it reads, so the txid
  hides only among that bucket's txids. Sealed shards hold `archive_target`
  per bucket, checked with the map. The recent shard is queried only when its
  map entry's `min_bucket_records` reaches the larger of the map's
  `recent_window` and the client's own floor (`MIN_RECENT_FLOOR`, 10,000), so
  a server publishing a low `recent_window` does not lower it. Otherwise the
  lookup returns `RecentBelowFloor` with no manifest, setup or query: not
  privately available, and never `Absent`. An honest publication keeps the
  window through reorgs above the sealed floor; a lineage that does not start
  at genesis starts with a small recent shard. The map check also refuses a recent
  shard below `recent_window` that does not start at the map's start (see
  the time tiers above). Archive lookups are unaffected. A map
  older than 30 s is fetched once more before this result, as for a placement.
- **Caches.**
  - Init is fetched on first use.
  - A map whose schema or seal parameters differ from the last map's is a new
    publication of the same chain, and init is fetched again.
  - A map naming another network or genesis hash than the first map the
    client accepted is `Protocol(Map)`. The client keeps the map it holds:
    no publication changes its chain, so a mainnet wallet never follows a
    testnet edge. A client therefore serves one chain, and a wallet keeps one
    client per chain and origin; Vizor keys its clients by origin.
  - A map whose seal bucket counts, or any entry's bucket count, lie outside
    1 to 64 is `Protocol(Map)`, before any bucket is computed.
  - When the map names a geometry init does not list, either document may be
    the older one during a swap: a later publication may add a geometry, or
    drop the one an older map names. The lookup fetches again whichever of
    init and the map it has not fetched yet, then tries once more. It returns
    `Unsupported` only when it fetched both and the geometry is still missing.
  - An init naming an unknown schema or codec stays cached, and lookups return
    `Unsupported` without a request, until `refresh_map` or a new client.
  - Each new recent map keeps the index chunks it names. It keeps the
    manifests and setups of the revisions it names through its recent entry
    or a kept chunk, and drops the rest: superseded recent revisions, the
    archives of a chunk that a seal or a window drop replaced, and all of a
    replaced publication, whatever shard ids it reused. A dropped archive's
    manifest and setups are fetched again on its next lookup.
  - A seal that adds an archive to the newest chunk replaces that chunk, so
    it drops the cached manifest and setup of every archive in it. At about
    64 seals a year, each such archive costs about 21 KB raw when it is next
    looked up.
  - A cached manifest is reused only when the map entry naming its digest
    still describes it field for field.
- **Errors.**
  - A 409 refetches the recent map (but no cached index chunk), retries the
    whole lookup once, then returns `Stale`.
  - A retry that would name the txid's bucket under another bucket count
    than this lookup already named returns `Stale` with no further request.
    Two counts would tell the server the tag's hash modulo both.
  - The map refresh a stale placement waits on fails like any request:
    `Unavailable`, `Transport`, `Refused`, `Protocol` or `Cancelled` rather
    than `PlacementUnknown`.
  - A 503 returns `Unavailable { retry_after }`, as do 429 and other 5xx; the
    client does not retry.
  - A 400, 408, 411, 421 or other 4xx returns `Refused(status)` without a
    retry.
  - Any validation failure returns `Protocol(kind)`, never `Absent`.
    Validation covers the map, chunks, manifest, init parameters, setups,
    per-frame binding, epoch and response length. It also covers rows: a
    noncanonical row, an undecodable entry, an entry outside the row its
    placement names, or the same tag in two segments.
  - Transport failures return `Transport`.

### A fresh publication

A display re-cut or re-layout cannot extend the live map: the controller halts
on a changed sealed entry. It is published as a fresh lineage instead, built
from its own root:
- its start may be lower;
- shard ids restart at 0, and every digest is new.

Network, genesis hash and seal parameters stay the same, because a worker
refuses a candidate that changes them. Each worker prepares the new candidate
beside its live publication and swaps at activation, so it never stops
answering.

A wallet still holding the old map follows the swap:

1. Its requests for old revisions and index chunks are answered from the
   worker's retired snapshot while the old runtimes are resident.
2. After the next prepare or an eviction drops those runtimes, an old
   revision gets a 409. The client fetches the new map and retries the lookup
   once on the new lineage.
3. A height below the old start is placed `Below` from the old map only
   while that map is less than 30 s old (see Placement above). A retry once
   the map is older fetches the new map first.
4. A geometry the old init did not list costs one init request, plus a map
   request when the lookup had not fetched the map, once the serving init
   lists it. A cached map naming a geometry the new publication dropped is
   fetched again the same way.
5. The first map of the new lineage drops every cached manifest and setup of
   the old one.
6. A wallet whose cached init is unsupported gets `Unsupported` with no
   request, so it sees no 409 and no new map, until it calls `refresh_map`
   or replaces the client.

The swap must keep the network and genesis hash: a client refuses a map that
changes them.

`shard_id`, `revision` and `manifest_digest` in a lookup's provenance name the
lineage that answered. Wallets should keep them as provenance only: a later
lookup is placed by height on the current map, never by a stored shard id.

Not built: neither the map nor init carries a lineage field. A field added to
the recent map would change its canonical bytes, which today's clients check,
so they would refuse it; digest-keyed caches make one unnecessary. Seal
parameters stay one set per publication, not per height range.

## Sizing and routing qualification

The [sizing and anonymity findings](transparent-txid-sizing-and-anonymity-findings.md)
sized the v1 record: its inline cutoff, overflow pages and page-count classes.
Fixed-size v2 entries make those findings historical. Its census of eligible
transactions per range still sizes shard counts.

What remains to measure for v2 is in [remaining work](../transparent/remaining-work.md):
- the share of entries carrying an omission, against the usage figures above;
- source-sidecar bytes from genesis.

Placement, certificates, runtime memory, rebuild and query time of the served
`txid-4k8` tables are in the
[4k8 evidence](../../evidence/txid/txid-display-4k8-2026-10-13/README.md); those of
`txid-2k8` in the [gate evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-2k8-gates-2026-10-11/README.md)
and the [8-bit evidence](../../evidence/txid/txid-display-p8-2026-10-13/README.md).

The journal census, retired with the rest of the [sizing tools](https://github.com/valargroup/wallet-pir/blob/f26b9476fbcd37aca4b4882b9264e90c1896be16/tools/txid/txid-sizing/README.md),
measured v2x journals and builds against the last pre-v2 source, `a368f19b`.

## Reproduce the native demo

Run from the repository root:

```bash
make transparent-txid-demo
```

The demo does the following:
1. `fixtures/verify_fixture.py` parses the frozen blocks and their 15 parent
   transactions with the Python standard library only. It recomputes every
   expected 113-byte entry independently of the Rust extractor.
2. The example ingests the blocks through `extract_block` and the event
   journal, writing v2x source sidecars. It restarts at the committed
   checkpoint and reads every entry back.
3. It publishes an archive and a recent shard with the production
   `publish_shard` and `write_candidate`.
4. It serves them from an archive-owner and a recent-replica worker behind an
   edge.
5. It looks every transaction up with `transparent-txid-client` over loopback
   HTTP.

Run with `--corrupt-oracle`, it must exit nonzero.

Frozen inputs are mainnet blocks 347499–347501 and genesis, plus fifteen
complete parent transactions. The fixture records pinned upstream vector URLs,
checksums and the public raw-transaction source. Genesis is a separate
publication, so the demo does not imply coverage over the heights in between.

| Case or control | Required evidence |
|---|---|
| Ordinary send with change | Complete entry; the change output's address equals the source |
| Several source scripts | Bit 2, with the first address-shaped source |
| More than two outputs, 300-output transactions | Bit 3, the true output count, still one fixed entry |
| External transparent unshielding | No inputs and the shielded bit; source absent |
| Coinbase | Coinbase bit, zero fee, no source |
| Output without an address (genesis P2PK) | Kind-3 slot with its value |
| Missing txid | `Absent`, with a transcript identical to a found lookup's |
| Wrong revision or binding | 409 setup and 400 query |
| Altered entry | The independent comparison rejects it |
| Unknown placement or codec | No query dispatched |
| Restart | The checkpoint reopens and sidecars read back unchanged |
| Privacy | No txid, tag or output script in any path, header or query string |

The report records:
- source and fixture SHAs;
- per-lookup request counts and upload and download bytes;
- the absent transcript;
- table packing;
- safe HTTP paths and header shapes.

Never retain client keys or encrypted query bodies as evidence.

A pass establishes server protocol and retrieval evidence. It does not
establish production capacity, cryptographic release readiness, wallet recovery
or Vizor correctness. Production deployment requires separate approval.

The v2 run at `9557d843`: [native demo evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/txid-display-v2-2026-10-08/README.md).

## Wallet stage

This stage lives in wallet-libraries and Vizor:
- The wallet stores validated entry facts with source and revision provenance.
- It checks them against everything it owns: owned outputs 0–1, spend events
  and their spent scripts, and metadata. A contradiction is recorded, never
  overwritten.
- The view shows the source with an owned flag, outputs 0–1 and the named
  omissions. It reports shared funding when the wallet owns some but not all
  inputs.
- Opening a transaction prioritises its missing details; cached facts render
  immediately.

Under `PrivateRequired`, a failed or omitted detail never by itself authorises
a public lookup. A public enhancement reveals that one transaction to the
server. It happens only when the user asks for it on that transaction, and it
derives P2PKH and P2SH input addresses from scriptSigs without parent lookups.

Transaction bytes cannot recover contact names, original intent, collaborative
attribution or application grouping.
