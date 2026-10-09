# Transparent txid display PIR: server contract and qualification

A wallet opening a transparent transaction privately fetches one fixed-size
display entry by txid. The entry gives who funded the transaction, where up to
two of its outputs went, the fee, and what the entry leaves out. This page is
the server contract, the lookup protocol and the reproducible demo.

Source and local qualification do not establish deployment or production
capacity. Executable milestone tracking belongs in
[remaining work](remaining-work.md).

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
  same resolved outputs (`extract_block`, `transparent-filter-server/src/extract.rs`).
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

## Lookup: one table per bucket

Every bucket of a display shard is one native table of 2,048 rows of 4,096
bytes. A row holds 36 entries in ascending tag order, then all-zero empty
slots, then 28 zero bytes. A server checks that rows are canonical when it
loads them, and a client checks again when it decodes them.

An entry has two candidate rows, derived from its tag, the shard id and its
bucket. The publisher places it by cuckoo insertion with bounded evictions,
which keeps about 95% of slots filled in one segment. A table that still
overflows opens another segment, and a query asks every segment.

A lookup always sends the same transcript:
1. the map, manifest and setups, when not cached;
2. exactly two row queries, whether the txid is found or absent.

Request and reply sizes depend only on the geometry and segment count. The
server learns the range, tier, bucket and shard, and the timing. Unlike v1, it
learns nothing about the transaction's size: there are no pages and no
page-count classes.

The native parameters are the history `txdirectory` kind's: the same rows, row
width and setup seeds as before. Bindings and table names are display-specific
(`directory-{b}`).

## Tiered display publication (proof of concept)

This proof of concept is not adopted. It publishes display tables apart from
history shards, so the anonymity set and query size do not follow history
geometry.

- **Time tiers.** Sealed archive shards are immutable and never rebuilt. One
  recent shard covers `[S, tip]` and is rebuilt per block. The client picks the
  shard from the transaction's height.
  - A seal takes the smallest oldest range in which every bucket holds
    `archive_target` real entries. The remainder must keep `recent_floor`, and
    the range must end at least `reorg_margin` blocks below the tip.
  - The rule is monotone, so bootstrap and incremental sealing produce
    identical shards.
  - A bounded window drops the oldest archive from the map.
- **Buckets.** `H("transparent-txid-display/bucket/v2" ‖ tag) mod N` selects a
  bucket, which is a separate table. N is a manifest parameter.
- **Publication.** A separate map (`txid-shards.json`), content-pure sealed
  manifests with absolute shard ids, and a recent revision lineage.
  - The `txid-2k` geometry (2,048 rows × 4,096 B, 38,920 B per dithered
    query upload, or 40,200 B at the 49 bits every server also accepts)
    lives in a display-only registry.
  - One table per bucket halves a shard's tables and runtimes compared with v1.
- **Split map.** The map is also published split, so a client's map bytes stay
  bounded as the archive window grows (below).
- **Leakage** is the range, tier, shard, bucket and timing.

Source:
- `transparent-shard/src/display/` (format, placement and seal rule);
- `transparent-filter-server/src/txid_display/` (`txid-display-controller`);
- `transparent-shard-server/src/display/` (`transparent-txid-server`,
  `txid-control`, `txid-inventory`);
- the `txid-display-*` deploy commands.

The v1 publication has run in production beside history since 2026-10-06
([status](status.md#tiered-txid-display-in-production-2026-10-07)), unaccepted.
The code on `main` no longer builds or reads v1. Serving v2 needs a fresh
journal and lineage; the gates are in [remaining work](remaining-work.md).

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

[Pre-deploy bench](../evidence/txid-display-shipped-runtimes-bench-2026-10-07/README.md);
[production deploy and measurement, 2026-10-08](../evidence/txid-display-shipped-runtimes-2026-10-08/README.md):
the worker's build CPU and query interference went away as designed, and the
80 MiB copy per block became the block-to-serving tail on the private network.

### Split map

The full map (`GET /v1/txid/shards`, about 583 B per entry) costs about 248 KB at
genesis coverage (426 entries). A client refetches it after every 409, and the
recent rebuild causes one every block. The controller therefore also publishes
the same entries as two kinds of document, derived from the full map
(`transparent-shard/src/display/split.rs`):

- **Recent map** (`GET /v1/txid/map`, `txid-map.json`): the seal parameters,
  the start, the archive count, one reference (start height and SHA-256) per
  index chunk, and the recent shard's entry. Compact canonical JSON, served
  with `X-Txid-Map-Sha256` and `Cache-Control: no-cache`.
- **Index chunks** (`GET /v1/txid/map/{base}/{sha256}`,
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

`transparent/crates/transparent-txid-client` is the wallet's client for this
publication: synchronous, with no HTTP stack or async runtime. The tooling
client in `transparent-shard-server/examples/support/txdisplay.rs` remains the
reference it is tested against.

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
    Otherwise it returns `Absent`, `PlacementUnknown(Below | Above)` or
    `Unsupported`.
  - The client caches native profiles, init, the recent map, index chunks by
    digest, manifests and setups. `refresh_map(transport, cancel)` fetches and
    validates only the recent map.
  - With the `testing` feature, `with_placement_refresh_age` and
    `cached_revisions` let tests make a map stale and inspect the cache.
    Wallets do not enable it.
- **Transcript.** Requests are sent one at a time:
  1. init, the recent map, and the index chunk covering an archive height;
  2. the manifest and the bucket's setups, when not cached;
  3. exactly two row queries, even when the rows coincide.

  Under the placement and cache rules below, init or the map can be fetched
  again after the map or a chunk, for example `Map, MapChunk, Init, Manifest,
  Setup, Query, Query`. A lookup sends at most 2 map, 3 init and 4 query
  requests. `Absent` sends the same two queries. Placement and support
  results send no query. `cancel` is polled before every request.
- **Placement.** A height below the map's start or above its tip is
  `PlacementUnknown(Below | Above)` straight from a map the lookup fetched,
  or one less than 30 s old by the wall clock. A lookup holding an older map
  fetches it once more and places the height again on the new map. The wall
  clock is used because a monotonic clock stops while a phone sleeps; a clock
  that went back counts as old.
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
    noncanonical row, an undecodable entry, or the same tag in two distinct
    rows.
  - Transport failures return `Transport`.

### A fresh publication

A display re-cut or re-layout cannot extend the live map: the controller halts
on a changed sealed entry. It is published as a fresh lineage instead, built
from its own root:
- its start may be lower;
- shard ids restart at 0, and every digest is new;
- shards may use another registered geometry, such as `txid-4k`.

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

The [sizing and anonymity findings](../../docs/transparent-txid-sizing-and-anonymity-findings.md)
sized the v1 record: its inline cutoff, overflow pages and page-count classes.
Fixed-size v2 entries make those findings historical. Its census of eligible
transactions per range still sizes shard counts.

What remains to measure for v2 is in [remaining work](remaining-work.md):
- the share of entries carrying an omission, against the usage figures above;
- placement load and segments at the chosen `archive_target`;
- source-sidecar bytes from genesis.

The journal census in `transparent/tools/txid-sizing` measured v2x journals.
It builds against the last pre-v2 source, `a368f19b`.

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

The v2 run at `9557d843`: [native demo evidence](../evidence/txid-display-v2-2026-10-08/README.md).

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
