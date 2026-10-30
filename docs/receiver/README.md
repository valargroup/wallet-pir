# Receiver directory

The receiver directory lets a restored wallet find Ironwood payments to keys it
cannot rescan for, such as its one-time IVK refund and incoming keys. It maps each
canonical 43-byte Ironwood receiver to the payments recoverable with the all-zero
outgoing viewing key (OVK). Coinbase recipients are excluded. Every Ironwood
Action still counts toward note positions, including coinbase and unrecoverable
outputs.

Receiver is a product of wallet-libraries' PIR role packages, each with a
`receiver` module and feature. `zakura-pir-protocol` owns the canonical fixed
record codec (`receiver::wire`), records, the directory and companion formats,
segment publications and rolling block filters; it parses structure only. Its
`receiver-crypto` feature checks receivers, unified addresses and record fields and
recovers receivers with the zero OVK. `zakura-pir-publisher` projects blocks into a
segment's directory and builds its directory, choice object and companion, and its
`receiver-crypto` feature builds their lower inclusion paths.
`zakura-pir-query-server` holds the evaluator and `zakura-pir-client` the PIR
client and the wallet `Transport` interface. The indexer's SQLite chain index and
optional payout audit store are this repository's `receiver-store` package. The
indexer enables the checks before storing a payment and before serving a
publication, and a wallet checks a retrieved payment before crediting it.
Dependencies run one way from wallet-pir to wallet-libraries; wallet-libraries
never consumes wallet-pir packages. `receiver-pir-server` holds reusable publication
and evaluator primitives and the certificate report; the inventory format is
wallet-libraries'.
`pir-query-server` owns HTTP serving, admission and synchronization. `receiver-indexer` builds them from a Zakura node; its
services run through `pir-publisher` and `pir-query-server`. The service shares process identity and
admission (`pir-control`), metrics (`pir-observability`) and the PIR profile
(`pir-native`) with the other services, and reads the node through
the server-only chain adapters (`zakura-server-chain`, `src/shared/pir-chain`), which decode
blocks, count note positions and recover receivers.

The indexer's `receiver_indexer::application` supplies operations to the internal
`src/shared/pir-pipeline` lifecycle. Shared supervision retains the poll delay after
each complete pass and the product's retry policy. Shared acquisition reconciles
and commits through the concrete chain client and store, then activates rolling
block hints; its work token borrows those dependencies. Shared preparation
consumes that token, building the publication from the committed chain index
alone, and passes the prepared product candidate to owner publication. The
optional payout audit only supplies the report health serves beside it. The final
canonical check and epoch-fenced distribution commit remain publisher owned; one-shot
publication writes the publication's files instead. Preparation failure keeps
committed progress for retry. `pir-publisher` configures these calls and
independently wires its distribution listener, canonical guard and optional payout audit;
the guard continues checking served anchors throughout both phases. Query hosts receive prepared objects over HTTPS and never contact the node. The shared acquisition seam accepts
product work without owning a store, protocol, queue or authority ([lifecycle boundaries](../enhance/architecture.md#ingestion-application)).

## Publisher distribution

The root-managed publisher exposes Receiver material under `/provider/receiver/v1/`.
Its `current` envelope, `objects/<digest>` downloads, and `authority/check`
operation use the shared publication authority contract. The deployment edge
must terminate HTTPS; these provider operations are separate from wallet routes.

A distribution binds each segment's directory rows, choice object, companion rows and
both setups to their sessions, plus shard filters and rolling block-hint ranges. Rolling filters can advance
before a new directory finishes preparing. Unchanged sealed files reuse exact-file
verification; changed files are checked again. Publication, withdrawal, and restart
recovery advance durable authority, and a downloaded object never authorizes serving.

## Node RPC policy

The current indexer source uses the server-only chain adapter with
a 20-second connection timeout and a 120-second total request timeout. The total
deadline includes connection establishment and response reading. The RPC cookie
is reread for every request; HTTP 401 remains an HTTP error. Responses retain the
chain-derived bound for a maximum-size verbose block and are streamed within
that bound even without Content-Length. The transport does not retry or split
batches. This source policy is independent of the existing deployment decision
to retain the prior Receiver release.

## Records and publications

A 289-byte record holds version (1 byte), receiver (43), page (4), total pages
(4), present flag (1), height (4), block hash (32), txid (32), transaction index
(4), Action index (4), note position (8), Action input nullifier (32), note
commitment (32), ephemeral key (32), ciphertext prefix (52) and ordinal (4). The
ordinal is the record's companion slot. Integers are little endian and hashes use
protocol byte order. The Action input nullifier is decryption context, not the
received note's spend nullifier. Structural encoding and decoding check lengths,
tags and page ranges. The indexer and wallet additionally refuse a note commitment
or Action input nullifier that is not a canonical field element, or an ephemeral key
that is not a valid Ironwood ephemeral public key.

A shard's payments are published in segments, each a **directory** and a
**companion**, both P8 tables of 4,096-byte rows:

- **Directory.** 2,048 rows of 186 entries of 22 bytes: a receiver's 14-byte tag
  under the segment's tag salt, its first companion slot (u24), its payment count in
  the segment (u16) and its last payment's height as an offset from the shard's start
  (u24). Each row is sorted by tag; an all-zero entry is empty. A receiver has one
  entry per segment.
- **Companion.** Up to 65,536 rows of five 801-byte slots: the record, then its
  512-byte witness payload (below). A receiver's payments are the consecutive slots
  `first..first+count`, ordered by tag then note position, so `page` is the slot
  less `first`, `total` is the count and `ordinal` is the slot.
- **Choice object.** Each directory's xor-retrieval choice table, keyed on the tag,
  says which of a tag's two candidate rows holds its entry, so a lookup is one
  directory query. It is served on its own route by digest and bound by the
  directory's digest, length and key count.

The tag is SHA-256 of the domain, the tag salt and the receiver. A segment's first
salt comes from the block before the shard and the segment index. When a block's new
receivers collide with a tag or do not place, the whole segment is placed again under
the first of eight retry salts that mix in that block's hash, which its own payments
could not have been ground against; every block has its own eight retries. Choice
seeds come from the tip block's hash for the open segment and the sealing block's
for a seal, trying up to 64 and 2^16 seeds. A publication whose choice table no seed
builds fails with `receiver.choice.failed` and the previous one keeps serving; at a
seal it stops sealing there without moving the boundary.

Each table's manifest binds the network, history correction revision and the shard's
coverage. The directory's also binds its tag salt, entry and payment counts, the
choice object and the rows' digest; the companion's binds the directory's revision,
its rows, `query_rows`, payment count, the tree it was built at and its rows' digest.
A table's session is its manifest and the SHA-256 of its public setup, and the
segment publication is the two sessions. Before serving, the publisher and every
query server check a segment whole: each entry routed by the choice to the row
holding it, distinct tags, extents tiling the payments, every slot's record and
payload in place (tag, page, total, ordinal, coverage and last height), complete
page runs, unique outputs and positions and zero rows past `query_rows`. The
indexer also checks every record cryptographically and every final payload against
its Ironwood shard's recorded root.

## Rolling block filters

`GET /v1/receiver/block-filters/head` advertises the network, history correction revision, first held height and tip hash. `GET /v1/receiver/block-filters/:revision/:from/:limit` returns one sparse range (`IWRANGE2`) of up to 1,048,576 heights within 4 MiB, starting exactly at `from`: the hash before its first height, its end hash and a set only for each block that pays a receiver, so an empty block costs nothing. It may be shorter than requested, so a dense gap continues from the height after the range's end. Pruned or stale requests return 410, future heights return 503 and invalid limits return 400. At most 8 ranges are built or sent at once, and more return 429 with `Retry-After`. Every response is uncached.

Sets use the shared GCS codec with P = 20 and M = 1,569,792, keyed by network and height alone, so a wallet can match history below its birthday. The publisher is trusted for which chain the sets were built on, as for complete indexing. A wallet compares a range's end hash with its own block where it holds one, and the next range's parent with the previous end. The indexer builds sets from indexed chain payments before witness or PIR preparation. Inventory retention does not hold back hints. The canonical guard revokes noncanonical hints along with affected sessions. A wallet queries PIR after a match. A negative match needs no receiver query.

The rolling window spans seven days of target time: 8,064 blocks of 75 seconds before NU7, a blend across its activation, and 24,192 blocks of 25 seconds once the whole week is past it, following Common's NU7 schedule. `--filter-blocks` can cap it lower. Encoded sets in the in-memory window are also bounded to 64 MiB, and the head advertises a shorter suffix if needed. It is rebuilt from the chain index on restart. This is not a permanent filter archive. No payment rows or commitments are pruned. Where hints are missing, a wallet runs a complete PIR check of every watched address over the gap, paid ones included.

Increase `--history-revision` after an indexing correction on an unchanged chain. The store refuses a lower revision after restart, and the same revision is bound into PIR publication identity. Advancing it immediately revokes older sessions before replacement preparation, including direct-PIR sessions. Reorgs use block hashes. A wallet must not settle a queued hit with an older history revision or a PIR frontier below the hit. Filters and PIR may become available at different times. Measure both availability lag and time to credit. Hints never delay ordinary wallet sync.

A publication is an inventory of Receiver shards, each with its segments' directories and companions and, once sealed, a shard filter (see [Sealed shards](#sealed-shards)). Nothing published is an issuance input.

## Companion payloads

A companion slot's 512-byte payload is its payment's sixteen siblings inside its
Ironwood shard of 2^16 leaves, each supplied only when it is a complete subtree at
the companion's tree size, the rest zero. A payload whose Ironwood shard has
completed is final on its chain; a provisional one is usable only with the wallet's
own tree at exactly the companion's block. Wallets authenticate payloads against
their own tree. The payload arrives with the record, so there is no separate witness
request.

Payloads are built per Ironwood shard, never from the whole commitment history.
The store keeps every commitment, records each completed shard's root and the
block that completed it, and keeps every payment's final payload; a rewind past
that block removes them. Only the filling shard's paths are held in memory and
extended with each publication's new commitments; a restart or a rewind rebuilds
them from that shard's at most 65,536 stored commitments.

The store is `directory-v3.sqlite` in the data directory, at schema version 4. A
version 3 store's seals, which recorded payment table geometry, are dropped in the
transaction that opens it as version 4, so an interrupted upgrade leaves version 3;
the sealer reseals from the retained chain index, and the data directory's earlier
`tables/` files are removed as not ours. An earlier release's `directory.sqlite`,
whose records lack the ordinal, is still copied into it once and left unchanged.

`Store::correct` applies an indexing correction on an unchanged chain: it replaces
stored blocks' payments, checking hashes, positions and commitments against the
stored blocks, and advances the store's correction revision. A payment it adds to a
completed shard rebuilds that one shard's payloads from its stored commitments
under the recorded root, and sealed shards ending at or after the first corrected
block are unsealed so their boundaries are recomputed. Restart the service with the
same `--history-revision`.

## Sealed shards

History is split into contiguous Receiver shards covering every block, empty ones
included: sealed shards, whose directories never change under one correction
generation on one chain, then the open shard, whose one segment is rebuilt each
publication from the sealer's projection.

The sealer projects each whole block into the open segment as it arrives. A shard
seals after the block that reaches 317,440 payments (31/32 of the companion's
327,680 slots, about 83% of the directory's 380,928 entries). It seals before a block
that would pass the companion's capacity, take one receiver past 65,535 payments in
the segment, sit 2^24 or more heights after the shard's start, or collide or fail
placement under all nine of its own salt attempts. Every rule is decided block by
block, so the boundary never depends on when publication runs, and the choice table
plays no part. A block too large for one segment seals the open shard, then becomes
a shard of its own split in position order into up to eight segments; one needing a
ninth publishes nothing, so coverage never passes it. The store records each seal and
its segment count, a rewind past a shard's end removes it, and a restart replays the
open shard, so publishing after every block, in batches or after a restart gives
the same bytes. There are no geometry settings.

A sealed segment's companion is rebuilt at each publication's tip until every
payment's Ironwood shard has completed, reusing final payloads, then frozen at the
later of the shard's end and the block completing the last of them. A frozen
companion depends on that block, so a reorg replacing it rebuilds the companion of an
unchanged directory. Any reorg moves a served inventory's anchor off the chain, and
the guard revokes every session before a replacement is served.

Each sealed shard keeps one GCS receiver filter (`ironwood-receiver/v1/shard-filter`,
P = 20, M = 1,569,792, up to 2^23 receivers and 24 MiB) for as long as its history is
served. It is keyed to the shard's coverage and correction generation, and its
inventory descriptor binds its digest, size and count. The open shard has none.

The inventory head names the network, correction generation, coverage and the
inventory identity, which binds every shard's coverage, its segments' directory and
companion session IDs and its filter in order. Chunks list 16 shards each; a full
sealed shard's descriptor is about 2.2 KB, so the 128 MiB capture budget holds about
60,000 shards. A displaced inventory keeps serving its head's chunks, sessions,
choice objects and filters for `--retention-seconds` (default 600, ten minutes) so a
wallet that captured it can finish a restore pass; at most 16 are retained, and a
publication waits for one to expire rather than drop one early. Sealed directories
are shared between inventories, so retention costs only displaced open segments and
companions.

**Archive tier (later).** Every segment uses one 2,048-row directory and one companion of up to 65,536 rows, so a shard seals at 317,440 payments. A wallet back after a week reads every directory sealed since then, so its cost grows with volume: about 3.5 MB at 10 indexed payments a second against 0.13 to 0.18 MB at today's volume (blind PIR traffic, estimates). Add an archive tier like Transparent's once sustained volume passes about 0.5 payments a second (a seal a week) or the recent tier holds about 64 sealed shards. It re-seals history older than a cutoff into wider directories, which cuts the directories a restore reads. A wider directory needs more than one companion, and naming the companion a hit reads would then reveal which part of the shard the payment is in. So the archive needs a way to hide that, such as reading every companion on a hit, or an explicit decision to accept it. Transparent's own tier-boundary movement isn't designed yet either (`docs/transparent/remaining-work.md`, "Track A").

## Serving and memory

Directories and companions are prepared independently: a companion's load reads its
directory's rows and choice object to check it against the directory's entries, but
never needs the directory prepared beside it. Each prepared table is written once to
the data directory's `tables/` (`d-<revision>` with `.rows`, `.choice`, `.public` and
`.json`, `c-<revision>` with `.rows`, `.public` and `.json`), and inventories hold only
those descriptions. A byte-budgeted cache (`--table-memory-mib`, default 4096) keeps
the most recently used prepared tables, counting tables that retained inventories'
and in-flight queries still use, and prepares an evicted table again from its files
when a query needs it.

A preparation is charged its peak before it starts, inside the blocking work that
runs it: two copies of its rows, a companion's directory and 320 MiB of hint and
analysis scratch. The charge is held until that work stops, whether or not the
request still waits, then shrinks to the prepared table's rows plus 72 MiB until the
last owner drops it. When tables in use leave no room a query gets 429 until they
finish. The minimum budget is the largest directory resident beside the largest
companion's preparation, 920 MiB, and a smaller one is refused at startup. A restart
finds every unchanged table by revision and prepares none until it is queried, and
tables no live or kept inventory uses are deleted once no request that selected them
is still running. Shard filters and choice objects are read into the same budget only
while a request serves them or the cache keeps them.

Measured on one Apple M3 Ultra core set (`receiver-pir-server` `examples/measure.rs`,
`release-fast`, heap bytes counted by the allocator, one query each):

| Table | Upload | Download | Setup | Prepare | Server | Resident | Peak |
|---|---:|---:|---:|---:|---:|---:|---:|
| Directory, 2,048 rows | 21,044 B | 5,172 B | 22,528 B | 0.64 s | 5 ms | 61 MB | 310 MB |
| Companion, 2,048 rows | 21,044 B | 5,172 B | 22,528 B | 0.86 s | 7 ms | 78 MB | 283 MB |
| Companion, 16,384 rows | 71,220 B | 5,172 B | 22,528 B | 1.0 s | 17 ms | 137 MB | 440 MB |
| Companion, 65,536 rows | 243,252 B | 5,172 B | 22,528 B | 2.7 s | 37 ms | 339 MB | 608 MB |

A companion has exactly the rows its payments need, so its full query is its
`query_rows` query. A table with more rows, which the server accepts, answers either
length; 65,536 rows trimmed to 2,048 upload 21,044 bytes. At the seal target, a
100-payment block projects in 0.3 ms, replaying a whole segment's projection takes
0.14 s, the segment build with its choice table 0.55 to 0.86 s and its structural
check 0.29 s. Each record is checked once when it is indexed; a publication reuses
the final payloads the store recorded and rebuilds only those of payments whose
Ironwood shard is still filling.

## Certificates

Certificates are an operator gate, as Transparent's are
(`docs/transparent/deployment.md`): the service keeps no certificate state, and a
passing `native-certificates.json` report is required before a deploy or cutover.
The operator certifies every sealed directory and every frozen companion the
publication serves, under its served seeds:

1. Write each table's dithered 8-bit report with `native_certificate receiver`
   (`cargo run -p transparent-tools --features receiver --example
   native_certificate`), from the table's stored rows and served public setup
   (`<data_dir>/tables/{d,c}-<revision>.{rows,public}`). It rebuilds the table's
   noise analysis under its Receiver profile, through the same 8-bit path as
   Transparent's tables, and refuses rows whose public setup is not the served one.
2. Run the pinned checker on each report at the 50-bit floor: ipir-sp `5ea080a`,
   v0.1.0-rc.7, `reinspiring/tools/security/certify_native.py`, SHA-256
   `eb4e57992a0d697cc362282ce2348620145b64485dbe80c1b3a049e54dd64918`, run as
   `python3 -I certify_native.py <report> --require-bits 50` from its checkout beside
   `src/native_gaussian_cdf.txt`. The earlier `d76e61a` checker refuses 8-bit
   reports.
3. Store each report and its certificate as the deploy's evidence, keyed by the
   public setup's SHA-256, and record the gate as passed only when every table
   reaches the floor.

The open shard's directory and companion are rebuilt at every tip and count as
uncertified, as do provisional companions of sealed shards until they freeze.

| Table | Certified failure bits | Report |
|---|---:|---:|
| Directory at the seal target | 269 | 3 s |
| Companion, 2,048 rows | 254 | 2.3 s |
| Companion, 16,384 rows | 243 | 5.6 s |
| Companion, 65,536 rows full | 154 | 2.6 s |

## Protocol

Both tables use the shared `pir-native` 8-bit profile (P8): a 4,096-byte row is two
2,048-coefficient packing blocks, with 28-bit dithered queries, 22-bit published
masks and 10-bit responses. Query masks and packing setups derive from the schema,
the table kind and the row count. A session ID is a domain-separated hash of the
table kind, the manifest's revision and the public setup's digest, and a query's
session ID selects the table among every live inventory's segments.

Wallet routes are served at the canonical `/receiver/v1/<operation>`, and at the
deployed `/v1/receiver/<operation>` for existing clients; both reach the same handlers
and share metric labels. Operator routes (`/v1/receiver/health`, `/metrics`) have no
canonical counterpart.

| Route | Content |
|---|---|
| `GET /receiver/v1/inventory` | The current inventory head, at most 16 KiB |
| `GET /receiver/v1/inventory/:inventory/:chunk` | One chunk of a live inventory's shards; 410 once not served |
| `GET /receiver/v1/shard-filters/:digest` | A live sealed shard's filter by digest |
| `GET /receiver/v1/choice/:digest` | A live segment's choice object by digest |
| `GET /receiver/v1/public/:session` | Public P8 setup of a directory or companion session |
| `POST /receiver/v1/query` | One encrypted directory or companion row query |
| `GET /receiver/v1/block-filters/head` | Block hint availability |
| `GET /receiver/v1/block-filters/:revision/:from/:limit` | A sparse range of block hints for a wallet's catch-up gap |
| `GET /v1/receiver/health` | Query process identity and the served inventory (management listener only) |

A query holds the request magic, the session ID, a fresh 16-byte nonce, the packing
key and the encrypted row selection. The response echoes that 52-byte header. The
receiver never appears in a route or header. A directory query covers its 2,048
rows. A companion query covers every row or exactly its `query_rows` prefix: its
occupied rows rounded up to 2,048-row blocks, at least 2,048, so an empty companion
answers one zero block. Any other length is refused before the query waits for
evaluation: 413 past the full length, else 400. A session not being served, whether
unknown, revoked or expired, returns 410. Every response on these routes, refusals
included, is `Cache-Control: no-store`, since a revoked session ID serves again if
the same publication is republished. A query revoked while it is evaluated is 410; a
response already being sent completes, and wallets revalidate their anchor. Queries
are admitted with the primitives Enhance uses (`pir_control::admission`): two in
flight per client, then a wait of up to 2 seconds for one of two evaluation slots. A
client at its cap or a full server gets 429 with `Retry-After: 1`.

`/v1/receiver/health` reports the process identity that
[the serving contract](../serving-contract.md) defines for every PIR server,
and `/metrics` the shared HTTP observations by route category. Both are for
operators: a deployment's edge proxies only the wallet routes.

## Trust model

PIR hides which receivers a wallet looks up. It does not authenticate the chain.
A wallet accepts a publication only at a block of its own chain, as
`AcceptedCoverage` (genesis, required history start, terminal height and hash),
and revalidates that anchor if its chain changes during a lookup. It trial
decrypts each payment with its own key and checks witness payloads against its own
tree. A directory match proves neither
ownership nor spendability. An indexer can still omit payments, so a missing entry
means no indexed payment in that segment, not an unused address, and a wrong last
height can hide one as an omitted entry could. Missing
or inconsistent pages and transport failures are errors, never absence or a
cleartext fallback.

## Running the publisher and query server

Continuous ingestion runs through `pir-publisher --config FILE` with the Receiver
feature and product section enabled. The root chain feed owns tip observations,
RPC configuration and scheduling; Receiver applies its default two-block depth.
The existing SQLite index, correction revision, witness checkpoints and sealed
material remain in `data_dir`. Optional payout audit credentials stay only on the
publisher. Its private product listener exposes HTTPS distribution behind the
edge, rather than wallet query routes.

The optional NEAR audit checks NEAR's own records against the index, so operators
hear if NEAR stops sending in a way wallets can find (zero OVK, Ironwood). It never
depends on NEAR's Zcash addresses. A reported payout must be paid in the transaction
NEAR names. A refunded swap out of ZEC, or a successful one with an excess refund
(a second origin transaction), must have at least one reported origin transaction
paying its refund receiver. An unmatched excess refund counts only once 1Click's
status confirms a refund was made. A found item is rechecked for two hours, then
retired, so the audit never rechecks history; index completeness is the canary's.
`/operator/receiver/health` reports the counts that pir-apm alerts on
([conditions](../enhance/observability-alerting.md)).

Queries run through `pir-query-server --config FILE` with the Receiver feature.
Use the [Receiver-only examples](../../tools/receiver/deploy/) or add the Receiver
section to the root query configuration. The query host consumes publisher HTTPS
only and needs no node or payout-audit credentials. Root `--check` validates the
configuration offline; it never opens state or grants serving authority.

One-shot maintenance uses `pir-publisher receiver publish --data-dir PATH
--rpc-url URL --no-auth` (or a node cookie). It prepares the same publication files
without a continuous runtime. `receiver-probe` remains the independent encrypted
payment and filter diagnostic; it reads directories and companions over PIR with
wallet-libraries' client. Its optional `--health-url` checks a
private publisher audit report at `/operator/receiver/health` when one is available; it never sends node credentials
to the query service.

The root public listener mounts only wallet operations under `/receiver/v1/` and
`/v1/receiver/`. `/v1/receiver/health` is available only on the root management
listener. Production cutover is a separate approved transactional deployment; the
old continuous and operator-staged serving commands have been removed.

## Wallet use

For each gap since its last check, a wallet reads the hints from where it last read them through its scanned tip, and a hit queues a PIR lookup for that address. A gap the window no longer covers falls back to a complete PIR check of every address it watches. A complete check reads, in every segment of every shard from the address's coverage onward, one directory query on the row the segment's choice object names. A miss, or a hit whose last height is below where the lookup starts or the account's birthday, reads nothing more. Otherwise it reads one companion query per record, for slots `first..first+count`, and checks each record against the entry: receiver, tag, page, total, ordinal and last height. A tag hit with another receiver fails the step closed. Capture one inventory per pass and accept its end hash and history correction revision before reading. Sealed shards' filters may stand in for PIR when they exclude a receiver. PIR hides the row, not which shards, segments or companions a wallet queries; that metadata privacy assumes an honest publisher. The server sees one directory query per segment and the number of records on a hit. There is no public row-file route.

## Tests

`cargo test -p receiver-store` covers store restart and rollback, commitment ranges,
completed Ironwood shards, sealed shards, corrections and the version 3 upgrade.
`cargo test -p receiver-pir-server` covers preparation charges at the minimum budget,
racing cold builds, cancellation during preparation, retention and revocation, and
the inventory's capture and setup budgets. `cargo test -p pir-query-server --features
receiver` covers HTTP admission (full and trimmed companion queries, wrong prefixes,
a directory query of the wrong length, a nonzero omitted row, another session's
answer, zero-payment publications), the choice route, expired sessions and HTTPS
synchronization. `cargo test -p receiver-indexer` covers sealing (capacity, target,
height offset, the 65,535-payment limit, oversized blocks of up to eight segments,
placement failure, corrections, and identical bytes across publication cadence and
restart, staged seals and their deciding blocks), segment checks, incremental
payloads, and, with `qualification-tests`, sealed shards served end to end through
freeze and reorgs. The forced tag collisions, retry budgets and choice-seed exhaustion
need wallet-libraries' test hooks and are tested there.

`cargo run -p receiver-pir-server --example measure` reproduces the measurements
above. Recovery follows `zcash/zips@afa086bd976e316612a5c06fb139429958d07d84`,
NU6.3 proposal, section 4.19.3 (`decryptovk`).

## Evidence

Pre-deployment mainnet runs are indexed in [the evidence README](../../evidence/receiver/README.md).

### HTTPS query host

Build `pir-query-server` with `receiver` to consume the Receiver provider over
HTTPS, either alone or alongside the other product features. Enable a `receiver`
section with `publisher`, optional `publisher_ca`, absolute `state_dir`, expected
`genesis` bytes and `activation_height`; set the root `public_listen` as usual.
`poll_seconds`, `bootstrap_max_bytes`, `table_memory_mib` and `retention_seconds`
control synchronization, download size, resident tables and retained inventories.
The default table budget is 4096 MiB and retention is 600 seconds. Node and payout
audit credentials belong only to the publisher.

Only wallet operations under `/receiver/v1/` and `/v1/receiver/` join the public
listener. Aggregate readiness remains on the root management listener. Cached
objects and saved active metadata never authorize serving after a restart: the
host must obtain fresh publisher authority first. A transient publisher outage
preserves already approved answers; a withdrawal, recovery fence or negative
canonical answer invalidates those answers, including work already in flight.


### Root-binary qualification

Run these filesystem-fault campaigns as an unprivileged user. They verify that
directory writes are actually denied before starting the publisher and reject
execution contexts that bypass that protection.

The existing unified HTTPS campaign also covers Receiver:

```sh
python3 tools/transparent/scripts/unified-http-sync-campaign.py --products receiver
python3 tools/transparent/scripts/unified-http-sync-campaign.py --products unified
```

The Receiver case runs a root publisher and two independent root query servers.
It checks encrypted payments and payloads against the synthetic chain, rolling
hints while table writes cannot finish, retained inventories, publisher outages,
missed recovery and correction, failed durable writes on both sides,
cancellation, reorgs and graceful shutdown. The unified case adds Receiver to
history, transaction details, Status and Enhance on one public listener, and
checks that a Receiver withdrawal closes aggregate readiness while the other
products keep answering. Private routes stay off the wallet listener.

The runner requires exactly one passing test for each selected case and retains
source and binary identities, logs and synthetic inputs under `.local-pir/`.
Reported resident memory values are samples on the qualification host; table
budget tests and production capacity qualification remain separate evidence.
These local synthetic campaigns do not perform production acceptance or a soak.
