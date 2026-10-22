# Receiver directory

The receiver directory lets a restored wallet find Ironwood payments to keys it
cannot rescan for, such as its one-time IVK refund and incoming keys. It maps each
canonical 43-byte Ironwood receiver to the payments recoverable with the all-zero
outgoing viewing key (OVK). Coinbase recipients are excluded. Every Ironwood
Action still counts toward note positions, including coinbase and unrecoverable
outputs.

The canonical fixed record codec is `zakura-receiver-crypto::wire` in
wallet-libraries. `zakura-receiver-directory` (crate `receiver_directory`) uses it
with cryptography disabled to hold records, publications and rolling block
filters; it parses structure only. The indexer's SQLite chain index and optional
payout audit store are this repository's `receiver-store` package. The same
library's `crypto` feature checks receivers,
unified addresses and record fields, recovers receivers with the zero OVK, and
builds lower inclusion paths for companion witness tables. The indexer enables these checks before storing
a payment and before serving a publication, and a wallet checks a retrieved
payment before crediting it. Dependencies run one way from wallet-pir to
wallet-libraries; wallet-libraries never consumes wallet-pir packages.
`receiver-pir` holds the PIR client and the wallet `Transport` interface, plus
the evaluator with the `server` feature. `receiver-pir-server` serves
publications over HTTP. `receiver-indexer` builds them from a Zakura node; its
binary is `receiver-directory`. The service shares process identity and
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
canonical check and epoch-fenced atomic activation remain product owned; one-shot
publication writes the publication's files instead. Preparation failure keeps
committed progress for retry. `receiver-directory` configures these calls and
independently wires the HTTP server, canonical guard and optional payout audit;
the guard continues checking served anchors throughout both phases. This
standard-path adapter adds no HTTP source. The shared acquisition seam accepts
product work without owning a store, protocol, queue or authority ([lifecycle boundaries](../enhance/architecture.md#ingestion-application)).

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
commitment (32), ephemeral key (32), ciphertext prefix (52) and witness ordinal
(4). The ordinal is the record's rank by note position in its payment table and
selects its companion witness payload; it means nothing under another table. Integers are
little endian and hashes use protocol byte order. The Action input nullifier is
decryption context, not the received note's spend nullifier. Structural encoding and decoding check lengths, tags and page ranges. The
indexer and wallet additionally refuse a note commitment or Action input
nullifier that is not a canonical field element, or an ephemeral key that is not
a valid Ironwood ephemeral public key. A receiver's payments are ordered by note position, one per
page from zero, and every page repeats the total.

Rows are 4096 bytes and hold 14 records plus zero padding. A domain-separated
hash of the salt, the receiver's tag and the page selects a row. Publications
start at 8192 rows, the fewest that PIR clients and servers accept. A crowded
bucket retries up to 16 salts derived from the terminal hash, the first being
the hash itself, and only then doubles the table, up to 65536 rows. Overflow at
the maximum fails the candidate instead of dropping records. So do more records
than the table has slots, caught before placement; a supplied manifest claiming
more records than its slots is malformed. The manifest (profile
`ironwood-zero-ovk-receiver-v1`) binds the network, the history correction
revision, inclusive block coverage, boundary hashes, tree positions, geometry,
salt, record count and the SHA-256 of the rows. The immutable revision is a
domain-separated SHA-256 of every field at fixed width, as Enhance and Status
hash their manifests, and a manifest with a field this version does not know is
refused.

`Snapshot::validate` checks a supplied publication's structure whole, before a
server prepares it: the row digest, every slot and row padding, each record's
coverage and bucket, the exact record count, every receiver's pages, each continuing the last
in chain order as `snapshot::check_next` requires, and that no two records share an
output or note position. No chain trust is implied. A record stored outside its
bucket would otherwise make lookups of that receiver find nothing.

## Rolling block filters

`GET /v1/receiver/block-filters/head` advertises the network, history correction revision, first retained height and tip hash. `GET /v1/receiver/block-filters/:revision/:from/:limit` returns a nonempty consecutive batch, including empty blocks, limited to 256 blocks and 4 MiB. The batch may be shorter than requested. Pruned or stale requests return 410, future heights return 503 and invalid limits return 400. At most 8 batches are built or sent at once, and more return 429 with `Retry-After`. Every response is uncached.

Filters use the shared GCS codec with P = 20 and M = 1,569,792. Their salt binds network, height and hash independently of PIR row layout. The indexer builds them from indexed chain payments before witness or PIR preparation. Inventory retention does not hold back hints. The canonical guard revokes noncanonical hints along with affected sessions. Wallets independently check hashes and linkage, and query PIR after a match. A negative match needs no receiver query.

The rolling window spans seven days of target time: 8,064 blocks of 75 seconds before NU7, a blend across its activation, and 24,192 blocks of 25 seconds once the whole week is past it, following Common's NU7 schedule. `--filter-blocks` can cap it lower. Encoded hints in the in-memory window are also bounded to 64 MiB and advertises a shorter suffix if needed. It is rebuilt from the chain index on restart. This is not a permanent filter archive. No payment rows or commitments are pruned. Missing hints use direct PIR for unfinished addresses, while historical repeats on completed addresses may require the wallet's manual action.

Increase `--history-revision` after an indexing correction on an unchanged chain. The store refuses a lower revision after restart, and the same revision is bound into PIR publication identity. Advancing it immediately revokes older sessions before replacement preparation, including direct-PIR sessions. Reorgs use block hashes. A wallet must not settle a queued hit with an older history revision or a PIR frontier below the hit. Filters and PIR may become available at different times. Measure both availability lag and time to credit. Hints never delay ordinary wallet sync.

A publication is an inventory of Receiver shards, each with its payment tables, their companion witness tables and, once sealed, a shard filter (see [Sealed shards](#sealed-shards)). The global NEAR recent/seen sets, the aggregate snapshot envelope with its `.filters` file and the aggregate HTTP filter route are gone. Rolling block hints are the only filters, and nothing published is an issuance input.

## Witnesses

Each payment table has a companion witness PIR table (profile
`ironwood-zero-ovk-witness-v1`). Row `o / 8`, slot `o % 8` holds the 512-byte
payload for the record with ordinal `o`: its sixteen siblings inside its Ironwood
shard of 2^16 leaves, each supplied only when it is a complete subtree at the
table's tree size, the rest zero. The witness manifest binds the payment table's
revision, the correction revision, the payload count, geometry, the tree context
(height, hash and size) and the rows' digest. Rows
are the fewest supported that hold the payloads, from 8192; more payloads than 65536
rows hold publish nothing. A payload whose Ironwood shard has completed is final on
its chain; a provisional one is usable only with the wallet's own tree at exactly
the table's block. Wallets authenticate payloads against their own tree.

The store is `directory-v3.sqlite` in the data directory. An earlier release's
`directory.sqlite`, whose records lack the ordinal, is copied into it once with zero
ordinals that every snapshot reassigns, and is left unchanged so the earlier binary
can still be rolled back to.

Witnesses are built per Ironwood shard, never from the whole commitment history.
The store keeps every commitment, records each completed shard's root and the
block that completed it, and keeps every payment's final payload; a rewind past
that block removes them. Only the filling shard's paths are held in memory and
extended with each publication's new commitments; a restart or a rewind rebuilds
them from that shard's at most 65,536 stored commitments. Before serving, the
indexer checks that each record's payload supplies exactly the expected siblings
and that every final payload folds from its commitment to its shard's recorded
root.

`Store::correct` applies an indexing correction on an unchanged chain: it replaces
stored blocks' payments, checking hashes, positions and commitments against the
stored blocks, and advances the store's correction revision. A payment it adds to a
completed shard rebuilds that one shard's payloads from its stored commitments
under the recorded root, and sealed shards ending at or after the first corrected
block are unsealed so their boundaries are recomputed. Restart the service with the
same `--history-revision`.

## Sealed shards

History is split into contiguous Receiver shards covering every block, empty ones
included: sealed shards, whose payment tables never change under one correction
generation on one chain, then a recent tier rebuilt each publication. The recent
tier's table starts at half occupancy and grows as a single table always has.

A Receiver-specific sealer follows Transparent's rules on what a shard holds. Every
sealed segment uses the pinned `--shard-rows` geometry (default 65536, at least
`--min-rows`) with salts
fixed by the block before the shard, so the sealer projects placement exactly as
blocks arrive. A block stays in the open shard only if some salt still places every
payment and the companion table still holds one payload per payment (524,288 at the
largest witness geometry). A block that would breach either seals the shard before
it; reaching `--shard-target` payments (default one seventh below the smaller
capacity) seals after it. Seals fall on block boundaries. A block too large for one
table seals the open shard, then becomes a shard of its own split into up to eight
segments, each of which places; a block needing more publishes nothing, so coverage
never passes it. The store records each seal with the geometry and witness capacity
it used, a rewind past a shard's end removes it, and restart replays only the open
shard, so a restart rebuilds identical tables. Raising `--min-rows` above the rows
of an existing seal publishes nothing until it is lowered again or the index is
rebuilt, since serving refuses tables below it.

A sealed shard's companion tables are rebuilt at each publication's tip until every
payment's Ironwood shard has completed, reusing final payloads, then frozen at the
later of the shard's end and the block completing the last of them. A frozen
companion depends on that block, so a reorg replacing it rebuilds the companion of
an unchanged payment table. Any reorg moves a served inventory's anchor off the
chain, and the guard revokes every session before a replacement is served.

Each sealed shard keeps one GCS receiver filter (`ironwood-receiver/v1/shard-filter`,
P = 20, M = 1,569,792, up to 2^23 receivers and 24 MiB) for as long as its history is
served. It is keyed to the shard's coverage and correction generation, and its
inventory descriptor binds its digest, size and count. The recent tier has none.

The inventory head names the network, correction generation, coverage and the
inventory identity, which binds every shard's coverage, segment session IDs and
filter in order. Chunks list 16 shards each. A displaced inventory keeps serving its
head's chunks, sessions and filters for `--retention-seconds` (default 600, ten
minutes) so a wallet that captured it can finish a restore pass; at most 16 are
retained, and a publication waits for one to expire rather than drop one early.
Sealed shards' sessions are shared between inventories, so retention costs only
displaced recent tiers and companions.

Prepared tables do not stay resident. Each table is written once to the data
directory's `tables/` beside its session manifest and public setup, named by its
revision, and inventories hold only those descriptions. A byte-budgeted cache
(`--table-memory-mib`, default 4096) keeps the most recently used prepared tables,
counting tables that retained inventories' and in-flight queries still use, and
prepares an evicted table again from its files when a query needs it. When tables in
use fill the budget a query gets 429 until they finish. A witness table prepares
beside its payment table, so the budget must hold the largest pair, 1536 MiB, and a
smaller one is refused at startup. A restart finds every unchanged table by
revision and prepares none until it is queried, and tables no live or kept inventory
uses are deleted once no request that selected them is still running. Sealed shards' filters are written there too, named by digest:
every inventory naming a filter shares one source, and its bytes are read into the
same budget only when a request serves them. Staged serving reloads evicted tables
and filters from the staged files.

## Protocol

The protocol `ironwood-receiver-pir-v1-two-mask-m29` is the shared `pir-native`
profile that Enhance and Transparent also use: a 4096-byte row is one
2,048-coefficient packing block, as in Transparent. The query masks and packing
setup derive from the protocol and the row count. At 8192 rows a query is 77,876
bytes, a response 5,684 and the public setup 14,848. A session manifest holds the
directory manifest, the protocol and the SHA-256 of the public setup, and refuses
unknown fields. A domain-separated hash of the protocol, the directory revision and
that digest is the session ID. The witness session is formed the same way from the
witness manifest under its own domain. Every segment's two sessions share
`/v1/receiver/query`, and a query's session ID selects the table among every live
inventory's segments.

Wallet routes are served at the canonical `/receiver/v1/<operation>`, and at the
deployed `/v1/receiver/<operation>` for existing clients; both reach the same handlers
and share metric labels. Operator routes (`/v1/receiver/health`, `/metrics`) have no
canonical counterpart.

| Route | Content |
|---|---|
| `GET /receiver/v1/inventory` | The current inventory head, at most 16 KiB |
| `GET /receiver/v1/inventory/:inventory/:chunk` | One chunk of a live inventory's shards; 410 once not served |
| `GET /receiver/v1/shard-filters/:digest` | A live sealed shard's filter by digest |
| `GET /receiver/v1/public/:session` | Public PIR setup of either session |
| `POST /receiver/v1/query` | One encrypted payment or witness row query |
| `GET /receiver/v1/block-filters/head` | Rolling hint availability |
| `GET /receiver/v1/block-filters/:revision/:from/:limit` | Bounded consecutive block filters |
| `GET /v1/receiver/health` | Process identity, the served inventory, the indexer's report and this process's NEAR reads (operators only) |

A query holds `RPQ1`, the session ID, a fresh 16-byte nonce, the packing key and
the encrypted row selection. The response echoes that 52-byte header. The
receiver and page never appear in a route or header. A session not being served,
whether unknown, revoked or expired, returns 410, a query longer than its
session's 413 and a shorter or otherwise malformed one 400; the length is
checked before the query waits for evaluation. Every response on these routes,
refusals included, is `Cache-Control: no-store`, since a revoked session ID
serves again if the same publication is republished. A query revoked while it is
evaluated is 410; a response already being sent completes, and wallets
revalidate their anchor. Queries are admitted with the primitives Enhance uses
(`pir_control::admission`): two in flight per client, then a wait of up to 2
seconds for one of two evaluation slots. A client at its cap or a full server
gets 429 with `Retry-After: 1`.

`/v1/receiver/health` reports the process identity that
[the serving contract](../serving-contract.md) defines for every PIR server,
and `/metrics` the shared HTTP observations by route category. Both are for
operators: a deployment's edge proxies only the wallet routes.

## Trust model

PIR hides which receivers a wallet looks up. It does not authenticate the chain.
A wallet accepts a publication only at a block of its own chain, as
`AcceptedCoverage` (genesis, required history start, terminal height and hash),
and revalidates that anchor if its chain changes during a lookup. It trial
decrypts each payment with its own key and checks witness paths against its own
tree root, never only the file's root. A directory match proves neither
ownership nor spendability. An indexer can still omit payments, so an empty page
zero means no indexed payment in that publication, not an unused address. Missing
or inconsistent pages and transport failures are errors, never absence or a
cleartext fallback.

## Running the indexer and server

```sh
cargo build --locked --release -p receiver-indexer --bin receiver-directory
receiver-directory --data-dir /srv/receiver-pir/index --rpc-url http://127.0.0.1:8232 \
  --cookie /path/to/.cookie --serve --min-rows 8192
```

`receiver-directory --help` and the `Config` documentation in
`src/receiver/receiver-indexer/src/service.rs` describe every flag. The indexer requires
mainnet and indexes from Ironwood activation to `--depth` (default 2) blocks below
the tip of the freshest mainnet `--rpc-url` node, which runs the whole pass and
must still be on mainnet to publish; the next poll checks each node's genesis and
ranks them again. `--no-auth` replaces
`--cookie`, which is read on every request. Each batch of raw blocks is checked
against the saved parent, heights, merkle roots, Action positions, terminal hash and
tree size before it is stored. A restart or reorg rewinds to the last saved
canonical block; a reorg below the start height needs a rebuild in a new directory.

Without `--serve`, one run writes its publication's files under `publications/` and
prints a summary. With `--serve`, it polls every `--poll-seconds`, prepares each
canonical tip in memory and serves on `--bind`, a loopback or private address
behind a TLS proxy. At a paused tip the publication keeps serving, and the
optional audit's latest report replaces only the report health serves with it. A
guard revokes every session once a node shows a served anchor is off its
chain. A replaced inventory stays available for `--retention-seconds`. Logs go to standard error, filtered by `RUST_LOG`. See [the DigitalOcean deployment](../../tools/receiver/digitalocean/README.md).

With an optional partner key in `NEAR_INTENTS_EXPLORER`, the server audits completed ZEC payouts in `providers.sqlite`. The audit retains reported receiver/transaction pairs and a bounded feed cursor, not a global list of issued addresses. Its report counts missing or uncheckable payouts. It does not gate indexing, hints, PIR publication or wallet issuance, and without a key the service runs without it. An audit read error leaves its cursor unchanged. Inspect health's `near.reads` when verifying the partner key separately.

### Serving operator-staged publications

`receiver-directory serve-publication --config FILE` serves publications an
operator stages, from the same binary, without indexing: it opens no index
database, ingests no blocks and reads no partner key. The JSON configuration
(unknown fields refused) names `publication_record`, `bind` (loopback or
private), `state_dir`, `rpc_urls`, and `cookie` or `"no_auth": true`, with the
optional `poll_seconds` and `min_rows` of the indexer. The record
is `{"directory": "/abs/path", "revision": "<64 hex>"}`: stage the complete
directory a one-shot run writes, a `<layout>.inventory.json` listing each shard's
segments by payment and companion table revision and its filter by digest, each
payment table's `<revision>.json` and `.rows`, each companion's own
`<witness revision>.witness.json` and `.witness`, and each filter's
`<digest>.filter`, then point the record at the layout's identity, the SHA-256 of
its bytes, atomically (write beside it, rename over it). It is a private local control input; there is no update route.

Staged serving publishes the immutable PIR directory and proofs only. Rolling hint routes return unavailable, so wallets use direct PIR. Run its probe with `--without-block-filters`. Indexing deployments retain the default probe, which requires live hints. No aggregate `.filters` file is produced or loaded.

Each record is verified in full before it can serve: the layout must digest to the
recorded identity and be for mainnet, every manifest must hold its listed revision
and at least `min_rows`, the rows must match their digest and every record must
check, every companion witness table must bind to its payment table and supply each
record's payload as the indexer checks it, every filter must hold its digest, and
the shards must form one contiguous inventory from Ironwood activation. The
server library re-validates the rows record by record while preparing PIR. Its anchor must then be canonical on the freshest node. Updates
are serial: a newer record supersedes one still preparing, a record that does
not verify is refused while the last valid publication keeps serving, and the
publication rules above (epochs, monotonic coverage, retention of displaced
inventories) are unchanged. The canonical guard runs independently
throughout and revokes on a confirmed reorg; an outage keeps serving. A revoked
but still-recorded publication is verified and confirmed again. A restart
re-reads the record and verifies everything before serving; the
`serve-publication.json` status file in `state_dir` is a report, never an input.
`history-revision.json` beside it records the highest history revision accepted,
written before a correction serves, and a record below it is refused, so a restart
never serves older, uncorrected history again. Deleting it is an explicit rollback.
Health's `publication` field reports the assigned, loaded and preparing records
and the latest refusal, typed as `invalid_source`, `unavailable`,
`noncanonical` or `superseded`. `--check` runs the same verification of the
recorded publication and exits without serving. The publisher remains trusted
for completeness. Operators install it as the `receiver.serve-publication`
external worker ([operations](../operations.md#external-workers)).

`receiver-directory probe --origin <url> --health-url <private health URL>
--rpc-url <node> --no-auth`, or `receiver-probe` with the same
arguments on the monitor host, is a `pir-monitor` service probe with an embedded
mainnet fixture. It checks the served inventory head's anchor against independent
nodes, captures every chunk, checks the fixture shard's filter when it is sealed,
looks up a pinned historical payment in that shard's segments over live encrypted
PIR, reads its companion payload over live encrypted PIR by the record's ordinal, and validates current block hints against the node. A final payload must fold from the fixture's commitment to the node's root for that Ironwood shard (`z_getsubtreesbyindex`). A provisional one, for a shard the chain has not completed, has no independent oracle here, so the probe reports it `oracle_unavailable` rather than passing. Retained fixtures must match their block's filter. A pruned fixture is still recovered through PIR. Provider feed freshness is not a deployment gate. `--max-lag` defaults to 12 blocks.

## Wallet use

`receiver_pir::transport::Inventories` and `InventoryClient` use the caller's routing, cancellation and response limits. Fetch bounded block hints when cheaper than direct lookups, match receivers locally and use PIR for positives. Otherwise query unfinished addresses directly. Capture one inventory per pass and accept its end hash and history correction revision before reading. Sealed shards' filters may stand in for PIR when they exclude a receiver. `page` reads one history record per encrypted query in one segment and `witness` reads a payment's companion payload by its record's ordinal, also over PIR. PIR hides the row, not which shards, segments or witness sessions a wallet queries; that metadata privacy assumes an honest publisher. Reuse setup where supported. The server can observe query counts and timing, but the receiver stays private. There is no public row-file route.

## Tests

`cargo test -p receiver-store` covers publications and store restart and
rollback, commitment ranges, completed Ironwood shards, sealed shards and
corrections.
`cargo test -p receiver-pir-server` tests the HTTP service with the canonical
library client. `cargo test -p receiver-indexer` covers indexing with synthetic
blocks, sealing on placement, witness capacity and target with oversized-block
segments, sealed shards served through the library client with filters and
witnesses, restart reconstruction, reorgs across a seal and of witness
dependencies, corrections older than filter retention, incremental witness payloads
against fresh rebuilds across shard completion, restart, reorg and correction, refusal of invalid records and payloads
before storing or serving, payloads read over PIR through the library client and
checked against an independent tree, serving from memory and the probe's encrypted
lookup.

The [receiver packages in wallet-libraries](https://github.com/zakura-core/wallet-libraries/tree/98927effb14aa71134ccc6c088f7a27f18ebdf30/zakura)
own record and address validation, public mainnet refund recovery, independent
tree witnesses, encrypted round trips at every geometry, and fixed protocol
seeds, framing and row placement. Run those tests in that repository with its
`pir-receiver` and `receiver-pir-server` development configurations. Recovery follows
`zcash/zips@afa086bd976e316612a5c06fb139429958d07d84`, NU6.3 proposal, section
4.19.3 (`decryptovk`).

## Evidence

Pre-deployment mainnet runs are indexed in [the evidence README](../../evidence/receiver/README.md).
