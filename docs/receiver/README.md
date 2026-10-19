# Receiver directory

The receiver directory lets a restored wallet find Ironwood payments to keys it
cannot rescan for, such as its one-time IVK refund and incoming keys. It maps each
canonical 43-byte Ironwood receiver to the payments recoverable with the all-zero
outgoing viewing key (OVK). Coinbase recipients are excluded. Every Ironwood
Action still counts toward note positions, including coinbase and unrecoverable
outputs.

The canonical fixed record codec is `zakura-receiver-crypto::wire` in
wallet-libraries. `receiver-directory` uses it with cryptography disabled to hold
records, publications and, with the `store` feature, the indexer's SQLite store;
it parses structure only. The same library's `crypto` feature checks receivers,
unified addresses and record fields, recovers receivers with the zero OVK, and
builds and checks witness files. The indexer enables these checks before storing
a payment and before serving a publication, and a wallet checks a retrieved
payment before crediting it. Dependencies run one way from wallet-pir to
wallet-libraries; wallet-libraries never consumes wallet-pir packages.
`receiver-pir` holds the PIR client and the wallet `Transport` interface, plus
the evaluator with the `server` feature. `receiver-pir-server` serves
publications over HTTP. `receiver-indexer` builds them from a Zakura node; its
binary is `receiver-directory`. The service shares process identity and
admission (`pir-control`), metrics (`pir-observability`) and the PIR profile
(`pir-native`) with the other services, and reads the node through
wallet-libraries' server-only chain adapters (`zakura-server-chain`), which decode
blocks, count note positions and recover receivers.

The indexer keeps validated chain ingestion on a dedicated filter worker. PIR preparation reads a committed prefix on its own worker, so proof construction and session-rotation grace do not delay filter publication. Both workers validate their publication anchors. Independent guards revoke orphaned archive heads and PIR sessions. Provider history is separate from paid-filter coverage. A failed or incomplete provider read suppresses seen issuance evidence without holding back block filters.

## Node RPC policy

The current indexer source uses wallet-libraries' server-only chain adapter with
a 20-second connection timeout and a 120-second total request timeout. The total
deadline includes connection establishment and response reading. The RPC cookie
is reread for every request; HTTP 401 remains an HTTP error. Responses retain the
chain-derived bound for a maximum-size verbose block and are streamed within
that bound even without Content-Length. The transport does not retry or split
batches. This source policy is independent of the existing deployment decision
to retain the prior Receiver release.

## Records and publications

A 285-byte record holds version (1 byte), receiver (43), page (4), total pages
(4), present flag (1), height (4), block hash (32), txid (32), transaction index
(4), Action index (4), note position (8), Action input nullifier (32), note
commitment (32), ephemeral key (32) and ciphertext prefix (52). Integers are
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
`ironwood-zero-ovk-receiver-v1`) binds the network, inclusive block coverage,
boundary hashes, tree positions, geometry, salt, record count, the filter sets
and the SHA-256 of the rows and of the filter file. The immutable revision is a
domain-separated SHA-256 of every field at fixed width, as Enhance and Status
hash their manifests, and a manifest with a field this version does not know is
refused.

`Snapshot::validate` checks a supplied publication's structure whole, before a
server prepares it: the row and filter digests, the declared filter sets and a paid set of exactly
the records' receivers, every slot and row padding, each record's coverage and
bucket, the exact record count, every receiver's pages, each continuing the last
in chain order as `snapshot::check_next` requires, and that no two records share an
output or note position. No chain trust is implied. A record stored outside its
bucket would otherwise make lookups of that receiver find nothing.

## Block filter archive

`archive.sqlite` retains the complete canonical per-block GCS history, including empty blocks, in authenticated 256-block pages. Keep it with the publisher during failover. Its archive identity, correction epoch and monotone seen union are continuity state. Losing it requires a new identity and wallet bootstrap. Never copy an old identity into a new database or restore an older epoch under an existing identity. `directory.sqlite` remains the rebuildable chain index.

Every indexed block hash has a persistent fingerprint of its complete ordered payment records. These comparison baselines survive shortened rebuilds and intervening forks, including branches that return later. They are continuity state, separate from obsolete page versions that GC can remove. A changed record on an unchanged block hash increments the durable epoch, including a correction below a simultaneous fork and a correction that adds the first payment to an empty block. A changed block hash alone replaces the affected pages without changing the epoch. Staging and activation are separate. The node must still accept the frontier before the staged head becomes public.

Canonical history has no age cutoff. Superseded heads and their unique objects remain for at least 180 seconds after retirement, and active/staged heads are protected. A range read pins a SQLite snapshot until its bounded response is buffered. Two download slots cover database work and response bytes through their last transport owner. Unknown/expired revisions return 410. Corruption, missing storage and exhausted capacity return an error, never an empty filter. The range index is capped at 65,536 pages. Exceeding that limit stops publication instead of truncating history.

PIR coverage declarations bind the archive identity and epoch to the actual session and connected block frontier. They are written only for an indexed anchor under the epoch captured before preparation. Current, grace and staged sessions are protected from concurrent cleanup. A wallet must still validate the connected chain and its local rewind/settings generations before settling work.

`--filter-depth` can select a shallower filter depth than `--depth`. Omitting it keeps the existing depth. This code does not change production settings. The `receiver filter ready` event records observed node tip, published filter height, configured depth and ingestion duration. `receiver filter publication` records filter build duration and epoch. Qualification must combine these with wallet receipt/processing timestamps to report p50, p95, p99 and worst delay, scanned-block distance, cache-hit and PIR-fallback rates. Separate direct issuer-watch hits. A three-block wallet cache is useful only when filter processing beats eviction at height `h+3`; eventual discovery alone is not latency qualification.

## Filters

Each publication carries an `IWFLT1` filter file of labeled BIP 158 Golomb-coded
sets of receivers, keyed by the salt, so a wallet can test its receivers before
looking any up. The manifest declares every set's label and size:

- `paid`: every receiver with a payment in the publication.
- `<provider>/recent`: every Orchard receiver a swap provider was given within the
  window the manifest declares, as a payout or refund address, in any status.
- `<provider>/seen`: every Orchard receiver the provider was given since its feeds'
  history begins, which the manifest also declares, as a payout or refund address.
  Wallets check it before issuing an address, so two devices of one seed don't
  give the provider the same one.

For each provider set the manifest also declares when the feed's last complete read
began, so a wallet can tell how current the set is. A recent window reaches back
from that read.

The indexer publishes `near-intents/recent` (24 hours) and `near-intents/seen`
from the NEAR Intents explorer. It adds the recent set once the payout and refund
feeds both have live coverage, and the seen set only once both feeds' history also
reaches that coverage without a gap ([NEAR feed history](#near-feed-history)).
Wallets use the sets they recognize and ignore the rest, so a new provider needs no
format change.

Sets are built with `bitcoin::bip158`, as Transparent's filters are, with P = 10
and M = 1,533 and SipHash keys from a domain-separated SHA-256 of the salt, so a
receiver outside a set matches it about once in 1,533 tests. The file holds the set
count, then for each set in label order its label, byte length (little endian) and
BIP 158 encoding (a CompactSize count and the coded deltas), about 1.6 bytes per
receiver, and is at most 8 MiB. A reader decodes every value and checks the
padding before matching. Every wallet downloads the same file, so testing it
reveals nothing. The publisher is trusted for the sets' completeness, as for the
rows.

## Witnesses

The `IWPROOF1` witness file has a 152-byte header binding the genesis, revision,
terminal height and hash, tree size and root. Sorted 37-byte nodes (level,
index, hash) follow, with every sibling of each payment position. It is capped
at 64 MiB. The indexer serves a file only once it has checked a path to that root
for every record in the rows. Building the file reads every commitment
since the empty tree, so the store refuses a history longer than the caller's
limit, the indexer's `--max-witness-commitments`, before reading it.

## Protocol

The protocol `ironwood-receiver-pir-v1-two-mask-m29` is the shared `pir-native`
profile that Enhance and Transparent also use: a 4096-byte row is one
2,048-coefficient packing block, as in Transparent. The query masks and packing
setup derive from the protocol and the row count. At 8192 rows a query is 77,876
bytes, a response 5,684 and the public setup 14,848. A session manifest holds the
directory manifest, the protocol and the SHA-256 of the public setup, and refuses
unknown fields. Its compact JSON, as `/v1/receiver/init` serves it, is at most 16
KiB: a server refuses to prepare a larger one, and a client reads no more. A
domain-separated hash of the protocol, the directory revision and that digest is
the session ID.

| Route | Content |
|---|---|
| `GET /v1/receiver/init` | Current session manifest |
| `GET /v1/receiver/public/:session` | Public PIR setup |
| `POST /v1/receiver/query` | One encrypted row query |
| `GET /v1/receiver/witness/:session` | Common witness file |
| `GET /v1/receiver/filters/:session` | Filter file |
| `GET /v1/receiver/archive/head` | Current complete archive head, independent of PIR readiness |
| `GET /v1/receiver/archive/range/:head/:page` | Immutable page and proof under a pinned head digest |
| `GET /v1/receiver/archive/seen/:digest` | Complete monotone seen object |
| `GET /v1/receiver/archive/coverage/:archive/:epoch/:session` | Exact PIR session coverage declaration |
| `GET /v1/receiver/health` | Process identity, the served session, the indexer's report, this process's NEAR reads and capacity refusal (operators only) |

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
  --cookie /path/to/.cookie --serve --witnesses --min-rows 8192
```

`receiver-directory --help` and the `Args` documentation in
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
behind a TLS proxy. A paused tip is republished when its provider sets or report
change. A guard revokes every session once a node shows a served anchor is off its
chain. A replaced PIR revision stays available for 60 seconds, and the next PIR rotation waits for that. Filter ingestion and publication continue during this grace period. Logs go to standard error, filtered by `RUST_LOG`. See [the DigitalOcean deployment](../../tools/receiver/digitalocean/README.md).

With a partner key in `NEAR_INTENTS_EXPLORER`, the server reads the NEAR Intents
explorer into `providers.sqlite` and publishes the `near-intents` recent and seen
sets. Without one, publications carry no provider sets. Serving with a key requires
`--near-since`. Health's `indexer` report, activated with each publication, gives
each feed's last live read (`feeds`), its history (`history`, see below), the
publication's [capacity](#capacity) and `payouts_missing`,
completed NEAR payouts with no indexed payment: the signal that the index missed
one or NEAR stopped paying with the zero OVK. `payouts_uncheckable` counts those
to an Orchard receiver first seen in the last day that NEAR reported without a
parsable transaction. Payouts to other recipients are not checked.
`src/receiver/receiver-indexer/src/near.rs` documents the feed and the report.

Health's `near.reads` gives, for each NEAR feed, when the last read this process
completed began, or null until one completes, so unlike the report's `feeds` it
shows that the running process's key works.

### NEAR feed history

Each poll first reads both feeds live: the newest swaps from the feed's cursor back
an hour, for swaps the explorer lists late, but never more than a day, which keeps
the recent set fresh. A live read that cannot reach back to its cursor's hour, as
the first does or one after an outage of over a day, restarts the feed's live
coverage there.

History comes from sweeps, which walk a feed from its newest swap down to a floor,
`--near-sweep-pages` (default 30) pages a feed each poll between two passes. Each pass that is running gets at
least half of them and any the other leaves, so neither starves the other. Each page
is recorded with where the next one starts, so a sweep resumes after a failed request
or a restart and no history is too long to read.

- The full pass reads down to the history's start. The first backfills to
  `--near-since`: set it before the first one-time address a supported wallet
  issued. An earlier `--near-since` later backfills the difference, and a later
  one never shrinks the history. After that a full pass begins a day after the
  last one began, so a swap the explorer lists or corrects however long after its
  creation is recorded within about a day plus the pass's own length, which grows
  with the history (about an hour for a month at October 2026 volume).
- The recent pass begins an hour after the history's end and rereads the week
  before it, so a swap the explorer listed after the live reads passed it, or that
  a read skipped, such as one listed without its address, is recorded within about
  an hour when it is less than a week old.

A feed's history is the span its completed sweeps cover without a gap, `since` to
`through`, `full` is when the last full pass began and `full_secs` how long it took. Every swap created in that
span that the explorer listed, in its current form, within a week of its creation or
before `full` is recorded. The report's `history` gives these for each feed, null
until the first sweep completes, with each pass in progress (`sweeps`: its floor,
start, the oldest creation time it reached and its pages). They are historical
completeness, apart from the live freshness `feeds` gives. The seen set is published
only once both feeds' history reaches their live coverage, and declares the
history's start. The probe fails `stale_history` when either feed's `through` is
more than six hours old, or its `full` older than twice `full_secs` plus a day, and
never for a `full` under three days old: a long history's pass is slow but healthy.

The stored receivers are an append-only union. Reorgs never touch
`providers.sqlite`, later reads and corrections only add, and the database refuses
deleting a receiver. A new backfill need not reproduce the union, since the
explorer can drop or correct a swap, so back the file up and restore it rather than
rebuild it.

### Serving operator-staged publications

`receiver-directory serve-publication --config FILE` serves publications an
operator stages, from the same binary, without indexing: it opens no index
database, ingests no blocks and reads no partner key. The JSON configuration
(unknown fields refused) names `publication_record`, `bind` (loopback or
private), `state_dir`, `rpc_urls`, and `cookie` or `"no_auth": true`, with the
optional `poll_seconds`, `witnesses` and `min_rows` of the indexer. The record
is `{"directory": "/abs/path", "revision": "<64 hex>"}`: stage the complete
directory a one-shot run writes (`<revision>.json`, `.rows`, `.filters`,
`.witness`), then replace the record atomically (write beside it, rename over
it). It is a private local control input; there is no update route.

Each record is verified in full before it can serve: the manifest must digest
to the expected revision, be mainnet, start at Ironwood activation and hold at
least `min_rows`, and the rows, filters and witnesses must be the ones it
commits to, which the server library re-validates record by record while
preparing PIR. Its anchor must then be canonical on the freshest node. Updates
are serial: a newer record supersedes one still preparing, a record that does
not verify is refused while the last valid publication keeps serving, and the
publication rules above (epochs, monotonic coverage, the predecessor's
60-second grace) are unchanged. The canonical guard runs independently
throughout and revokes on a confirmed reorg; an outage keeps serving. A revoked
but still-recorded publication is verified and confirmed again. A restart
re-reads the record and verifies everything before serving; the
`serve-publication.json` status file in `state_dir` is a report, never an input.
Health's `publication` field reports the assigned, loaded and preparing records
and the latest refusal, typed as `invalid_source`, `unavailable`,
`noncanonical` or `superseded`. `--check` runs the same verification of the
recorded publication and exits without serving. The publisher remains trusted
for completeness. Operators install it as the `receiver.serve-publication`
external worker ([operations](../operations.md#external-workers)).

`receiver-directory probe --origin <url> --health-url <private health URL>
--rpc-url <node> --no-auth [--witnesses]`, or `receiver-probe` with the same
arguments on the monitor host, is a `pir-monitor` service probe with an embedded
mainnet fixture. It checks the served anchor against independent nodes, looks up a
pinned historical payment over live encrypted PIR, checks the filter file and, with
`--witnesses` (set as for the indexer), the witness file against the node's
Ironwood root, then the feed's freshness, the payout report and the feeds' history,
and prints the indexer's capacity. `answer_mismatch` marks wrong served data, a
correctness incident; `oracle_invalid` a fixture that fails its pin; anything else
is an availability failure. `--max-lag` (default 12)
must cover the indexer's `--depth` plus about ten blocks. With
`--await-feed-reads <seconds>`, as a deploy runs it, the probe first waits for
`near.reads` to show both feeds read and for the served publication's
`indexer.feeds` to hold those reads and its `indexer.history` to be current, failing
as `feeds_not_read` otherwise.
`services/receiver-indexer/src/probe.rs` documents every check.

## Capacity

Past any of these limits the indexer creates no publication, and the previous one
keeps serving and goes stale. Health's `indexer.capacity` gives each publication's
use of each (`limits`, each `used` and `max`) and a `level`: `warning` from 70% of
any limit, `critical` from 90%.

| Limit | Maximum | Set by |
|---|---|---|
| `records` | 240,000 records, which 65,536 rows place reliably (below) | `MAX_ROWS` in wallet-libraries |
| `filter_bytes` | 8 MiB | `MAX_FILTERS_BYTES` in wallet-libraries |
| `witness_bytes` | 64 MiB | `MAX_WITNESS_BYTES` in wallet-libraries |
| `witness_commitments` | `--max-witness-commitments` (2^24) | the indexer's unit |
| `full_pass_secs` | 2 days for the longer feed's last full pass, or the running one's so far | `--near-sweep-pages` and the explorer's request limit |

Placement hashes each record to a row, and a salt fails when any row gets more than
its 14 slots, so the largest table fills well before its sizing ceiling of 458,751
records. Rows load about as a Poisson variable: at 240,000 records about half a row
overflows per salt and all 16 salts fail about once in six million publications,
while at 300,000 nearly every publication fails. In a simulation of random receivers
the best of 16 salts placed 291,280 records.

Health's `capacity_refusal` names the limit that stopped this process's latest
publication, with its use (null when the build stopped before measuring it) and
maximum, until a publication succeeds: one of the above, or `buckets` when every
salt overflowed a bucket at the maximum row count. A witness file over its bound is
`witness_bytes` even below the commitment limit, since every payment's siblings
count. The probe passes `capacity` on whatever else it finds, with a refusal making
it `critical`, and `pir-monitor` raises `capacity_warning` and `capacity_critical`
apart from availability, so a capacity alert neither fails the probe nor hides a
failure.

At `warning`, start the change that raises the limit, so it ships before
`critical`. For `full_pass_secs`, a correction older than a week waits up to about
twice the duration: raise `--near-sweep-pages` in `receiver-pir.service.in` through
a reviewed deploy. Each page adds 11 seconds to every poll, so at most 60 keeps the
recent set fresh. At the default each feed reads about 260 pages an hour, and the
explorer's one request per 5.5 seconds caps both feeds together at about 15,700 a
day, so beyond that ask NEAR for a higher rate. The records, filter and witness
byte limits are wallet-libraries constants, so raising one is a coordinated format
change of wallets and servers. For `witness_commitments`, measure the witness
build's peak memory, then raise `--max-witness-commitments` with the unit's
`MemoryHigh` and `MemoryMax` in `receiver-pir.service.in` through a reviewed deploy. At `critical` or a refusal, ship that change now: wallets accept a
publication at most 100 blocks behind their chain, about two hours. Never drop
records, receivers or history to fit a limit.

## Wallet use

`receiver_pir::transport::DirectoryClient` runs over a host `Transport` that
applies the wallet's route policy, cancellation and timeouts. A transport
enforces response limits while streaming, rejects redirects and maps 410 to
`Error::Revision`. A wallet calls `fetch_manifest`, accepts the manifest's end
block against its own chain and calls `fetch_filters`, which checks the filter
file against the manifest. It tests its receivers with `Filter::matches` and
calls `connect_manifest` only if any need a lookup. `witnesses` fetches the
common witness file, and `page` reads one history page per encrypted query. Reuse the
client across lookups and reconnect for a new revision. The server sees the
number of queries, so it learns how many lookups found several payments, but
never which receivers were looked up. The service has no row file route. It keeps
the rows only to answer queries.

## Tests

`cargo test -p receiver-store` covers publications and store restart and
rollback, including the commitment history a witness file is built from, and the
provider store's resumable sweeps, history and append-only receivers.
`cargo test -p receiver-pir-server` tests the HTTP service with the canonical
library client. `cargo test -p receiver-indexer` covers indexing with synthetic
blocks, refusal of invalid records and witness files before storing or serving,
witness files fetched over HTTP and checked with the library, serving from
memory and the probe's encrypted lookup.

The [receiver packages in wallet-libraries](https://github.com/zakura-core/wallet-libraries/tree/0c110882ad6d36c9d3ad811903abdf37b13d35d5/zakura)
own record and address validation, public mainnet refund recovery, independent
tree witnesses, encrypted round trips at every geometry, and fixed protocol
seeds, framing and row placement. Run those tests in that repository with its
`pir-receiver` and `receiver-pir-server` development configurations. Recovery follows
`zcash/zips@afa086bd976e316612a5c06fb139429958d07d84`, NU6.3 proposal, section
4.19.3 (`decryptovk`).

## Evidence

Pre-deployment mainnet runs are indexed in [the evidence README](../../evidence/receiver/README.md).
