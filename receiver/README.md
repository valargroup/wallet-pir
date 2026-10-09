# Receiver directory

The receiver directory lets a restored wallet find Ironwood payments to keys it
cannot rescan for, such as its dynamic IVK refund and incoming keys. It maps each
canonical 43-byte Ironwood receiver to the payments recoverable with the all-zero
outgoing viewing key (OVK). Coinbase recipients are excluded. Every Ironwood
Action still counts toward note positions, including coinbase and unrecoverable
outputs.

`receiver-directory` holds zero-OVK extraction, records, publications, common
witnesses and, with the `store` feature, the indexer's SQLite store.
`receiver-pir` holds the PIR client and the wallet `Transport` interface, plus
the evaluator with the `server` feature. `receiver-pir-server` serves
publications over HTTP. `receiver-indexer` builds them from a Zakura node; its
binary is `receiver-directory`. The service shares process identity and
admission (`pir-control`), metrics (`pir-observability`) and the PIR profile
(`pir-native`) with the other services, and carries its own small node client
until a shared one exists.

## Records and publications

A 285-byte record holds version (1 byte), receiver (43), page (4), total pages
(4), present flag (1), height (4), block hash (32), txid (32), transaction index
(4), Action index (4), note position (8), Action input nullifier (32), note
commitment (32), ephemeral key (32) and ciphertext prefix (52). Integers are
little endian and hashes use protocol byte order. The Action input nullifier is
decryption context, not the received note's spend nullifier. Encoding and
decoding refuse a note commitment or Action input nullifier that is not a
canonical field element, or an ephemeral key that is not a valid Ironwood
ephemeral public key. A receiver's payments are ordered by note position, one per
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

`Snapshot::validate` checks a supplied publication whole, before a server prepares
it: the row and filter digests, the declared filter sets and a paid set of exactly
the records' receivers, every slot and row padding, each record's coverage and
bucket, the exact record count, every receiver's pages, each continuing the last
in chain order as `snapshot::check_next` requires, and, across receivers,
distinct note positions in chain order, one block hash per height, one txid per
transaction index at a height and one location per txid. No chain trust is implied. A
record stored outside its bucket would otherwise make lookups of that receiver
find nothing.

## Filters

Each publication carries an `IWFLT1` filter file of labeled BIP 158 Golomb-coded
sets of receivers, keyed by the salt, so a wallet can test its receivers before
looking any up. The manifest declares every set's label and size:

- `paid`: every receiver with a payment in the publication.
- `<provider>/recent`: every Orchard receiver a swap provider was given within the
  window the manifest declares, as a payout or refund address, in any status.
- `<provider>/seen`: every Orchard payout address the provider was given since its
  feed started, which the manifest also declares.

For each provider set the manifest also declares when the feed's last complete read
began, so a wallet can tell how current the set is. A recent window reaches back
from that read.

The indexer publishes `near-intents/recent` (24 hours) and `near-intents/seen`
from the NEAR Intents explorer, and adds a provider's sets only once its feed
has completed a read. Wallets use the sets they recognize and ignore the rest,
so a new provider needs no format change.

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
at 64 MiB. A server publishes a file only once it has checked a path to that
root for every record in the rows. Building the file reads every commitment
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
| `GET /v1/receiver/rows/:session` | Complete row file |
| `GET /v1/receiver/witness/:session` | Common witness file |
| `GET /v1/receiver/filters/:session` | Filter file |
| `GET /v1/receiver/health` | Process identity, the served session, the indexer's report and this process's NEAR reads (operators only) |

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
[the serving contract](../docs/serving-contract.md) defines for every PIR server,
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
`services/receiver-indexer/src/main.rs` describe every flag. The indexer requires
mainnet and indexes from Ironwood activation to `--depth` (default 2) blocks below
the tip of the freshest mainnet `--rpc-url` node, which runs the whole pass; the
next poll checks each node's genesis and ranks them again. `--no-auth` replaces
`--cookie`, which is read on every request. Each batch of raw blocks is checked
against the saved parent, heights, merkle roots, Action positions, terminal hash and
tree size before it is stored. A restart or reorg rewinds to the last saved
canonical block; a reorg below the start height needs a rebuild in a new directory.

Without `--serve`, one run writes its publication's files under `publications/` and
prints a summary. With `--serve`, it polls every `--poll-seconds`, prepares each
canonical tip in memory and serves on `--bind`, a loopback or private address
behind a TLS proxy. A paused tip is republished when its provider sets or report
change. A guard revokes every session once a node shows a served anchor is off its
chain. A replaced revision stays available for 60 seconds, and the next one waits
for that. Logs go to standard error, filtered by `RUST_LOG`. See [the DigitalOcean deployment](ops/digitalocean/README.md).

With a partner key in `NEAR_INTENTS_EXPLORER`, the server reads the NEAR Intents
explorer into `provider.sqlite` and publishes the `near-intents` recent and seen
sets; without one, publications carry no provider sets. Health's `indexer` report,
activated with each publication, gives each feed's last read and `payouts_missing`,
completed NEAR payouts with no indexed payment: the signal that the index missed
one or NEAR stopped paying with the zero OVK. `payouts_uncheckable` counts those
first seen in the last day that NEAR reported without a usable recipient or
transaction.
`services/receiver-indexer/src/near.rs` documents the feed and the report.

Health's `near.reads` gives, for each NEAR feed, when the last read this process
completed began, or null until one completes, so unlike the report's `feeds` it
shows that the running process's key works.

`receiver-directory probe --origin <url> --health-url <private health URL>
--rpc-url <node> --no-auth [--witnesses]`, or `receiver-probe` with the same
arguments on the monitor host, is a `pir-monitor` service probe with an embedded
mainnet fixture. It checks the served anchor against independent nodes, looks up a
pinned historical payment over live encrypted PIR, checks the filter file and, with
`--witnesses` (set as for the indexer), the witness file against the node's
Ironwood root, then the feed's freshness and the payout report. `answer_mismatch`
marks wrong served data, a correctness incident; `oracle_invalid` a fixture that
fails its pin; anything else is an availability failure. `--max-lag` (default 12)
must cover the indexer's `--depth` plus about ten blocks. With
`--await-feed-reads <seconds>`, as a deploy runs it, the probe first waits for
`near.reads` to show both feeds read, failing as `feeds_not_read` otherwise.
`services/receiver-indexer/src/probe.rs` documents every check.

## Wallet use

`receiver_pir::transport::DirectoryClient` runs over a host `Transport` that
applies the wallet's route policy, cancellation and timeouts. A transport
enforces response limits while streaming, rejects redirects and maps 410 to
`Error::Revision`. A wallet calls `fetch_manifest`, accepts the manifest's end
block against its own chain and calls `fetch_filters`, which checks the filter
file against the manifest. It tests its receivers with `Filter::matches` and
calls `connect_manifest` with the remaining lookup count only if any need a
lookup. Small jobs use PIR. Larger ones (about 400 lookups at 8192 rows)
download the row file once and check its digest. `use_file_for_work` switches
when new work arrives. `witnesses` fetches the common witness file, and `lookup`
returns a receiver's complete history or an error. Over PIR it reads up to
`MAX_PIR_PAGES` (16) pages, and loads the row file for a longer history. Reuse
the client across batches and reconnect for a new revision. The server sees the
mode and the number of queries, so it learns how many lookups found several
payments, but never which receivers were looked up.

## Tests

`cargo test -p receiver-directory --features store` covers recovery of a public
mainnet refund, records, publications, store restart and rollback, and witnesses
against an independent tree. `cargo test -p receiver-pir-server` runs encrypted
round trips at every geometry and the HTTP service. `cargo test -p receiver-pir
--features server --test golden` pins the protocol's seeds, framing and row
placement against digests from a request built outside `Client`. `cargo test -p
receiver-indexer` covers indexing with synthetic blocks, serving from memory and
the probe's encrypted lookup. Recovery follows
`zcash/zips@afa086bd976e316612a5c06fb139429958d07d84`, NU6.3 proposal, section
4.19.3 (`decryptovk`).

## Evidence

Pre-deployment mainnet runs are indexed in [the evidence README](evidence/README.md).
