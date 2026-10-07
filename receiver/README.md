# Receiver directory

The receiver directory lets a restored wallet find Ironwood payments to keys it
cannot rescan for, such as swap refund and incoming keys. It maps each canonical
43-byte Ironwood receiver to the payments recoverable with the all-zero outgoing
viewing key (OVK). Coinbase recipients are excluded. Every Ironwood Action still
counts toward note positions, including coinbase and unrecoverable outputs.

`receiver-directory` holds zero-OVK extraction, records, publications, common
witnesses and, with the `store` feature, the indexer's SQLite store.
`receiver-pir` holds the PIR client and the wallet `Transport` interface, plus
the evaluator with the `server` feature. `receiver-pir-server` serves
publications over HTTP. The indexer binary, `receiver-directory`, lives in
`enhance-pir-server` behind its `receiver` feature, to reuse its block parser and
RPC client. Enhance builds without the feature compile no receiver code, and the
indexer enables no Enhance route and never modifies an Enhance journal.

## Records and publications

A 285-byte record holds version (1 byte), receiver (43), page (4), total pages
(4), present flag (1), height (4), block hash (32), txid (32), transaction index
(4), Action index (4), note position (8), Action input nullifier (32), note
commitment (32), ephemeral key (32) and ciphertext prefix (52). Integers are
little endian and hashes use protocol byte order. The Action input nullifier is
decryption context, not the received note's spend nullifier. A receiver's
payments are ordered by note position, one per page from zero, and every page
repeats the total.

Rows are 4096 bytes and hold 14 records plus zero padding. A domain-separated
hash of the salt, the receiver's tag and the page selects a row. Publications
start at 8192 rows. A crowded bucket retries up to 16 salts derived from the
terminal hash, the first being the hash itself, and only then doubles the table,
up to 65536 rows. Overflow at the maximum fails the candidate instead of dropping
records. The manifest (profile `ironwood-zero-ovk-receiver-v2`) binds the network,
inclusive block coverage, boundary hashes, tree positions, geometry, salt, record
count and the SHA-256 of the rows and of the filter file. Its own SHA-256 is the
immutable revision.

## Filters

Each publication carries an `IWFLT1` filter file with three Golomb-coded sets of
receivers, keyed by the salt, so a wallet can test its receivers before looking
any up:

- `paid`: every receiver with a payment in the publication.
- `recent`: every Orchard receiver a swap provider was given in the last 24 hours,
  as a payout or refund address, in any status.
- `seen`: every Orchard payout address a swap provider was given since the feed
  started.

Values come from a domain-separated SHA-256 of the salt and receiver and use Rice
parameter 10, so a receiver outside a set matches it about once in 1,024 tests.
Each set is its count and byte length (little endian) followed by its bits, about
1.4 bytes per receiver. Every wallet downloads the same file, so testing it
reveals nothing. The publisher is trusted for the sets' completeness, as for the
rows.

## Protocol

The profile `ironwood-receiver-pir-v1-q48` uses ipir-sp P16Q48 over the rows. A
session manifest holds the directory manifest, the protocol and the SHA-256 of
the public PIR setup. Its domain-separated digest is the session ID.

| Route | Content |
|---|---|
| `GET /v1/receiver/init` | Current session manifest |
| `GET /v1/receiver/public/:session` | Public PIR setup |
| `POST /v1/receiver/query` | One encrypted row query |
| `GET /v1/receiver/rows/:session` | Complete row file |
| `GET /v1/receiver/witness/:session` | Common witness file |
| `GET /v1/receiver/filters/:session` | Filter file |

A query holds `RPQ1`, the session ID, a fresh 16-byte nonce, packing keys and the
encrypted row selection. The response echoes that 52-byte header. The receiver
and page never appear in a route or header. An unknown session returns 409, a
revoked one 410, an oversized query 413, a malformed one 400 and a full queue 503.

The `IWPROOF1` witness file has a 152-byte header binding the genesis, revision,
terminal height and hash, tree size and root. Sorted 37-byte nodes (level, index,
hash) follow, with every sibling of each payment position. It is capped at 64 MiB.

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
cargo build --locked --profile release-fast -p enhance-pir-server --features receiver --bin receiver-directory
receiver-directory --data-dir /srv/receiver-pir/index --rpc-url http://127.0.0.1:8232 \
  --cookie /path/to/.cookie --serve --witnesses --min-rows 8192
```

`--no-auth` replaces `--cookie` for an explicitly selected node without RPC
authentication. The indexer requires mainnet and covers Ironwood activation
through the node's tip, or a test range from `--start-height` to `--end-height`.
Raw blocks arrive concurrently in batches of up to 64. Each batch is checked
against the saved parent, heights, Action positions, terminal hash and tree size
before it is stored. A restart rewinds to the last saved canonical block. A reorg
below the start height needs a rebuild in a new directory.

Each run writes `<revision>.rows`, `<revision>.filters`, `<revision>.json` and,
with `--witnesses`, `<revision>.witness` under `publications/`. Witnesses need commitments from
position zero, so the index must start at Ironwood activation. With `--serve`,
the process polls every `--poll-seconds` (default 10), publishes each new
canonical tip and serves on a loopback `--bind` (default `127.0.0.1:18380`)
behind a TLS proxy. A separate guard rechecks served anchors and revokes every
session on a mismatch or failed check. A recovery epoch fences work that began
before a revocation. The previous revision stays available for 60 seconds. See
[the DigitalOcean deployment](ops/digitalocean/README.md).

The swap provider sets come from the NEAR Intents explorer. While serving with a
partner key in `NEAR_INTENTS_EXPLORER`, the indexer reads every swap into or out of
ZEC every `--near-poll-seconds` (default 60), whichever app created it, into
`provider.sqlite`. Its first read starts a day back, or at `--near-since`. The key
is never logged. Without a key, the recent and seen sets stay empty.

## Wallet use

`receiver_pir::transport::DirectoryClient` runs over a host `Transport` that
applies the wallet's route policy, cancellation and timeouts. A transport
enforces response limits while streaming, rejects redirects and maps 409 and 410
to `Error::Revision`. A wallet calls `fetch_manifest`, accepts the manifest's end
block against its own chain and calls `fetch_filters`, which checks the filter
file against the manifest. It tests its receivers with `Filter::matches` and calls
`connect_manifest` with the remaining lookup count only if any need a lookup. Small jobs use PIR. Larger ones (about 240 lookups at
8192 rows) download the row file once and check its digest. `use_file_for_work`
switches when new work arrives. `witnesses` fetches the common witness file, and
`lookup` returns a receiver's complete history or an error. Over PIR it reads up
to `MAX_PIR_PAGES` (16) pages, and loads the row file for a longer history. Reuse the client across batches and reconnect for a new revision. The
server sees the mode and the number of queries, so it learns how many lookups
found several payments, but never which receivers were looked up.

## Tests

`cargo test -p receiver-directory --features store` covers recovery of a public
mainnet refund, records, publications, store restart and rollback, and witnesses
against an independent tree. `cargo test -p receiver-pir-server` runs encrypted
round trips at every geometry and the HTTP service.
`cargo test -p enhance-pir-server --test receiver` covers indexing with synthetic
blocks. Recovery follows `zcash/zips@afa086bd976e316612a5c06fb139429958d07d84`,
NU6.3 proposal, section 4.19.3 (`decryptovk`).
