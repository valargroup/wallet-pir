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
`enhance-pir-server` to reuse its block parser and RPC client. It enables no
Enhance route and never modifies an Enhance journal.

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
start at 8192 rows and double on bucket overflow up to 65536. Overflow at the
maximum fails the candidate instead of dropping records. The manifest binds the
network, profile, inclusive block coverage, boundary hashes, tree positions,
geometry, salt, record count and the SHA-256 of the rows. Its own SHA-256 is the
immutable revision.

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
or inconsistent pages, an exhausted page budget and transport failures are
errors, never absence or a cleartext fallback.

## Running the indexer and server

```sh
cargo build --locked --profile release-fast -p enhance-pir-server --bin receiver-directory
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

Each run writes `<revision>.rows`, `<revision>.json` and, with `--witnesses`,
`<revision>.witness` under `publications/`. Witnesses need commitments from
position zero, so the index must start at Ironwood activation. With `--serve`,
the process polls every `--poll-seconds` (default 10), publishes each new
canonical tip and serves on a loopback `--bind` (default `127.0.0.1:18380`)
behind a TLS proxy. A separate guard rechecks served anchors and revokes every
session on a mismatch or failed check. A recovery epoch fences work that began
before a revocation. The previous revision stays available for 60 seconds. See
[the DigitalOcean deployment](ops/digitalocean/README.md).

## Wallet use

`receiver_pir::transport::DirectoryClient` runs over a host `Transport` that
applies the wallet's route policy, cancellation and timeouts. A transport
enforces response limits while streaming, rejects redirects and maps 409 and 410
to `Error::Revision`. A wallet calls `fetch_manifest`, accepts the manifest's end
block against its own chain, then calls `connect_manifest` with the job's
remaining lookup count. Small jobs use PIR. Larger ones (about 240 lookups at
8192 rows) download the row file once and check its digest. `use_file_for_work`
switches when new work arrives. `witnesses` fetches the common witness file, and
`lookup` returns a receiver's complete history or an error. Reuse the client
across batches and reconnect for a new revision. The server sees the mode, never
the receivers.

## Tests

`cargo test -p receiver-directory --features store` covers recovery of a public
mainnet refund, records, publications, store restart and rollback, and witnesses
against an independent tree. `cargo test -p receiver-pir-server` runs encrypted
round trips at every geometry and the HTTP service.
`cargo test -p enhance-pir-server --test receiver` covers indexing with synthetic
blocks. Recovery follows `zcash/zips@afa086bd976e316612a5c06fb139429958d07d84`,
NU6.3 proposal, section 4.19.3 (`decryptovk`).
