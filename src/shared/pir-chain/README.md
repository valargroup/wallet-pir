# PIR chain readers

`zakura-server-chain` reads Zakura node data for the PIR services in wallet-pir and
interprets it, returning plain values (bytes, heights, amounts, positions) that the
services map onto their own records. It owns the chain interpretation boundary used by those services. RPC ingestion
reads transactions with the borrowed wire reader `zakura-transaction-wire`
(wallet-libraries, [PR 287](https://github.com/zakura-core/wallet-libraries/pull/287)),
which checks the wire grammar, amount ranges and transaction boundaries and computes
transaction IDs. It depends only on `zakura-protocol` and two hash crates, so the
production RPC reader and the Transparent, Enhance and Status readers compile no
`zakura-primitives`, Sapling, Orchard or Halo2 crate. The reader does not check that
shielded fields are valid curve points, proofs or signatures: blocks come from a
node that validated them, and that remains the node's responsibility. Block headers
are framed and hashed here (version at least 4, a 36- or 1344-byte Equihash
solution). `receiver` adds Orchard to check Ironwood tree roots; the `testing`
fixtures and the `rocksdb` reader use the node's own crates. The reader does not
compile Zakura node packages or RocksDB.

Decoding and chain interpretation (block and transaction decoding, the product
values below and `NodeError`) are synchronous and resolve no runtime or HTTP stack.
Product cores enable only their product feature (and `testing` in tests); hosts
and tools that read a node add `rpc`.

- `rpc` (feature): the node JSON-RPC client and each product's asynchronous reads,
  with Reqwest and Tokio. `NodeClient::new` rereads a cookie file on every
  request and bounds every response; `NodeClient::from_cookie_file` reads it once.
  Nodes rank by tip on a checked network. Every request has a 120-second total
  deadline, including connection establishment and streaming the response body.
  Receiver has a separate 20-second connection timeout and its chain-derived
  response bound. Fixed-cookie clients (Enhance, Status and Transparent) have no
  separate connection timeout and now limit each whole response, including a
  batch, to 64 MiB. Responses without Content-Length obey the same bound.
  Batch replies must contain each issued integer ID exactly once and are returned
  in request order. The transport does not retry or split batches.
- `hash`: conversions between the node's display order and protocol order.
- `receiver`: Ironwood blocks checked against their headers and the node's tree
  sizes, every Action's note position (coinbase included), the payments
  recoverable with the zero outgoing viewing key, and Ironwood roots.
- `enhance`: each Ironwood Action's Enhance fields with its transaction's exact fee,
  resolving spent outputs from the same block, a bounded cache, then batched reads;
  an unresolved output fails the block.
- `status`: canonical blocks' transaction IDs and the mempool's.
- `transparent`: every transaction's transparent outputs and the outputs its inputs
  spend, as raw scripts with exact fees, resolving same-block spends first, then a
  bounded cache, then batched reads; an unresolved output fails the block. Also
  compact blocks rebuilt from raw blocks, and the raw JSON-RPC calls the services'
  independent spot checks make.
- `rocksdb` (feature): `transparent::state::StateReader`, which opens an archive
  node's finalized state as a RocksDB secondary and reads the same transparent
  blocks from it. Blocks and spent transactions are read as stored bytes and decoded with the
  same lightweight reader as RPC, avoiding a full node decode and reserialization.
  The node storage crates are still dependencies, so this feature still compiles
  their shielded backends. It reads the snapshot visible when it opened and never
  catches up; open a new reader to see later blocks, and reopen it periodically during a
  long backfill, since the node's compaction can retire files it still names. A database that pruned raw
  transactions, or was written by a pruned checkpoint-syncing node, is refused with
  an error. Its errors carry only this crate's types. Its `zakura-state` revision
  must match the running node's format.
- `testing` (feature): enables the pinned node chain crate for independent synthetic
  fixtures. Production decoding still uses Common. Synthetic transactions and block
  envelopes built with the node's own encoders, for service tests, without a
  runtime or HTTP stack.
- `testing-node` (feature): adds `rpc` and a JSON-RPC node double serving those
  fixtures over HTTP, for host and tool tests of the node reads.

Receiver backfill and the combined Enhance block reader currently use mainnet
activation rules. `Network::Testnet` provides explicit identity/height queries; it
does not configure the RPC client or enable Testnet backfill.

The optional RocksDB reader retains its node dependencies and disk-format pin.
Blocks cross from the node reader into the shared extractor through canonical
serialization and Common decoding; database opening and previous-output lookups
still use the node implementation. This adds a conversion step to direct block
reads; no backfill throughput improvement is claimed.

This crate is server-only and owned by wallet-pir, at `src/shared/pir-chain` in
the root workspace. It moved here from wallet-libraries' separate `zakura/pir-chain`
workspace (revision `f48b62b80ec7d9d7fb7893401435a24b189b8ec2`) without behavior
changes. It consumes wallet-libraries' receiver recovery at the revision the
services pin; wallet-libraries never depends on it. It depends on no other
wallet-pir package. Service-facing values contain only reader-owned records,
errors, bytes, heights and network choices; node and Common types remain
implementation details. It is the only wallet-pir package with direct Common and
node dependencies (`tools/ci/boundary.py`), and query-serving packages never reach it.

## Product selection

Default features enable only shared transport, hashes and network helpers. Select
`transparent`, `enhance`, `status` or `receiver` explicitly on each consumer's
`zakura-server-chain` dependency, with `default-features = false`. Only `receiver`
enables receiver recovery. `rocksdb` implies `transparent` and adds the node's
database reader. `testing` adds shared synthetic fixtures without selecting an
adapter; the receiver and status fixture modules follow their product features.
For example, Transparent service tests select `["transparent", "testing"]`.

This separates adapter compilation, not the shared transaction parser or its
cryptographic dependencies. Selecting all products in one workspace still unifies
those features, so this change does not establish a build-time speedup.

## Validation

From the repository root:

```sh
python3 tools/ci/chain_graph.py
python3 tools/ci/full_packages.py --group shared
make check-package PACKAGE=zakura-server-chain TEST_TARGET=transparent
```

`full_packages.py --group shared` runs the three explicit selections: decoding and
fixtures without `rpc`, RPC reads of every product against the HTTP node double,
and the RocksDB reader. Fast checks run the four product RPC targets; full checks
add `state`. The graph check resolves fresh consumers with and without fixture
support, seeded from the workspace lockfile and source patches. Fast feedback
applies those resolve rules; running this tool directly also type-checks each
consumer.

Historical mainnet and testnet block vectors compare the RPC decoder against the
pinned node decoder, including genesis, transaction IDs, output bytes and Merkle
roots. Ironwood fixtures compare the action fields. The fresh production consumer
check rejects active node or RocksDB dependencies while allowing the optional
RocksDB and fixture configurations to resolve them.
