# Wallet integration

Use the schema-11 wallet implementation in [wallet-libraries PR #28](https://github.com/zakura-core/wallet-libraries/pull/28).
The server publishes `ironwood-enhance-pir-v5` with 653-byte records and 33
records per row. The HTTP routes, `EPQ4` binding header, and setup domain are
unchanged from the architecture-2 transport; the record encoding is incompatible.
See [protocol](protocol.md) for offsets and validation.

## Wallet flow

1. During compact scanning, persist the ephemeral key and 52-byte ciphertext
   prefix alongside the action identity and tree position.
2. Fetch the manifest and compare its anchor and coverage with locally scanned
   state. Enforce application-selected resource limits before setup.
3. Use `query_row_requests` for queued actions in the same transaction and row.
   It preserves request order and duplicate identities while sending one query
   per row. Split work across rows locally; never transmit transaction IDs.
4. Pass returned `(request, record)` pairs to `apply_ironwood_enhance_records`.
   The wallet reconstructs the full ciphertext, checks live identities,
   authenticates notes and applies the batch atomically. A live rejection or
   SQL error rolls back the batch; stale identities remain no-ops.
5. On generation expiry, fetch and independently accept a new manifest before
   retrying. Missing compact context requires normal compact rediscovery, not
   inventing a key or prefix from server data.

Fee, expiry and transparent flags remain indexer assertions. Successful note
decryption does not authenticate those fields. Send-only association can require
server trust when decryption cannot authenticate the action. Network timing and
row-query counts remain observable; batching does not add cover traffic.

## Local CLI

The internal `v4` module and CLI option name are retained for operational
continuity. They now speak schema 11:

```sh
cargo run -p enhance-pir --features cli --bin enhance-pir-cli -- \
  --v4 --server http://127.0.0.1:8280 metadata
cargo run -p enhance-pir --features cli --bin enhance-pir-cli -- \
  --v4 --server http://127.0.0.1:8280 query 0
```

The CLI returns an encoded suffix record; it does not perform wallet
acceptance, reconstruct compact context, or authenticate a note.

## Reproducible interoperability

[The isolated harness](../tools/schema11-interop/run.py) verifies the wallet
checkout is at PR #28's pinned commit and does not modify it:

```sh
python3 enhance/tools/schema11-interop/run.py /absolute/path/to/wallet-libraries
```

It runs real loopback coordinator and worker servers against the wallet client,
checks row boundaries and generation expiry, and scans a SQLite wallet before
querying and atomically applying authentic suffix records. It also verifies
that corrupting a live record rolls back the batch. Synthetic fixtures and
loopback results do not establish production capacity or a deployed cutover.
