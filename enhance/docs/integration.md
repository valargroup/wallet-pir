# Wallet integration

Use the `enhance-v7` implementation of `zakura/pir-enhance` in
[wallet-libraries](https://github.com/zakura-core/wallet-libraries). The server
publishes `ironwood-enhance-pir-v7`, schema 11, with 653-byte records and 33 records
per row. v6 clients are incompatible. The q48 IPIR dependency remains pinned to
`611a29284264d844bf4dba00de2874c5b762f8c2`. See [protocol](protocol.md) for the
116-byte `EPQ7` binding and canonical session identity.

## Wallet flow

1. During compact scanning, persist the ephemeral key and 52-byte ciphertext
   prefix alongside the action identity and tree position.
2. Fetch `PendingClient`, validate the manifest, and compare its anchor and
   coverage with independently scanned state. Create `GenerationAcceptance` with
   that trusted anchor and application resource limits, then accept the client.
3. Fetch routing at sync start, when `refresh_due()` is true (30 seconds), and
   after 409/410. Independently accept the new anchor and call `accept_routing`.
   Unchanged sessions are rebound and their material reused. The HTTPS wrapper
   exposes `fetch_routing`, `accept_routing`, and `refresh_due`. It never silently
   trusts an anchor fetched from the server.
4. Use `query_row_requests` for queued actions in the same transaction and row.
   It preserves request order and duplicate identities while sending one query
   per row. Follow manifest-owned routes, including predecessor suffixes mapped
   into successor domains. Never transmit transaction IDs.
5. Pass returned `(request, record)` pairs to `apply_ironwood_enhance_records`.
   The wallet reconstructs ciphertexts, checks live identities, authenticates
   notes and applies the batch atomically. A live rejection or SQL error rolls
   back the batch; stale identities remain no-ops.

A 409 means routing is stale; a 410 means the session is unavailable or revoked.
Both require refresh and fresh wallet acceptance before retry. New requests use
fresh randomness and request IDs even when cached session material is reused.
Missing compact context requires normal compact rediscovery. Never invent a key
or prefix from server data.

Fee, expiry and transparent flags remain indexer assertions. Successful note
decryption does not authenticate those fields. Send-only association can require
server trust when decryption cannot authenticate the action.

## Optional cover traffic

Cover is off by default. `query_positions_with_cover` takes target positions and
the wallet's birthday first position. Within one accepted routing view it queries
every domain since that birthday, sends uniform round counts, randomizes domain
order and creates fresh dummy queries. A 429/503 retries the whole round once;
partial rounds are not returned. Application code must refresh and reaccept
routing before beginning another interval when required.

This policy conceals the target domain within the chosen cover set for that
interval. It does not conceal the birthday window, timing, total round count,
network identity, or intersection across intervals. Same-row batching alone does
not provide cover. Application developers must opt into the extra bandwidth and
compute cost explicitly.

## CLI and reproducible interoperability

```sh
cargo run -p enhance-pir --features cli --bin enhance-pir-cli -- \
  --server http://127.0.0.1:8280 metadata
cargo run -p enhance-pir --features cli --bin enhance-pir-cli -- \
  --server http://127.0.0.1:8280 query 0
python3 enhance/tools/v7-interop/check_vectors.py
python3 enhance/tools/schema11-interop/run.py /absolute/path/to/wallet-libraries \
  --wallet-revision <full-commit> --full
```

The CLI returns a suffix record; it does not perform wallet anchor acceptance or
note authentication. The independent vector checker verifies session encoding.
The external-wallet harness fingerprints the wallet source, runs real HTTP
coordinator/worker queries, tests composition, confirmation, recovery and session
reuse, and scans a SQLite wallet before atomically applying authentic records.
It also verifies rollback of a corrupt live record. Use `--full` for full-size
lifecycle cases. Loopback tests do not establish production capacity.
