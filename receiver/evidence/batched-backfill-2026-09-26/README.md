# Batched backfill measurement

September 26, 2026, on the Studio against the public mainnet RPC below. The
baseline source was `46f99bff` and the optimized source `aa0fb4fc`, the batching
change first committed with this record; the run metadata is in
[`manifest.json`](manifest.json). Binary hashes, timestamps, row digests, and
elapsed times are retained in [the measurement record](results.json).

This record is historical: publications then started at 4 rows, so the revision
below is not what the same range publishes today at 8192 rows or more. The
batching it measured is unchanged; it is now `receiver_batch` in
[`blocks.rs`](../../services/receiver-indexer/src/blocks.rs).

Both binaries indexed heights 3,496,100–3,496,200 into fresh, separate SQLite
directories with the ordinary backfill paused. They used the same local Cargo
test build configuration and RPC endpoint. The optimized run used a batch size of
64 and concurrency of 8. Both ranges had already been requested before this pair
to reduce cold-cache differences. This is a small network measurement, not a
guaranteed full-history rate or an isolated node benchmark.

| Run | Elapsed |
|---|---:|
| Sequential baseline | 28.855 seconds |
| Concurrent batches | 2.091 seconds |

The optimized run was 13.8 times faster. Both publications were byte-identical:
20 payments, 630 Actions, 14 excluded coinbase Actions, and revision
`346932851fd94db77e31ac522460586f262f7782601e2eb03209a18e0b2ff925`.
The range includes the known refund described below.

Each run used the equivalent of:

```sh
receiver-directory --data-dir <fresh-directory> \
  --rpc-url http://159.65.183.89:8232 --no-auth \
  --start-height 3496100 --end-height 3496200
```

All four receiver integration tests passed. The batch regression checks bounded
concurrency, reordered responses, bad parent links, wrong heights and tree sizes,
wrong or changing canonical anchors, malformed raw blocks, and failed RPC calls.
The CLI restart/reorg test still passes. Clippy and formatting completed with no
new warnings; existing warnings remain in unrelated server and dependency code.

## Earlier smoke run

The same day, a smoke run indexed the same range with the binary then in
`enhance-pir-server` (`cargo run -p enhance-pir-server --bin receiver-directory`,
the arguments above and `--data-dir /tmp/receiver-smoke`), at an unrecorded commit.
These observations come from its original note; none of its outputs were retained.

- Terminal hash, in RPC display order:
  `00000000005090cbf22c88e5f9db4b710323573cae7c4ebbc70021be25430120`.
- 20 authenticated non-coinbase receiver records, not classified as NEAR, in 4
  rows of 4,096 bytes (16,384 bytes) with the revision above.
- The known refund
  `2060cf68088b55dcd9e2f91556c72528e1ab8c6834ea3f71b8c80fca9fc51653` is at
  height 3,496,114, transaction index 16, Action index 0, global note position
  610503. Its block hash, transaction identity, Action nullifier, commitment,
  ephemeral key and 52-byte ciphertext prefix matched an independent verbose RPC
  block response; the row digest was recomputed independently, and a rerun gave
  identical counts and revision. The raw transaction is retained as the
  indexer's test fixture
  ([`receiver-refund.hex`](../../services/receiver-indexer/tests/fixtures/receiver-refund.hex)),
  whose tests recover the same Action.
- That revision's suites passed: six shared-crate tests with `--features store`
  and three server `--test receiver` tests, including a command-line restart and
  reorg replacement using synthetic envelopes around public ciphertext, and the
  shared crate passed Clippy with warnings denied. Encrypted queries and wallet
  insertion were not covered.
