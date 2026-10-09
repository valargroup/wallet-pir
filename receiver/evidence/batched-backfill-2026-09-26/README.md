# Batched backfill measurement

September 26, 2026, on the Studio against the same public mainnet RPC. The
baseline source was `46f99bff` and the optimized source `aa0fb4fc`, the batching
change first committed with this record. Both predate the rewrite of the receiver
history into its current commits: they are reachable only from an integration
branch, not from `main`, so they are historical references rather than a
reproduction recipe (see [`manifest.json`](manifest.json)). Binary hashes,
timestamps, row digests, and elapsed times are retained in
[the measurement record](results.json).

This record is historical: publications then started at 4 rows, so the revision
below is not what the same range publishes today at 8192 rows or more. The
batching it measured is unchanged.

Both binaries indexed heights 3,496,100–3,496,200 into fresh, separate SQLite
directories with the ordinary backfill paused. They used the same local Cargo
test build configuration and RPC endpoint. The optimized run used a batch size of
64 and concurrency of 8. Both ranges had already been requested before this pair
to reduce cold-cache differences. This is a small network measurement, not a
guaranteed full-history rate or an isolated node benchmark.

The Rust compiler and resolved dependencies that built the two binaries were not
recorded, nor was the exact build command or its output. Both source commits pin
Rust 1.91.0, share one `Cargo.lock` and declare `opt-level = 1` for the `test`
profile, but that is source configuration, not proof of what built the binaries;
[`manifest.json`](manifest.json) records it under `toolchain`.

| Run | Elapsed |
|---|---:|
| Sequential baseline | 28.855 seconds |
| Concurrent batches | 2.091 seconds |

The optimized run was 13.8 times faster. Both publications were byte-identical:
20 payments, 630 Actions, 14 excluded coinbase Actions, and revision
`346932851fd94db77e31ac522460586f262f7782601e2eb03209a18e0b2ff925`.
The range includes the previously verified refund at position 610503.

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
