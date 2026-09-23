# V4 local functional and load evidence

This is a development smoke run on Roman's macOS machine with 128 GiB RAM.
It is **not** an 8 GiB memory qualification or a production throughput result.
The workload starts with 67 synthetic records and appends during traffic.
The generator, coordinator and two worker processes share the host; development
checks also ran on it during this work. Each measured stage lasts 30 seconds.

| Concurrency | Correct responses | Incorrect responses | HTTP 429 | Correct responses/s | All-request p99 |
|---|---:|---:|---:|---:|---:|
| 1 | 1,948 | 0 | 0 | 64.91 | 29.55 ms |
| 2 | 3,103 | 0 | 0 | 103.38 | 49.18 ms |
| 4 | 3,547 | 0 | 672 | 118.06 | 39.10 ms |
| 8 | 3,588 | 0 | 2,370 | 119.35 | 42.14 ms |

All successful responses were checked byte-for-byte against the positional
fixture oracle. Higher-concurrency stages intentionally characterize rejection
at the two-query coordinator admission limit; their successful driver exit does
not satisfy a zero-error release gate. Percentiles include rejected requests.

The manifest records the base revision, dirty-working-tree status, executable
SHA-256 hashes, initial/final public manifests, and health after each stage.
Individual command files and JSON reports retain the exact invocations/results.
The source continued to change after this run; these executable hashes identify
the tested build, not an assertion that all later changes were load-tested.

Additional development tests passed for:

- Unequal 8K + 2K units versus a monolithic 16K domain, including preprocessing,
  evaluation and implicit zero padding.
- Real encrypted bootstrap queries, immutable runtime reuse and disk restore.
- Distributed HTTP retrieval, five-generation expiry/client refresh, replica
  failover, ordinary publication with one replica unavailable, and coordinator
  restart.
- A full 32K carve-out and 4K-owned return through HTTP, preserving queries
  prepared against both old lender and borrower sessions (76.53 seconds).

The [implementation status](../../docs/architecture_2-implementation.md) lists
the remaining acceptance and production gates.
