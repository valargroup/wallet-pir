# Live PIR measurements — 20 September 2026, Dubai

**Enhance database:** 450,163 encrypted records, **331.77 MB** raw, 7 populated shards (65,536-row logical domain).

**Transparent database:** 353.38 million events, **57.25 GB** padded tables + **63.43 MB** filters, 175 shards.

| Per encrypted query | Upload | Download | Server p50 / p95 |
|---|---:|---:|---:|
| Enhance | 420.01 KiB | 10.02 KiB | 40.9 / 43.0 ms |
| Transparent recent directory | 125.01 KiB | 5.02 KiB | 11.8 / 34.2 ms |
| Transparent recent page | 125.01 KiB | 5.02 KiB | 11.2 / 27.9 ms |
| Transparent archive directory | 252.01 KiB | 5.02 KiB | 7.9 / 9.3 ms |
| Transparent archive page | 420.01 KiB | 5.02 KiB | 12.6 / 13.6 ms |

**Growth:** Enhance scans all populated groups; upload grows in geometry steps. Doubling the current record count projects upload **420 → 772 KiB**, with a **10 KiB response**. Transparent adds fixed-size shards: per-query cost stays stable if runtimes remain warm; more matching shards/history pages cause more queries. Doubling a table on the same archive worker measured:

**112 → 224 MiB table; 252 → 420 KiB upload; 7.9 → 12.6 ms evaluation; 5 KiB response unchanged.**

*Fresh live measurements, repeated single-client queries, actual payload lengths, server timing-counter deltas, and exact-answer checks. Enhance time includes coordinator post-upload processing and worker RPC; Transparent time is worker evaluation only. Times are successful-query percentiles, not whole-wallet recovery or maximum capacity. Transparent recovery needs two directory queries per matching script/shard plus any pages, filters and setup.*

**Current readiness caveat:** Transparent public initialization returns 404 and its public filter map is stale. The test bootstrapped from verified private metadata and used public query endpoints. Cache-admission and transport failures occurred; retain these caveats when presenting the performance numbers.

Source: [full report](REPORT.md), [machine-readable results](summary.json), and retained raw observations in this directory. Future-size numbers are projections; the current-size and archive 2×-table comparison are measured.
