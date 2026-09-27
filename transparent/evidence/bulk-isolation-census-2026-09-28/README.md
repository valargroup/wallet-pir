# Bulk-history isolation census, 2026-09-28

Architecture update §4: what ordinary wallets would gain if long histories
(more than 38 events in a shard) lived in a separate bulk table that does not
decide shard boundaries. Offline; no bulk table is implemented.

| Field | Value |
|---|---|
| Tool | `shard-census --bulk-long` (new `PageBasis::PackedOrdinary`: seal on short histories' packed rows only), same commit as this note |
| Input | The recent journal slice from the [single-lookup census](../single-lookup-census-2026-09-27/README.md), heights 3,262,749–3,473,686, shard ids from 160 |
| Commands | `shard-census --data-dir <slice> --geometry {recent-8k,recent-4k-8k} --first-shard-id 160 --end-height 3473686 --bulk-long --placement --per-shard`; `pairs.py <census> sample.json <slice>`; `filter-sweep --census <census> --params 19:784931,13:12288` |
| Output | `census-*.txt`, `results.txt` (pairs and filter totals for today's boundaries and both bulk variants) |

Only each census's first policy (the geometry's seal policy) is used. The
later blocks in the raw census output are the default sweep.

## Results

| Boundaries | Shards | restore-6m pairs (sealed + tail) | restore-6m matched shards | Filter bytes, P=19 / P=13 | Bulk rows |
|---|---:|---:|---:|---:|---:|
| Today, recent-8k (seal on packed page rows) | 14 | 5.93 | 4.93 | 1,293,378 / 924,952 | none |
| Bulk isolation, recent-8k | 5 (4 sealed) | 5.45 | 3.53 | 1,136,251 / 812,577 | 86,750 total, up to 24,018 per shard |
| Bulk isolation, recent-4k-8k | 10 (9 sealed) | 5.81 | 4.57 | 1,236,033 / 883,934 | 81,187 total, up to 10,764 per shard |

- **Sealing reason.** With bulk isolation every sealed shard closes on the
  script target (98,304 at recent-8k), not on page rows. Ordinary page rows per
  shard fall to 3,418–4,938 of 8,192.
- **Short absences.** 1-day and 7-day catch-ups touch only the tail at
  recent-8k: 1.0 matched shard each, against 1.81 and 1.22 today.
- **Multi-script.** Matched shards fall from 13.3 to 5.0, and pairs from 53.1
  to 46.1.

**Derived restore-6m effect** with choice tables, from these counts and the
measured per-request sizes:
- about −12% filter bytes;
- −0.5 directory queries;
- −1.4 manifests and setups.

In total about 2.13 against 2.39 MB per sync (−11%), plus fewer sequential
rounds.

## Costs and open issues

- **A new table type.** Wallets with long histories query the bulk table.
  Choosing the bulk table reveals a long history. Today the number of page
  queries already reveals roughly this, but a privacy review must confirm it
  stays within the accepted query-count leakage, or the choice must be hidden.
- **Bulk sizing.** A bulk shard needs up to 24,018 rows at recent-8k, which is
  three 8K segments. Every segment answers every bulk query, so a long-history
  wallet pays about three times per bulk lookup, unless the bulk geometry is
  sized separately.
- **Longer-lived tails.** Recent shards seal about every 52,000 blocks (about 6
  weeks) instead of about every 15,000. The tail is rebuilt and republished
  every block and grows to about 98,000 scripts, so publication work and the
  tail's table grow with it.
- **recent-4k-8k.** Most of the gain disappears there (10 shards).

This is a moderate gain for a new table type, a privacy review and changed
publication behaviour. It ranks behind the filter profile and request
concurrency.
