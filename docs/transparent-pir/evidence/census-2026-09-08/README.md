# Mixed-tier census, 2026-09-08

The Gate 1 census of the pinned dataset under the accepted two-tier geometry,
run on the coordinator against the journal the [inventory](../inventory-2026-09-08/README.md)
identified. Raw output is `census.txt`; `provenance.txt` records the host, the
tool commit and the exact command; `census-time.txt` is `/usr/bin/time -v`.
`SHA256SUMS` covers the three.

| Field | Value |
|---|---|
| Run | [Backfill transparent events, action `census`, run 34184785923](https://github.com/valargroup/enhance-pir/actions/runs/34184785923) |
| Tool commit | `742b91a414acecbfce22a3af733e340cc095dc8e` (`shard-census`, release, `-C target-cpu=x86-64-v3`) |
| Host | `enhance-pir-coordinator-01`, 8 vCPU, `m-8vcpu-64gb-intel`, ams3; ingest unit running |
| Journal | heights 0–3,473,686; 352,873,356 events; genesis `00040fe8…dce08`; anchor `0000000000755137…0be1d` |
| Tiers | `archive-wide` 0–3,262,748 at seal `393216:458752, 63488:65536`; `recent-8k` 3,262,749–3,473,686 at seal `98304:114688, 7936:8192` (both derived by `SealPolicy::for_geometry`, printed in the raw output) |
| Elapsed | 3 min 37 s wall; peak RSS 714,696 KiB |

## Result

| Quantity | Archive tier | Recent tier | Whole set |
|---|---:|---:|---:|
| Shards | 160 (159 sealed on page rows, 1 on the geometry change) | 14 (13 sealed on page rows, 1 open tail) | 174, ids 0–173 |
| Segments per table | 1 in every shard | 1 in every shard | no shard needs a second segment |
| Packed page rows per shard, p50 / max | 63,501 / 64,018 | 7,936 / 8,079 | |
| Scripts per shard, p50 / max | 152,352 / 284,221 | 34,509 / 45,454 | |
| Filters | 58.82 MB | 1.23 MB | 60.05 MB |
| Pinned plaintext (packed rows) | 56.4 GB (352 MB per shard) | 0.82 GB (59 MB per shard) | 57.2 GB |
| Script–shard pairs | 23,529,066 | 491,458 | 24,020,524 over 9,264,547 scripts |

Scripts appear in both tiers: 66,305 scripts have events on both sides of the
cutoff, 312,041 only in the recent tier, 8,886,201 only in the archive. The
per-script figures below are merged across tiers by script identity before
the percentiles are taken.

| Per indexable script | mean | p50 | p90 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| Shards matched | 2.59 | 1 | 3 | 9 | 35 | 160 |
| Queries (two per shard) | 5.19 | 2 | 6 | 18 | 70 | 320 |
| Query upload, MB | 2.06 | 0.52 | 2.41 | 7.14 | 33.46 | 40,176 |

The upload figure charges 258,056 B per archive directory query and 430,088 B
per archive page query, 128,008 B for either recent query. It is the private
query bill of one script with no filter false positives; it excludes filters,
setup, responses and transport, and it is a script figure, not a wallet
figure. The maximum is a single script with 552,936 transactions, which pays
for pages rather than shards.

## Sizing against the deployment budgets

Reservations use `runtime::reserved_bytes` as the deployment records them:
576 MiB per `archive-wide` shard and 256 MiB per `recent-8k` shard, both
tables together.

- Archive: 160 shards over two owners is 80 shards each, 45.0 GiB reserved
  against a 48 GiB cache. That fits with 6.25% of the cache free, which is
  under the 15% headroom `shard-assign plan` keeps by default; the assignment
  is planned with `--headroom 0.06` or the c-8 measured RSS (589 MiB per
  shard, 46.0 GiB per owner) decides that the cache budget moves. Gate 6
  measures the whole process on the target host before either is accepted.
- Recent: 14 shards is 3.5 GiB reserved against a 5 GiB cache on every
  replica; retaining three superseded tail revisions adds 0.75 GiB, 4.25 GiB.

These are reservation arithmetic, not measured RSS. The uniform 162-shard
proxy in the evidence README is superseded by this set for sizing.

## What this does not establish

The census predicts boundaries from the same sealer the publisher uses; the
publication's emitted `shards.json` is compared to it with
`shard-census --compare-map` once it exists. Recent catch-up and old-birthday
restoration costs across the mixed set are measured under Gate 6 with real
syncs, not projected from these per-script counts.
