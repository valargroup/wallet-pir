# v5 packed page rows, measured

Measured 2026-09-06 on the coordinator, from commit `9dfd717`, against the
journal at `/srv/zakura/transparent-event-data/superseded-v1` — Ironwood
activation through height 3,473,474, 45,332 blocks, 869,283 events, the same
coverage as every other figure in this directory. Published to
`/srv/zakura/transparent-shards-v5`, which is a directory of its own because a
set is published under one manifest schema and the loader refuses to mix.

- [census-v5.txt](census-v5.txt) — boundaries and utilisation
- [measurement-v5.json](measurement-v5.json) — six workloads over real HTTP with real PIR

## Storage

| | v4 | v5 |
|---|---:|---:|
| Generations | 6 | 6 |
| Page rows | 40,003 | 20,897 |
| Live bytes | 98.4 MB | 96.1 MB |
| **Pinned plaintext** | **220.2 MB** | **132.1 MB** |
| Live share of pinned | 44.70% | 72.75% |
| Generations needing a second page segment | 0 | 0 |

`du` reports 127 MiB on disk. The census ran in 2.15 s at 61 MB resident.

The projection archived in
[the geometry sweep](packed-row-geometry-sweep-notes.md) said 132.1 MB over six
generations, and that is what was built. The builder asserts it emits exactly
the rows the packing rule demands and the publisher asserts the builder and the
sealer agree; both held over 869,283 real events, which is what turns the
projection into a measurement.

## What a wallet pays

Every workload reconstructs exactly — event multiset, UTXO set and
per-transaction history against an independent traversal, not balances.

| Workload | v4 | v5 | |
|---|---:|---:|---:|
| 20 and 100 unused scripts | 318,816 B | 324,689 B | +1.8% |
| 10 small histories | 2,512,907 B | 2,449,314 B | −2.5% |
| 10 median histories | 2,533,451 B | 2,449,314 B | −3.3% |
| Restoration, 5 active and 95 unused | 1,430,660 B | 1,415,989 B | −1.0% |
| Largest single history | 351,063,398 B | 294,605,085 B | **−16.1%** |

Query counts are identical everywhere, which is the contract: packing changes
where a fragment sits, not how many a history has. What changed is the size of
each page query — 128,008 bytes against a 8,192-row table, 106,504 against
4,096 — and the number of generations a wallet opens, which fell from seven to
five for the small and median wallets and from fourteen to twelve for the
largest, saving 38,650 bytes of published setup in each case.

The largest history is the case this design has always been weakest on, and it
is where most of the saving landed: 56.4 MB, almost all of it query upload. It
remains about 2.7 times the extrapolated compact-scanning baseline for one
script, down from about 3.1.

**The public floor grew by 5,873 bytes**, which every wallet pays whether or not
it queries. Filters are per generation and sized by the distinct scripts in one,
so a script active either side of a boundary is an element in both; the packed
boundaries fall elsewhere than v4's and catch slightly more of that repetition.
It is 1.8% on a wallet that retrieves nothing, against 2.5% to 16% saved by
every wallet that retrieves anything, and it is a real cost rather than a
rounding artefact.

## What this does not establish

The census figures model directory placement as `scripts / slots` rather than
running the real two-choice placer, so directory segment counts here — and in
the archived v4 census, which does the same — are a lower bound.

Unchanged from the v4 evidence: this is one 39-day window of chain, and the
early chain has a different script and event distribution, which is what a
full-journal census is for. The synthetic wallets are script groupings drawn
from the journal, not user traces. The scanning comparison extrapolates a
measurement of 1,152 blocks. Device latency and memory, server resident bytes
per loaded generation, revision churn, and the exceptional segment path in
production all remain unmeasured.
