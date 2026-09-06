# Shard utilisation evidence

Archived 2026-09-06. These are the raw outputs behind the utilisation figures
in [the architecture document](../../transparent_pir_architecture.md), which
previously warned that its census output was not linked and should not be
quoted as a reproducible claim. This is that output.

All of it covers the same journal: Ironwood activation through height
3,473,474, 45,332 blocks, 869,283 events. The chain data and the published
bytes are real; the wallets in the measurement are synthetic script groupings
drawn from the journal, not user traces.

| File | What it is |
|---|---|
| `census-baseline.txt` | Census at the geometry in use before this work: 17,920-byte page rows, `PAGE_ROWS` 4,096 |
| `census-v4.txt` | Census at the geometry after it: 3,584-byte page rows, `PAGE_ROWS` 8,192 |
| `measurement-v4.json` | Six workloads synced against the published v4 set over real HTTP with real PIR |
| `packed-row-projection.txt` | The v4 census again, with the row demand packed short histories would need carried alongside it. See [the notes](packed-row-projection-notes.md) |

## What changed between the two censuses

| | Baseline | v4 |
|---|---:|---:|
| Generations | 21 | 6 |
| Directory rows used | 27.0% | 69.8% |
| Page rows used | 43.1% | 82.6% |
| Page slots filled | 8.7% | 45.2% |
| Live bytes of pinned | **6.04%** | **44.70%** |
| Plaintext tables | 1,695.5 MB | 220.2 MB |

Two changes produced this, and they are separable. Narrowing the page row from
17,920 to 3,584 bytes raised page fill, because a page belongs to one script and
the median script that touches one has a handful of events. Raising `PAGE_ROWS`
from 4,096 to 8,192 raised *directory* fill, because page rows are what close a
generation, so that figure decides how many scripts a generation accumulates
before sealing.

## What it cost

Every workload still reconstructs exactly — event multiset, UTXO set and
per-transaction history against an independent traversal of the journal, not
merely equal balances. Realistic workloads got cheaper. One did not:

| Workload | Baseline geometry | v4 |
|---|---:|---:|
| 20 and 100 unused scripts | 439,230 B | 320,000 B |
| Restoration, 5 active and 95 unused | 1,549,858 B | 1,430,000 B |
| Largest single history | 75,817,678 B | 351,060,000 B |

The largest history is a 94,332-event script — an exchange or pool address, not
a user wallet. Narrower pages give it proportionally more rows, and a wider page
table makes each of its thousands of queries slightly larger. It is now about
three times the extrapolated compact-scanning baseline rather than below it, and
bounded evaluation-key reuse is the change that would address it: roughly 86 KB
of each query is packing keys, which is the term that does not shrink with
geometry.

## Reproducing

Both censuses and the measurement come from the `census`, `publish` and
`measure` actions of `.github/workflows/backfill-transparent-events.yml`, run on
the coordinator against the journal at
`/srv/zakura/transparent-event-data/superseded-v1` and the published set at
`/srv/zakura/transparent-shards-v4`.

The census reads a journal and is read-only. The measurement serves the
published set in-process on a loopback port and syncs against it, so it
disturbs no running service.

Note that a census is a function of the seal parameters compiled into the
binary at the time it ran; `census-baseline.txt` cannot be regenerated from
current `main` without reverting the geometry.
