# Shard utilisation evidence

Archived 2026-09-06. These are the raw outputs behind the utilisation figures
in [the architecture document](../../transparent_pir_architecture.md), which
previously warned that its census output was not linked and should not be
quoted as a reproducible claim. This is that output.

Most of it covers the same journal: Ironwood activation through height
3,473,474, 45,332 blocks, 869,283 events. Two later sets do not, and the
distinction matters more than it looks:

| files | journal | scope |
|---|---|---|
| everything else here | `superseded-v1` | Ironwood → 3,473,474, 45,332 blocks |
| `genesis-*` | partial genesis journal | genesis → ~330,000, **9.4% of chain height** |
| `fullchain-*` | genesis journal, complete | genesis → 3,473,686, 352,873,356 events |

**Only the `fullchain-*` figures describe the whole chain.** The `genesis-*`
ones were read as full-chain during later work and are not: at the pinned
geometry they report 511 shards and a 30.01 GB fleet where the whole chain is
1,091 shards and 64.1 GB. See
[fullchain-geometry-notes.md](fullchain-geometry-notes.md).

 The chain data and the published
bytes are real; the wallets in the measurement are synthetic script groupings
drawn from the journal, not user traces.

| File | What it is |
|---|---|
| `census-baseline.txt` | Census at the geometry in use before this work: 17,920-byte page rows, `PAGE_ROWS` 4,096 |
| `census-v4.txt` | Census at the geometry after it: 3,584-byte page rows, `PAGE_ROWS` 8,192 |
| `measurement-v4.json` | Six workloads synced against the published v4 set over real HTTP with real PIR |
| `packed-row-projection.txt` | The v4 census again, with the row demand packed short histories would need carried alongside it. See [the notes](packed-row-projection-notes.md) |
| `packed-row-geometry-sweep.txt` | Three censuses sealing on packed demand, across page table sizes and script targets, to choose a geometry. See [the notes](packed-row-geometry-sweep-notes.md) |
| `census-v5.txt` | Census of the shipped packed layout: 4,096-row page table, short histories sharing rows. See [the notes](v5-notes.md) |
| `measurement-v5.json` | Six workloads synced against the published v5 set over real HTTP with real PIR |

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

## Cross-geometry comparison, added 2026-09-07

**[fullchain-geometry-comparison.md](fullchain-geometry-comparison.md) is the
authoritative cross-geometry restoration comparison** and supersedes every such
figure quoted elsewhere in this repository, including in `layout.rs` and the
deploy notes. Three geometries, one tool, one journal, one anchor.

Reading it first will save re-learning what this README already says about
scope: the full-chain 8,192 restoration figures measured there differ
substantially from the ones the deploy notes carried, which appear to have come
from the `genesis-*` census covering 9.4% of chain height.

## `archive-wide`, added 2026-09-07

[`fullchain-archive-wide.txt`](fullchain-archive-wide.txt) and
[`fullchain-archive-wide-matches.txt`](fullchain-archive-wide-matches.txt) are
`fullchain-*` in scope — the complete genesis-to-tip journal — and are the
first census of a geometry whose directory and page tables have *different* row
counts, and the first direct measurement of `g` at anything other than the
pinned 8,192. Unlike everything above them they were produced by a binary that
takes the geometry as an argument, so they can be regenerated from `main`:
the run inputs, anchor and build SHA are in
[fullchain-archive-wide-notes.md](fullchain-archive-wide-notes.md).

The `genesis-*` files are the exception to all of the above. They read a
different journal — genesis through ~330,000, not Ironwood onward — and so
compare with each other and with nothing else here. See
[genesis-geometry-notes.md](genesis-geometry-notes.md), which also records that
the journal grew under the runs, since the backfill was still going.
