# Three geometries over the complete chain

Archived 2026-09-07. The authoritative comparison: three geometries censused
with one tool, over one journal, at one anchor. Supersedes every
cross-geometry restoration figure quoted elsewhere in this repository.

## Why this document exists

The deploy notes and `layout.rs` carried restoration figures for 8,192 — mean
`g` 27.4, mean restoration 3.75 MB, "27 shards on average and 2,794 at the
worst" — that do not describe the whole chain. Measured here, full-chain 8,192
is mean `g` **6.55** and mean restoration **2.03 MB**. The earlier numbers
appear to come from the `genesis-*` census covering 9.4% of chain height, which
is the misreading the [README](README.md) was written to warn about after it
produced a 55% error in shard count. It has now happened twice, and the second
time it was used to justify a geometry.

## Provenance

Journal `/srv/zakura/transparent-event-data`, heights 0–3,473,686, 3,473,687
blocks, 352,873,356 events. Genesis
`00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08`, anchor
`0000000000755137ff64d560a32b9190e62ae085923c9e69930c05550f50be1d`. Builds
`0aaf25c` and `4f189a8`. Each policy is what `SealPolicy::for_geometry`
derives, so each is the boundary set its publisher would really produce.

| Geometry | Raw output |
|---|---|
| `recent-8k` 8,192/8,192 | [fullchain-8192.txt](fullchain-8192.txt), [fullchain-8192-matches.txt](fullchain-8192-matches.txt) |
| `archive-32k` 32,768/32,768 | [fullchain-archive-32k.txt](fullchain-archive-32k.txt) |
| `archive-wide` 32,768/65,536 | [fullchain-archive-wide.txt](fullchain-archive-wide.txt), [fullchain-archive-wide-matches.txt](fullchain-archive-wide-matches.txt) |

## What the chain does at each shape

| | `recent-8k` | `archive-32k` | `archive-wide` |
|---|---:|---:|---:|
| Shards | 1,091 | 314 | 162 |
| Closed on page rows | 1,042 | 313 | 161 |
| Closed on scripts | 48 | 0 | 0 |
| Max scripts/shard | 98,667 | 223,711 | 284,221 |
| …against capacity | 114,688 (86.0%) | 458,752 (48.8%) | 458,752 (62.0%) |
| Multi-segment shards | — | 0 of 314 | 0 of 162 |
| Fleet plaintext | 64.1 GB | 73.8 GB | 57.1 GB |
| Fleet prepared runtime | 273 GiB | 137 GiB | 91.1 GiB |
| Directory query | 128,008 B | 258,056 B | 258,056 B |
| Page query | 128,008 B | 258,056 B | 430,088 B |

## What it costs a wallet

Over 9,264,547 distinct indexable scripts:

| | `recent-8k` | `archive-32k` | `archive-wide` |
|---|---:|---:|---:|
| mean `g` | 6.55 | 3.48 | 2.58 |
| mean `2g` queries | 13.09 | 6.97 | 5.16 |
| **p50 restoration MB** | **0.26** | **0.52** | **0.52** |
| mean restoration MB | **2.03** | 2.33 | 2.07 |
| p90 restoration MB | **1.54** | 2.32 | 2.41 |
| p95 restoration MB | 7.30 | 8.52 | **7.14** |
| p99 restoration MB | 44.42 | 42.32 | **33.46** |
| max restoration MB | **12,046** | 24,144 | 40,177 |

`recent-8k` is cheapest at the median, the mean and p90. `archive-wide` is
cheapest at p95, p99 and in fleet memory. `archive-32k` wins nothing: it costs
the median what `archive-wide` does while giving up half the memory saving.

## The median is set by the directory alone

The median script has `g` = 1 and a history inside the two-event inline
allowance, so it issues **two directory queries and never touches the page
table**. Its cost is exactly twice the directory query:

| | directory query | 2x | measured p50 |
|---|---:|---:|---:|
| `recent-8k` | 128,008 B | 0.256 MB | 0.26 |
| `archive-32k` | 258,056 B | 0.516 MB | 0.52 |
| `archive-wide` | 258,056 B | 0.516 MB | 0.52 |

This corrects an intuition worth stating because it is backwards from the
obvious one. Widening the *page* table does not touch the ordinary wallet —
most scripts never read a page. Widening the *directory* doubles what every
ordinary wallet pays, on every sync, forever. Page width is charged only to
scripts with real history, and there it buys fewer shards and so fewer queries.

## And the directory cannot be narrowed independently

Page rows set how long a shard is; shard length sets how many scripts it holds;
the directory has to hold those with the placement slack
`SealPolicy::for_geometry` reserves — a seventh. So the two dimensions are
coupled, and there is no "narrow directory, wide pages" shape available:

| Page rows | Max scripts/shard | Capacity needed | Smallest legal directory |
|---:|---:|---:|---|
| 8,192 | 98,667 | 115,111 | 16,384 — 8,192 holds 114,688, 0.4% short |
| 32,768 | 223,711 | 260,996 | 32,768 — 16,384 holds 229,376, 2.5% short |
| 65,536 | 284,221 | 331,591 | 32,768 |

The 16,384/32,768 row of that table is the pairing the deploy notes already warn
against, and this census reproduces its 223,711 exactly. The 8,192 row is the
more interesting one: the shipped geometry is itself 0.4% under the rule, which
is why 48 of its shards close on scripts rather than page rows. It works, but
it has no placement margin, and that is a property of what ships today.

## The decision taken on this evidence

`archive-wide` for the archive tier; `archive-32k` dominated and not to be
published; the recent tier unchanged. Recorded with its conditions in
[transparent_pir_geometry_decision.md](../../transparent_pir_geometry_decision.md),
which also carries the droplet measurements that settled the server-cost half.

## What this does and does not decide

Against the plan's acceptance rule — *reject if it introduces ordinary
multi-segment shards, or increases **both** median restore bytes and prepared
memory* — neither archive candidate is rejected. Both keep every shard in one
directory segment, and both cut prepared memory. Both also double the median
wallet's bytes.

So the rule does not choose. What is left is a trade the census cannot settle:
`archive-wide` saves 182 GiB of fleet memory — the difference between roughly
nine hosts and twenty-one — and charges every ordinary wallet 0.26 MB more per
sync to do it. That is a product decision about who pays.

Nothing here measures latency, RSS under load, or bytes over a real network.
`shard-residency` and `shard-scaling` have never run at 32,768 or 65,536 rows;
the prepared-runtime figures above are `reserved_bytes` arithmetic. No set has
been published at either archive geometry and no wallet has synced from one.
