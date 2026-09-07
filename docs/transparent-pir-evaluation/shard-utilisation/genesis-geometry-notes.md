# Directory geometry against the genesis journal

> **Scope, restated.** This covers genesis through ~330,000 — 9.4% of chain
> height, and the densest part of it. The shard counts and fleet sizes below are
> for that range and **not for the chain**; they were read as full-chain during
> later work, which is the mistake this note exists to prevent. The whole-chain
> census is [fullchain-geometry-notes.md](fullchain-geometry-notes.md): 1,091
> shards and 64.1 GB at the same geometry, against the 511 and 30.01 GB here.
> The relative comparisons below still hold; only their absolute scale does not.

Measured 2026-09-07 on the coordinator, through the `census` action of the
transparent-event backfill workflow, against the journal at
`/srv/zakura/transparent-event-data` — **genesis through height ~330,000**, which
is a different journal from every other figure in this directory. Everything
else here covers Ironwood activation through 3,473,474, from
`superseded-v1`. The two do not overlap, so nothing below is comparable with
[census-v5.txt](census-v5.txt) or [measurement-v5.json](measurement-v5.json).

| File | Commit | Geometry | What it varies |
|---|---|---|---|
| [genesis-census-2048.txt](genesis-census-2048.txt) | `35ba75e` | 2,048 dir / 4,096 page | the shipped sweep, with real placement |
| [genesis-census-4096.txt](genesis-census-4096.txt) | `5b545ef` | 4,096 dir / 4,096 page | the same sweep, directory doubled |
| [genesis-targets-greedy.txt](genesis-targets-greedy.txt) | `5b545ef` | 4,096 / 4,096 | script targets 28,672–49,152, greedy placer |
| [genesis-targets-relocating.txt](genesis-targets-relocating.txt) | `11dd67b` | 4,096 / 4,096 | the same targets, relocating placer |
| [genesis-targets-8192.txt](genesis-targets-8192.txt) | `11dd67b` | 8,192 / 8,192 | targets 57,344–98,304 |
| [genesis-cost-2048.txt](genesis-cost-2048.txt) | `5ba272b` | 2,048 / 4,096 @ 24,576 | restoration cost per script |
| [genesis-cost-4096.txt](genesis-cost-4096.txt) | `5ba272b` | 4,096 / 4,096 @ 49,152 | the same, at the wider directory |
| [genesis-cost-8192.txt](genesis-cost-8192.txt) | `5ba272b` | 8,192 / 8,192 @ 98,304 | the same, wider again |

**The journal grew under the runs**, from 328,001 to 332,001 blocks, because the
backfill is still going at roughly 40 blocks a minute. Shard counts therefore
drift about half a percent between runs and are not exactly comparable. Every
conclusion below rests on differences far larger than that; none rests on a
comparison of that size.

## The journal is nothing like the Ironwood sample

150.1M events over 328,001 blocks is **458 events a block**, against 19.2 in the
Ironwood sample. Early Zcash was almost entirely transparent and mining pools
batched payouts into transactions with hundreds of outputs, each one a receive
event under its own script, which is the shape that produces this.

Density is not stationary within the journal either:

| heights | events/block | |
|---|---|---|
| 0 – ~12,000 | 69 → 393 | launch ramp |
| ~12,000 – ~102,000 | 200–260 | plateau |
| **~102,000 – ~110,600** | 572 → **2,171** → 257 | an 8x burst, ~8,000 blocks wide |
| ~110,600 – ~315,000 | 250 → 600 | slow doubling |
| ~315,000 – 332,000 | 736 → **1,484** | accelerating, still rising at the head |

Two things follow. Sizing from this journal is provisional, because the head has
not plateaued. And the changes are a burst plus a continuous climb rather than a
threshold, so a geometry chosen per height era would have no stable boundary to
key on — the design's answer to era is a variable block span at fixed geometry,
which is what the span figures below show it doing.

## Placement was modelled, and the model was wrong in the expensive direction

Every archived directory figure before these runs modelled placement as
`scripts / slots`. Run against the builder's own rule, at the shipped 24,576
target:

```
segments  3623 placed against 2564 modelled; 1072 of 2551 shards need more than one
fullest row  14 of 14, p50 14, p95 14
```

**42% of shards did not fit one directory segment.** A wallet must query every
segment of a shard, so each of those doubles the directory query *and* the
published setup for everyone who touches it. This never appeared in the Ironwood
measurements because that range reported one segment throughout; the early chain
is where it bites, and the genesis republish is where it would have landed.

Two independent fixes, both measured:

**Halve the load factor.** At 4,096 directory rows the same 24,576 target is 43%
loaded rather than 86%, and overflow goes to zero — 0 of 2,551 shards, fullest
row 11 of 14. Costs +10,240 B on a directory query, nothing on response or setup.

**Let a full row make room.** The placer never moved a script once placed, so one
crowded row cost a whole segment while the table sat at 84%. Following a
breadth-first augmenting path to a row with space holds 86% of capacity in one
segment. The client is untouched: a script is still in one of its own two
candidate rows, still two queries.

| target (4,096 rows) | greedy overflow | relocating overflow |
|---|---:|---:|
| 40,960 | 0 of 1,367 | 0 of 1,374 |
| **49,152** | **283 of 1,113** | **0 of 1,119** |

## What the geometry is worth

Fleet pinned plaintext, packed, over the same journal:

| directory / page | target | shards | pinned | live share |
|---|---:|---:|---:|---:|
| 2,048 / 4,096 (shipped) | 24,576 | 2,551 | 56.27 GB | 40.0% |
| 4,096 / 4,096 | 40,960 | 1,374 | 40.34 GB | 51.7% |
| 4,096 / 4,096 | 49,152 | 1,119 | 32.85 GB | 61.4% |
| 8,192 / 8,192 | 81,920 | 517 | 30.36 GB | 60.2% |
| 8,192 / 8,192 | 98,304 | 484 | 28.42 GB | 63.6% |

At 49,152 the directory runs at 86% of capacity and the page table at 69%, with
414 of 1,113 shards closing on page rows — the first geometry in this series
where both pinned tables are working rather than one idling.

This section originally concluded that going wider was mispriced, on the
argument that a history's fragment count barely falls as shards widen. That is
wrong: fragments are per script *per shard*, so they fall with shard count too.
The cost measurements below replace this reasoning and reverse its conclusion.

## What a wallet pays, which is what the earlier sections got wrong

Everything above chooses a geometry on storage and placement. Both are real
costs and neither is the one that matters most, and choosing on them produced
the wrong answer.

The missing input was `g`, the shards a script appears in, which decides the
directory queries a restoration makes. It was never measured before these runs.
Over the genesis journal at the shipped target it is **mean 27.4, p50 2, p95
140, p99 563, maximum 2,794** — not the ~1 a synthetic journal suggested. This
is a chain of heavy address reuse: 2.46M distinct scripts carrying 163M events,
65 apiece.

With `g` in hand the census prices a restoration directly — two directory
queries per shard a script appears in, one page query per fragment, at each
candidate's own query size:

| | 2,048/4,096 @24,576 | 4,096/4,096 @49,152 | 8,192/8,192 @98,304 |
|---|---:|---:|---:|
| shards | 2,797 | 1,191 | **511** |
| fleet pinned | 56.27 GB | 34.97 GB | **30.01 GB** |
| query bytes | 96,264 / 106,504 | 106,504 | 128,008 |
| `g` mean / p99 | 27.4 / 563 | 19.4 / 376 | **12.2 / 202** |
| fragments | **9.94M** | 12.08M | 12.12M |
| cost p50 | 0.39 MB | 0.43 MB | **0.26 MB** |
| cost p90 | 7.89 MB | 7.67 MB | **7.30 MB** |
| cost p95 | 28.36 MB | 26.20 MB | **23.04 MB** |
| cost p99 | 118.33 MB | 91.91 MB | **65.67 MB** |
| cost max | 1,760.76 MB | **1,482.22 MB** | 1,561.95 MB |
| cost mean | 5.71 MB | 4.66 MB | **3.75 MB** |

**The widest candidate wins every percentile but the maximum**, p99 by 44%, and
takes storage with it. Three things this settles that argument could not:

*The 20% query premium is not the deciding term.* A wider table costs more per
query and is asked 5.5 times fewer of them. Only the product decides, and it
favours width.

*The inline transfer is real and loses anyway.* Fragments rise 9.94M to 12.12M,
22%, because the two inline events are granted per script per shard and wider
shards grant fewer of them. It does not come close to paying for the directory
queries saved.

*The benefit is not linear in width.* At 4,096 the median script gets **worse**
than the shipped geometry — 0.43 MB against 0.39 — because the query grew 10%
while `g` at p50 stayed at 2. The gain arrives at 8,192, where `g` p50 falls to
1 and the median script restores in two queries.

The crossover sits between p99 and the maximum. The worst script is present in
every shard with a history too long to shrink, so its fragment count is fixed by
its event count and the query premium bites with nothing to offset it. That case
is the one the design already answers with query budgets and resumable work.

### The recommendation this supersedes

An earlier draft of these notes recommended 4,096 / 4,096 at 49,152, chosen
before `g` was measured. It is superseded. The evidence above says 8,192 / 8,192
near 98,304, and says so on the axis that matters rather than on storage.

**Server cost remains unmeasured and moves the other way.** Each shard pins
about 59 MB at this geometry against 29, and PIR preprocessing scales with the
table. There are fewer shards to hold and each is larger, and which way that
nets is the one axis that could still overturn this.

## The density burst, as a proxy for a spam era

The 8x burst at ~106,000 is the closest thing this journal holds to a
sandblasting period, and the sealer absorbed it without any exceptional path. At
the 24,576 target, across the whole journal:

- blocks per shard **min 11**, p50 83, p95 573, max 2,308 — a 210-fold span
  variation, which is content-based sealing narrowing the range where density
  spikes;
- **no oversized-block seals**;
- packed page rows max **4,092 against a 4,096 capacity**, with 63 shards sealing
  on page-row capacity.

This is evidence about the mechanism, not about heights 1.8M–2.2M. Zcash's
sandblasting was primarily shielded spam and its transparent footprint is a
different shape. Measuring that range needs its own journal: ~400,000 blocks at
40 blocks a minute is about seven days of ingest, and the `start` action refuses
to run while the genesis backfill holds the unit.

## What is still unmeasured

Everything on the wallet side. These are census figures — boundaries, occupancy,
placement and pinned bytes. No set has been published at 4,096 rows and no wallet
has synced against one, so the byte totals a restoring wallet would actually pay
are still a projection. The archived measurements cover a range this journal does
not contain, so that comparison needs a control set published at the current
geometry over the same heights rather than a diff against
[measurement-v5.json](measurement-v5.json).

Server resident memory per loaded shard is also unmeasured and is the figure most
likely to bind in practice: each shard pins ~29 MB of tables, and it is the
loaded-shard count, not the disk total, that decides what a server can serve.
