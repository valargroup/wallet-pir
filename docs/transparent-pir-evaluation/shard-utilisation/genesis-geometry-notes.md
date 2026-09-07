# Directory geometry against the genesis journal

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
| **4,096 / 4,096** | **49,152** | **1,119** | **32.85 GB** | **61.4%** |
| 8,192 / 8,192 | 81,920 | 517 | 30.36 GB | 60.2% |
| 8,192 / 8,192 | 98,304 | 484 | 28.42 GB | 63.6% |

At 49,152 the directory runs at 86% of capacity and the page table at 69%, with
414 of 1,113 shards closing on page rows — the first geometry in this series
where both pinned tables are working rather than one idling.

Going wider still is diminishing and mispriced. 8,192 rows buys 32.85 → 30.36 GB,
8%, and costs +20% on **every** query of both kinds, because query upload is the
one term that follows row count. Page queries are where that lands worst: a
history's fragment count barely falls as shards widen, so those queries stay as
numerous and each costs more, on the workload already sitting at 2.7x the
scanning baseline. By 98,304 only 6 of 484 shards seal on scripts at all — the
directory capacity is bought and never used.

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
