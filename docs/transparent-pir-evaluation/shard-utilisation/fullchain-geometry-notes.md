# Geometry against the whole chain

Measured 2026-09-07 on the coordinator, through the `census` action of the
transparent-event backfill workflow, against the journal at
`/srv/zakura/transparent-event-data` — **genesis through height 3,473,686**,
3,473,687 blocks and **352,873,356 events**. That journal completed the same
day, in 3 h 28 m, after the ingester was switched to the state-backed reader;
the RPC path had been projecting about twenty-five days.

This is the first census over the complete chain. Everything else in this
directory covers a fraction of it:

| file | journal | scope |
|---|---|---|
| `census-v4/v5`, `measurement-v4/v5` | `superseded-v1` | Ironwood activation → 3,473,474, 45,332 blocks |
| `genesis-*` | partial genesis journal | genesis → ~330,000, **9.4% of chain height** |
| `fullchain-*` (here) | genesis journal, complete | genesis → 3,473,686 |

**The `genesis-*` figures are not full-chain numbers**, and their 511 shards and
30.01 GB fleet were mistaken for full-chain ones while this work was in
progress. The correct figure at the same geometry is 1,091 shards and 64.1 GB.
`genesis-geometry-notes.md` scopes itself honestly; the mistake was in reading
it, and it is recorded here so the next reader does not repeat it.

| file | geometry | policy |
|---|---|---|
| [fullchain-8192.txt](fullchain-8192.txt) | 8,192 dir / 8,192 page | `98304:114688,7936:8192` — the shipped pin |
| [fullchain-32768.txt](fullchain-32768.txt) | 32,768 / 32,768 | `393216:458752,31744:32768` |
| [fullchain-65536.txt](fullchain-65536.txt) | 65,536 / 65,536 | `786432:917504,63488:65536` |

Targets are derived the way `shard-publish` derives them — directory capacity is
`rows x 14` slots and the target is `capacity - capacity/7`; the page target is
`rows - rows/32`. At the pinned geometry that reproduces the publisher's own
`98304:114688,7936:8192` exactly, which is what makes the wider rows comparable.

Each run streamed the whole journal in about three and a half minutes at 321 MB
resident, so scoring another geometry is cheap.

## What the three runs say

| rows | shards | fleet pinned | live share | per shard | max scripts | dir capacity used |
|---:|---:|---:|---:|---:|---:|---:|
| **8,192** | **1,091** | 64.1 GB | 63.98% | ~59 MB | 98,667 | **86%** |
| 32,768 | 314 | 73.8 GB | 51.16% | ~235 MB | 223,711 | 49% |
| 65,536 | 162 | 76.1 GB | 48.37% | ~470 MB | 284,221 | 31% |

Three things follow that the earlier partial-journal censuses could not show.

**Shard count falls by about 1.9x per doubling, not 2.33x.** The ratio measured
across the `genesis-*` runs was extrapolated at 2.33 while this work was in
progress, which predicted 201 and 86 shards at the two wider geometries against
the 314 and 162 actually measured. Widening still pays; it pays about half as
much as that extrapolation claimed.

**Fleet plaintext grows as the tables widen** — 64.1 → 73.8 → 76.1 GB, with the
live share falling from 64% to 48%. Wider tables pad more. What falls is
*resident* memory, because the PIR pack matrices are a fixed 96 MiB per segment
whatever the row count and amortise over fewer shards. Disk is not the
constraint here; RAM is, and the two move in opposite directions.

**Pages bind almost everywhere, and the directory has slack that grows.** At the
pinned geometry 1,019 shards close on the page-row target and 48 on scripts,
with the directory at 86% of capacity — nearly binding. At 32,768 and 65,536,
*no* shard reaches the script target and the directory sits at 49% and 31%.
Scripts grow sub-linearly with shard size, so the two tables do not fill
together at full-chain scale the way the layout's sizing assumes.

That points at a geometry nobody has scored: a **wide page table with a narrower
directory**. At 65,536 pages with a 32,768 directory the observed maximum of
284,221 scripts sits at 62% of capacity — comfortable — while directory queries
fall from 430 KB to 258 KB. Do **not** pair a 16,384 directory with 32,768
pages: 223,711 against 229,376 capacity is 2.5% headroom, and one shard
overflowing into a second segment costs every wallet an extra directory query
for that shard forever.

## What these runs do not establish

**`g`, the number of shards a script appears in, was not measured.** These runs
were made without `--shard-matches`, so every restoration-cost figure at the
wider geometries is extrapolated from the `genesis-*` distribution — and that
same extrapolation was already 55% wrong for shard count. A geometry decision
worth roughly $1,000/mo should not rest on it. The counter is implemented and
runs over the full journal; it costs an external sort and spill space.

**Where the 48 script-bound shards sit is not shown.** These runs predate
`--per-shard`, which emits heights and the closing limit for every shard. The
binding limit is not uniform along the chain, so a geometry chosen on the
aggregate is chosen on a mixture, and a per-era geometry cannot be evaluated
without it.

**These are projections, not a built set.** `proj_*` figures come from the
sealer's own accounting over the journal. The only set actually built at any of
these geometries is the three-shard Ironwood set at the pinned 8,192.
