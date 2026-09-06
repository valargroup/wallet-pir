# Choosing a page table for packed rows

Measured 2026-09-06 on the coordinator, from commit `328d576`, through three
`census` runs against `/srv/zakura/transparent-event-data/superseded-v1` — the
Ironwood-activation-to-3,473,474 journal behind every other figure here.

Raw output: [packed-row-geometry-sweep.txt](packed-row-geometry-sweep.txt).

Every row below seals on packed demand rather than on a row per fragment, which
is what [the projection](packed-row-projection-notes.md) established was
necessary: packing at the shipped geometry saves nothing, because the page table
is padded to `PAGE_ROWS` whatever it holds.

## What the sweep is scored on

Pinned bytes are not the only cost, and on their own they choose badly.

A wallet must query **every** segment of a generation, because asking for the
one that holds its script would disclose part of the script before PIR begins.
So an n-segment generation multiplies setup, responses and server work by n for
every lookup in it. Segments exist for the indivisible block that fits nowhere,
not for routine capacity. A geometry that makes ordinary generations stack is
not cheaper however small its table, so the census reports how many generations
would need a second page segment and refuses a policy whose page-row capacity
exceeds the segment it is scored against.

Generation count is the second cost. A restoring wallet fetches published setup
per table per generation it opens, so halving the table and doubling the
generations is not obviously a win — and the directory table is charged per
generation too.

## The sweep

Baseline is the shipped v4 build: 6 generations, 220.2 MB, 44.70% of pinned
bytes live.

| Page rows | Script target | Generations | Pinned | Live share | Stacked segments |
|---:|---:|---:|---:|---:|---:|
| 8,192 | 24,576 | 5 | 183.5 MB | 52.07% | 0 |
| 8,192 | 27,000 | 5 | 183.5 MB | 51.98% | 0 |
| 4,096 | 16,384 | 8 | 176.2 MB | 55.08% | 0 |
| 4,096 | 20,480 | 7 | 154.1 MB | 62.57% | 0 |
| **4,096** | **24,576** | **6** | **132.1 MB** | **72.78%** | **0** |
| 4,096 | 27,000 | 6 | 132.1 MB | 72.75% | 0 |
| 2,048 | 16,384 | 11 | 161.5 MB | 60.71% | 0 |
| 2,048 | 24,576 | 11 | 161.5 MB | 60.70% | 0 |
| 2,048 | 27,000 | 11 | 161.5 MB | 60.70% | 0 |

## What it says

**A 4,096-row page table at the script target already shipped is the pick.** It
stores 132.1 MB where v4 stores 220.2 MB, a 40% cut, over the same six
generations — so a restoring wallet opens no more generations and fetches no
more published setup than it does today. No ordinary generation needs a second
segment at any point in the range.

**Halving the table again is worse, not better.** At 2,048 rows the page limit
binds almost every generation, the range splits into eleven, and pinned bytes go
*up* to 161.5 MB — because the directory is charged per generation, and eleven
directories cost more than the page rows saved. The script target stops
mattering entirely: 16,384 and 27,000 produce identical output, because pages
close every generation before scripts get near their limit.

**Leaving the table at 8,192 leaves most of the saving on the floor.** Sealing
on packed demand alone is worth 220.2 → 183.5 MB, because two of v4's six
generations were closed by page rows that were mostly padding. But the table is
then less than half used, and that slack is pinned.

The 24,576 and 27,000 script targets give the same six generations and the same
132.1 MB. 24,576 is the better of the two because both limits stay engaged —
three generations close on scripts and three on pages — where at 27,000 pages
close all five sealed generations and the script limit is inert. A limit that
never binds is one that has stopped describing the content.

## What this is not

Still a projection. It assumes a packed builder emits exactly the rows the
packing rule demands; nothing has been built or published, and that equality is
what the builder's own accounting check has to establish. Directory placement is
modelled as `scripts / slots` rather than run through the real two-choice
placer, so directory segment counts here and in the archived v4 census alike are
a lower bound. And this is one journal: the early chain has a different
script and event distribution, which is what the full-journal census is for.
