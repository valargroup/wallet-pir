# Directory geometry: what the instrumentation can now measure

Date: 2026-09-06. Status: instrumentation landed; the decisive run has not been
made. Nothing here is a mainnet measurement.

The question this answers is whether the directory's script capacity is fixed at
28,672 for a reason, and whether packing more entries into a row to double it
would help. It is asked now because the directory has become the limit that
closes a shard: at the shipped policy 3 of the 5 sealed shards in
[census-v4.txt](census-v4.txt) closed on scripts, and under packing
[the projection](packed-row-projection-notes.md) records that no shard reaches
the page-row target at all.

## Where 28,672 comes from

`DIRECTORY_ROWS * DIRECTORY_SLOTS` = 2,048 x 14. Neither factor was chosen for a
workload:

- 2,048 is the smallest row count `params_for_simplepir` accepts, and row counts
  pad up to a multiple of it, so nothing smaller exists.
- 3,584 bytes is one scheme instance and the smallest legal row width. The next
  legal width is 7,168.
- 14 is `(3584 - 4) / 248`, where the 248-byte entry is 56 bytes of header and
  192 bytes of two inline events. 4 + 14 x 248 leaves **108 dead bytes in every
  directory row** — real, but 140 short of a fifteenth slot.

So the directory is already the smallest table this scheme will serve. The
number is the scheme's floor.

## What each dimension costs

Computed from the pinned `ipir-sp` rev `223626f` using the formulas the
production code uses — `max_query_bytes` in the shard server, `open_segment` in
the wallet client — rather than a model of them. The 2,048-row upload matches
[measurement-v4.json](measurement-v4.json) exactly: its directory-only workloads
paid 1,925,280 bytes over 20 queries, which is 96,264 each.

| Directory rows | Upload per query | Response | Published setup |
|---|---:|---:|---:|
| 2,048 (compiled directory) | 96,264 B | 5,136 B | 14,336 B |
| 4,096 (compiled pages) | 106,504 B (+10.6%) | 5,136 B | 14,336 B |
| 8,192 | 128,008 B (+33.0%) | 5,136 B | 14,336 B |
| 16,384 | 169,992 B (+76.6%) | 5,136 B | 14,336 B |

These are now pinned as a test rather than a note —
`server/transparent-shard-server/tests/geometry_costs.rs` derived the identical
figures independently, which is the reason to trust either.

Only the upload moves, because only the upload carries a term in `db_rows`.
Response and published setup are functions of `db_cols`, which is the row width.
That asymmetry is the whole answer to the question as asked: **row width is the
expensive way to buy directory slots and row count is the cheap one.** Doubling
the width to 7,168 would give 28 slots a row but double both the response and
the setup; doubling the count gives the same capacity for +10.6% on the upload
alone.

Packing more entries into the existing row is the third option and the worst of
the three. The only bytes available are the 108 spare (not enough), the 40-byte
script field (26 bytes gives 15 slots, +7%, at the cost of a published coverage
limit), or the inline events, which are 77% of the entry:

| Inline events | Slots per row | Scripts per segment |
|---|---:|---:|
| 0 | 63 | 129,024 |
| 1 | 23 | 47,104 |
| 2 (compiled) | 14 | 28,672 |
| 3 | 10 | 20,480 |

Two inline events are what keep 79% of active scripts from ever issuing a page
query, and a page query is ~107 KB against the 4,096-row table packing left
behind. Buying directory slots by giving them up
trades a cheap term for an expensive one. The census can now score all four rows
of that table, so the trade is measurable rather than argued.

## Placement has less headroom than the model shows

Every archived directory figure models placement as `scripts / slots`. The
census can now run the builder's own two-choice placer instead — one
implementation, shared with `build.rs`, so a census cannot report a placement
the builder would not produce.

Measured over a **synthetic** 6,000-block journal, not mainnet
(`journalgen` in the session scratchpad; mostly fresh scripts, so shards reach
their script target):

| Script target | Load | Shards stacking a 2nd directory segment | Fullest row |
|---|---:|---:|---:|
| 25,000 (shipped) | 87.2% | 3 of 5 | 14 of 14 |
| 24,576 | 85.7% | 1 of 5 | 14 of 14 |
| 22,000 | 76.7% | 0 of 6 | 13 of 14 |
| 20,000 | 69.8% | 0 of 6 | 12 of 14 |

At 4,096 directory rows the same targets scale: 44,000 into 57,344 slots is also
76.7% and also the last one that fits, and 25,000 — which stacked 3 of 5 shards
at the compiled geometry — places with the fullest row at 9 of 14.

So two-choice placement into 14-slot rows appears safe to roughly **77% of slot
capacity**, and the shipped target sits at 87%.

**This does not say the published set is wrong.** The real set was built at
25,000 and [measurement-v4.json](measurement-v4.json) records one directory
segment throughout, so on the real journal nothing overflowed. Overflow at this
load is probabilistic, not certain, and six shards is a small sample. What the
synthetic run establishes is that the margin is thin enough for the outcome to
depend on the script set — which is exactly the thing `scripts / slots` cannot
show, and the reason the real journal now needs a `--placement` run.

Note also that a shard which overflowed reports its row load *after* the extra
segment was added, so a low load there means it stacked rather than that it had
room. The census reports the two figures apart for that reason.

## What is now instrumented

- `--directory-rows-per-segment`, `--directory-row-bytes` and `--inline-events`
  score a candidate directory geometry, refusing shapes the scheme would round
  up. The inline allowance moves boundaries rather than only arithmetic, and the
  census says so when it is overridden.
- `--placement` runs the real placer and reports segments, fullest row, and
  whether any shard overflowed.
- `--shard-matches DIR` counts how many shards each exact script appears in, by
  external sort, and reports the `2g` directory queries that implies. This is
  the term a wider shard buys down and it could not be derived from occupancy.
- Per-table pinned bytes: at the compiled geometry the split is 33.3%
  directory, 66.7% pages, the page table having been halved to 4,096 rows when
  packing landed.
- `ByteCharges` in the wallet is now split by table, so a measurement can show
  the transfer that a wider shard causes: two inline events are granted per
  script *per shard*, so merging shards keeps fewer histories inline and moves
  cost from the directory to the pages.

With every override at its compiled default the census output is byte-identical
to the previous build's, over the same journal, except for two added lines. That
is what establishes the refactor moved no boundary.

## What has not been measured

The full-journal sweep, which is the run that decides anything. It needs the
coordinator's journal and was not reachable from the machine this work was done
on. Until it runs, nothing here selects a geometry: the cost table says what a
candidate would cost per query, and the placement table says how much headroom a
target has, but neither says how many shards a restoring wallet would open or
what it would pay in total.
