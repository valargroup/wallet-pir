# `archive-wide` over the complete chain

Archived 2026-09-07. The first census of a geometry whose directory and page
tables have different row counts, and the first direct measurement of `g` — the
number of shards a script appears in — at anything other than the pinned 8,192.

## Provenance

Both runs are the complete genesis-to-tip journal at
`/srv/zakura/transparent-event-data`, censused over its whole extent, so these
are `fullchain-*` figures in the sense the [README](README.md) defines. They
are not comparable with the `genesis-*` files, which cover 9.4% of chain
height and were misread as full-chain once already.

| | |
|---|---|
| Journal | heights 0–3,473,686, 3,473,687 blocks, 352,873,356 events |
| Genesis | `00040fe8ec8471911baa1db1266ea15dd06b4a8a5c453883c000b031973dce08` |
| Anchor | `0000000000755137ff64d560a32b9190e62ae085923c9e69930c05550f50be1d` |
| Build | `0aaf25cb6bce450e4312fffbafbd1dcb62bf2f5a` |
| Geometry | `archive-wide` — 32,768 directory rows, 65,536 page rows, 3,584-byte rows, 2 inline events |
| Policy | `393216:458752,63488:65536`, which is what `SealPolicy::for_geometry` derives |

| File | Command | Elapsed | Peak RSS |
|---|---|---|---|
| [`fullchain-archive-wide.txt`](fullchain-archive-wide.txt) | `--geometry archive-wide --policy 393216:458752,63488:65536 --per-shard --placement` | 4:31 | 328,576 kB |
| [`fullchain-archive-wide-matches.txt`](fullchain-archive-wide-matches.txt) | `--geometry archive-wide --policy 393216:458752,63488:65536 --shard-matches` | 2:40 | 476,152 kB |

The policy was passed explicitly rather than left to the default so that one
sealer ran rather than six; the value is the one the geometry implies, so this
is the boundary set the publisher would actually produce.

## What it says

**162 shards, 161 sealed, and every one of them closed on page rows.** Not a
single shard reached the 393,216-script target. The deploy notes expected the
binding limit to vary along the chain — at 8,192 it is 1,042 page-row closures
against 48 script closures — and at this geometry it does not vary at all. The
directory is simply never the constraint, which is the whole premise of pairing
a 32,768-row directory with a 65,536-row page table.

**The narrower directory holds.** The real two-choice placer put all 162 shards
in one directory segment each: `162 placed against 162 modelled; 0 of 162
shards need more than one`. The fullest row anywhere holds 11 of its 14 slots,
p50 7, p95 10. The busiest shard holds 284,221 scripts against a 458,752
capacity — 62.0%, which is exactly what the deploy notes predicted. This is the
clause of the acceptance rule that would have rejected the candidate outright,
and it passes on measurement rather than on projection.

**Storage is better than the projection it replaces.** 57.1 GB of pinned
plaintext at 64.5% live utilisation, against the 76.1 GB and 48.4% projected
for a 65,536/65,536 shape. Halving the directory is where the difference comes
from: shard count is set by the page table, so a narrower directory costs no
extra shards and pins half the directory bytes.

## `g`, and what it costs a restoring wallet

Measured over **9,264,547 distinct indexable scripts**, 23,922,656
script-shard pairs:

| | mean | p50 | p90 | p95 | p99 | max |
|---|---:|---:|---:|---:|---:|---:|
| `g` (shards holding a script) | 2.58 | 1 | 3 | 9 | 35 | 156 |
| directory queries (`2g`) | 5.16 | 2 | 6 | 18 | 70 | 312 |
| restoration MB | 2.07 | 0.52 | 2.41 | 7.14 | 33.46 | 40,176.58 |

**Corrected 2026-09-07, after measuring the baseline.** This section first
compared against the 8,192 figures the deploy notes carried — mean `g` 27.4,
mean restoration 3.75 MB — and claimed a 10.6-fold fall in `g` and a fall in
restoration bytes. Those figures are not full-chain. Measured with this tool
over this journal at this anchor
([fullchain-8192-matches.txt](fullchain-8192-matches.txt)), 8,192 gives mean
`g` 6.55 and mean restoration 2.03 MB, and the real comparison is:

| | 8,192 | `archive-wide` | |
|---|---:|---:|---|
| mean `g` | 6.55 | 2.58 | 2.54x fewer |
| mean `2g` queries | 13.09 | 5.16 | 2.54x fewer |
| mean restoration MB | 2.03 | 2.07 | **1.02x worse** |
| p50 restoration MB | 0.26 | 0.52 | **2.00x worse** |
| p90 restoration MB | 1.54 | 2.41 | **1.56x worse** |
| p95 restoration MB | 7.30 | 7.14 | 0.98x better |
| p99 restoration MB | 44.42 | 33.46 | 0.75x better |
| max restoration MB | 12,046 | 40,177 | **3.34x worse** |

So `archive-wide` is a trade, not a win. The ordinary wallet pays about double
the restoration bytes; the fleet holds 91.1 GiB of prepared runtime instead of
273 GiB.

The mechanism is the **page** table, not the directory. Directory bytes do
favour `archive-wide` — 1.33 MB mean against 1.68 MB — because `g` falls faster
than the 128,008-to-258,056-byte query grows. But a page query costs 430,088
bytes at 65,536 page rows against 128,008 at 8,192, a 3.36-fold rise, and the
median script has `g` = 1 with a handful of fragments, so page queries are most
of what it actually pays. Widening the page table is what hurts the ordinary
wallet, and it is the half of `archive-wide` that buys the shard-count fall.

That points at [`archive-32k`](fullchain-archive-32k-notes.md) — the same
32,768-row directory with a 32,768-row page table — as the shape that might
keep the fleet-RAM win without doubling the median wallet's bytes.

Against the deploy plan's acceptance rule — *reject if it introduces ordinary
multi-segment shards, or increases **both** median restore bytes and prepared
memory* — `archive-wide` is not rejected: it increases median restore bytes and
decreases prepared memory. That is a decision about who pays, not a passing
grade, and it is not one this census can make.

The 40 GB maximum is one script, and it is not a wallet. This is a
chain-script distribution: exchanges and heavily reused addresses dominate its
tail in a way a real wallet population does not, and no user has a 40 GB
restoration. It is reported rather than trimmed because the server has to serve
it either way.

## What this does not establish

The 8,192 comparison above is **quoted from the deploy notes, not re-measured
with this tool over this journal**. The acceptance rule asks for coverage-matched
data, and until `--geometry recent-8k --shard-matches` is run over the same
journal the comparison is between a measurement and a citation. That run needs
roughly 12 GB of spill under the runner's temporary directory, which is on the
coordinator's root disk rather than the 1 TB `/srv/zakura` volume, so check the
space before starting it.

Nothing here measures latency, resident memory under load, or wallet bytes over
a real network. `shard-residency` and `shard-scaling` have never been run at
this geometry, and the fleet-RAM figure below is a formula, not an observation:
576 MiB per shard — a 224 MiB directory runtime plus a 352 MiB page runtime —
gives 91.1 GiB across 162 shards, matching the deploy notes' estimate but
confirming nothing.

No set has been published at this geometry, and no wallet has synced from one.
