# Plan: table utilisation through packed pages and generation lanes

Date: 2026-09-06. Status: implementation plan, not a measurement.

## Context

Published generations are mostly empty. Measured over the current set — six
generations covering Ironwood activation to height 3,473,474, archived under
[shard-utilisation](transparent-pir-evaluation/shard-utilisation/):

| | |
|---|---:|
| Directory slots filled | 69.8% |
| Page rows used | 82.6% |
| Page slots filled | 45.2% |
| **Live bytes of pinned bytes** | **44.70%** |
| Plaintext tables | 220.2 MB |

That is already up from 6.04% and 1.6 GB, through two changes: narrowing the
page row from 17,920 to 3,584 bytes, and raising page rows per segment so
generations grow until the directory fills too. Both were parameter choices.
What remains is structural, and parameters cannot reach it.

Two independent causes remain, on orthogonal axes:

**A page row belongs to one script.** The measured distribution is p50 2 events,
p90 5, p95 8. A script that touches a page at all typically fills a few slots of
36, and a three-event history occupies a private 3,584-byte row for 144 bytes of
payload. This is *within-row* waste, and it is why page slots sit at 45%.

**One boundary closes both tables.** The directory is sized by distinct scripts
and the page table by page rows, but a generation seals when either binds — so
the other is left short. Today page rows bind and the directory stops at 69.8%.
This is *which-table-wastes*, and no choice of row counts fixes it, because the
binding quantity is not the same everywhere on the chain.

The second cause is about to get worse. The early chain is running **435 events
per block** against Ironwood's 19, and its mining-pool coinbases produce large
numbers of scripts holding one event each — script-dense and page-light, the
opposite of recent chain. Across a genesis-covering set **the binding quantity
flips partway through**, so a single page-rows-per-segment value cannot make
both tables fill in both regimes.

## Goal, and what is deliberately not the goal

**Optimise the common case: a script with a short history.** That is what the
distribution is made of, and it is what pays the padding today.

**Not the whale.** The 94,332-event history costs 351 MB and is the one workload
losing against compact scanning. Nothing here improves it: its cost is query
*count*, and neither change alters how many rows a long history occupies. The
design's stated answer for large and externally spammed histories is query
budgets and resumable work, and that remains the right answer. Contorting the
table geometry around an exchange address would make the common case worse,
which is the trade that produced the current waste in the first place.

**Also not wallet bytes.** A restoring wallet pays 1.43 MB, 79 times better than
coverage-matched compact scanning. Its cost is dominated by an irreducible
86 KB of packing keys on each 94 KB query, which only bounded key reuse
addresses and which is out of scope. Expect this work to move storage, not the
wallet's bill. Filter and setup bytes fall slightly as a side effect; that is
not the point of it.

## Change 1: packed page rows

Let one page row carry several scripts' histories, the way a directory row
already carries several entries.

A row becomes a count followed by packed, variable-length entries, each an exact
script plus its events. A client scans the row and keeps the entry whose script
bytes match — the same discipline the directory uses, and the same reason:
a row is located by an index that could point anywhere, so identity is checked
against content rather than assumed.

Placement stays **builder-assigned and directory-located**. The directory entry
keeps `first_page` and `page_count`; the builder decides which scripts share a
row. It does *not* become hash-addressed.

That distinction matters and is the reason to prefer this shape:

- Hash placement needs overflow handling, because a bucket can fill. Builder
  placement cannot overflow — the builder simply starts another row.
- A locator that the builder wrote cannot point at another script's data by
  construction. A hash that collided can.
- This is the table where a lost event yields a wrong balance that looks
  entirely normal, so the failure modes worth removing are the silent ones.

A history longer than one row still occupies a run of consecutive rows exactly
as today, so the long-history path degrades to current behaviour rather than
becoming a new one.

Expected: page slots 45% → ~85%, page rows roughly halving.

## Change 2: generation lanes

Give the directory and the page table their own boundary sequences.

Once pages are packed, page rows are sized by *paged events* rather than by
paged scripts, so the two lanes are sized by genuinely independent quantities.
Each then seals on its own, and neither is cut short by the other.

- **Directory lane**: seals on distinct scripts. Carries the public filter,
  since the filter is what a wallet tests before it queries anything.
- **Page lane**: seals on page rows.

**Page boundaries must be a subset of directory boundaries.** A page generation
is a union of whole directory generations, never a partial one. This is what
keeps a directory hit resolving to a single page generation: without it, a
script's paged events could straddle several page generations and a wallet would
pay a query per generation crossed, which would cost the common case more than
the packing saved.

Each lane gets its own published map, manifest chain and tail. The directory
entry's locator gains the page generation it refers to.

Expected: directory slots ~97%, and page rows able to fill independently of how
script-dense the range happens to be.

## Expected effect

Estimates, to be replaced by census output before anything is published:

| | Now | Packed | Packed + lanes |
|---|---:|---:|---:|
| Directory slots | 69.8% | ~70% | ~97% |
| Page slots | 45.2% | ~85% | ~85% |
| Live of pinned | 44.7% | ~60% | **~77%** |
| Storage, this range | 220 MB | ~150 MB | **~117 MB** |

The lane change is worth little without packing and vice versa: fixing one
shifts the bind onto the other. That is the argument for doing both, and for
doing them together.

## Work

**Packed rows** — `pir/transparent-shard/src/records.rs`. A page row becomes
count-prefixed variable entries. Reuse the directory row's shape: an entry
carries its exact script, unused bytes are zero, and a row with content past its
count is refused. Keep the header's height bounds per entry rather than per row,
since a client navigates by them.

**Builder packing** — `pir/transparent-shard/src/build.rs`. Replace the
one-row-per-script loop with a packer that fills a row before starting another,
walking scripts in sorted order so the result stays a deterministic function of
the events. Long histories keep their contiguous runs.

**Row accounting** — `pir/transparent-shard/src/layout.rs`. `page_rows_for` is
currently per script and is what the sealer prices with. It becomes a function
of packed occupancy, so the sealer needs the packer's accounting rather than a
closed form. Keep it incremental: the seal pass streams blocks and must not
re-pack a candidate range per block.

**Lanes** — `manifest.rs`, `pir/transparent-filter/src/wire.rs`,
`server/transparent-shard-server/`, `pir/transparent-wallet/`. Two maps, two
manifest chains, two tails. The wallet's sync gains a page-generation lookup
between the directory decode and the page fetch.

**Schema** — `SCHEMA` moves to v5, and the set publishes to its own directory.
`ShardSet::open` already refuses a set it cannot read, and the workflow already
takes `shard_dir` and `data_dir` inputs.

## Verification

1. `make check`. Geometry invariants are const assertions, so a packing change
   that breaks one fails the build.
2. **Exact replay equality is the gate.** Every workload must reconstruct its
   UTXO set, spend set and per-transaction history identically to an independent
   traversal of the journal — not merely the same balance. This is the check
   that a packed row cannot silently lose an event, and it already exists.
3. Add a test that builds a generation at high packing density and asserts every
   script's history reassembles, since near-full rows are where a packer's
   off-by-one lives.
4. `shard-census` over the journal before publishing anything, confirming
   generations stay single-segment and reporting the new fill.
5. `measure` against the published set. The common-case workloads must not
   regress; the whale is expected to stay where it is.
6. Archive census and measurement output alongside the existing evidence.

## Risks

**The packer is the risk, not the lanes.** Lanes are bookkeeping: two
sequences where there was one. The packer changes the table where a silent loss produces a
wrong balance that looks normal. Builder-assigned placement removes the overflow
case, and exact-equality replay is the check, but this is where the tests should
be strongest.

**Sealing gets harder to price.** Today `page_rows_for` is a closed form the
sealer evaluates per script incrementally. Packed occupancy is not, and a naive
implementation would re-pack per candidate boundary and make the census
quadratic. The accounting needs to stay incremental and slightly conservative:
over-estimating rows seals early and wastes a little, under-estimating overflows
a segment and multiplies query cost for every user of that generation.

**Lane skew.** If the two lanes drift far apart in width, a directory generation
could span more page generations than intended. The subset constraint prevents
it structurally; it should also be asserted at publication rather than assumed.

## Sequencing

Both need a schema change and a republish, and a genesis republish is on the
order of fourteen hours at the measured rate. The composition argument says
either alone captures well under half the benefit, so they should land together,
once, and **before the genesis publish** rather than after.

The genesis journal is still ingesting. When it lands, the census should report
what each lane would seal at independently alongside the coupled result: if the
binding quantity does flip between chain eras, that comparison shows it directly
and settles whether lanes are worth their complexity on evidence rather than on
the argument above.
