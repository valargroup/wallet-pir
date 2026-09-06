# Transparent PIR page geometry: decision summary

Date: 2026-09-06. Status: bounded-journal geometry sweep complete; no packed set
has been published.

## Executive summary

The preferred direction is a smaller global page-table segment, with additional
segments reserved for indivisible outliers. Short histories should be packed so
that ordinary generations fit the smaller segment. This is a geometry change,
not a page-lane change: the directory and pages retain one generation boundary,
revision, and privacy domain.

Packing alone does not reduce pinned storage. A generation pins every row in its
declared geometry whether the row is occupied or empty. Packing is useful because
it may permit the global page-table row count to be reduced without making
ordinary generations use multiple segments.

The bounded-journal result favors 5,120 rows. The final row count is not yet
known because the genesis-to-tip journal and end-to-end PIR costs remain to be
measured.

## What has already been established

The implemented v4 geometry changes reduced plaintext storage over the measured
Ironwood sample from 1,695.5 MB to 220.2 MB. Live-byte utilisation rose from
6.04% to 44.70%, page-row use from 43.1% to 82.6%, and page-slot fill from 8.7%
to 45.2%.

The packed-row projection over heights 3,428,143 through 3,473,474 found:

- 40,579 unpacked fragments become 20,937 projected packed rows, a 1.94-fold
  reduction in row demand.
- The largest projected generation at the existing boundaries needs 4,856 rows,
  compared with the current 8,192-row page segment.
- Short histories account for 6,244 projected rows.
- Long histories account for 14,693 projected rows, about 70% of demand. This
  version deliberately leaves those rows dedicated and therefore does not
  improve their query count.
- Pinned storage remains 220.2 MB at unchanged geometry. The reduction in live
  encoding bytes is not a reduction in allocated table bytes.

These are projections from per-script counts, not emitted v5 bytes. They cover a
45,332-block sample, not genesis through tip.

## Bounded-journal geometry sweep

The production census workflow replayed the actual 869,283-event journal while
sealing on exact projected packed demand. Every tested generation remained
single-segment. At a 27,000-script target, the results were:

- 8,192 page rows: 5 generations and 183.5 MB pinned.
- 7,168 page rows: 5 generations and 165.2 MB pinned.
- 6,144 page rows: 5 generations and 146.8 MB pinned.
- 5,120 page rows: 5 generations and 128.5 MB pinned.
- 4,096 page rows: 6 generations and 132.1 MB pinned.
- 3,072 page rows: 7 generations and 128.5 MB pinned.
- 2,048 page rows: 11 generations and 161.5 MB pinned.

The result is not monotone: below 5,120 rows, extra directory tables offset the
smaller page tables. The 3,072-row geometry ties 5,120 rows on storage but needs
two additional generations. The 5,120-row geometry is therefore the
storage-and-generation-count winner in this sample. It reduces pinned bytes by
30.0% relative to packed sealing at 8,192 rows and by 41.6% relative to the
220.2 MB published-v4 baseline.

At 5,120 rows, the five generations require 21,011 packed rows rather than
40,109 unpacked fragments, a 1.91-fold reduction. Projected page-row utilisation
is 82.1%, projected live-byte utilisation is 74.35%, and no generation needs a
second page segment.

The generation-match pass found 93,281 distinct supported scripts:

- At 8,192 rows, a script matches 1.241 generations on average, implying 2.482
  directory queries. The distribution is p50 1, p90 2, p95 3, p99 5, maximum 5.
- At 5,120 rows, a script matches 1.254 generations on average, implying 2.508
  directory queries. The distribution is unchanged at p50 1, p90 2, p95 3,
  p99 5, maximum 5.
- At 3,072 rows, a script matches 1.352 generations on average, implying 2.703
  directory queries. Its p99 and maximum rise to 7.

Thus 5,120 rows adds about 1.0% average directory queries versus the 8,192-row
packed boundary, while 3,072 rows adds about 8.9% and has a worse tail. These
figures exclude filter false positives.

This sweep establishes projected table shape from real journal contents; it is
not a v5 publication measurement. Codec bytes, actual directory placement, PIR
parameter discontinuities, setup bytes, response bytes, and latency still need
measurement before 5,120 can become the default.

## Proposed operating model

Use one fixed page geometry globally. Seal ordinary generations before their
packed demand exceeds one segment. If one block cannot fit an empty segment,
publish that block in its own generation using as many identical segments as it
requires.

This preserves automatic availability for adversarial or unusually dense blocks
without sizing every ordinary generation for the worst case. It also keeps the
row geometry independent of the selected script.

Additional segments must remain exceptional. A wallet cannot reveal which
segment contains its selected row, so page retrieval is evaluated across every
page segment in the generation. An `n`-segment generation therefore multiplies
page response bytes, setup material, and server work for every lookup touching
that generation. Routine stacking can save pinned bytes while making wallet and
server costs worse.

Independent page-generation lanes remain deferred. Selecting a separate page
container after a private directory lookup can reveal a partition of directory
scripts unless the choice is hidden by an additional retrieval mechanism.

## Directory-query consequence

For an active script, let `g` be the number of generations whose filters match
that script. Ignoring filter false positives, full restoration requires:

`directory queries = 2 * g`

The factor of two comes from the directory's two candidate rows. In a
multi-segment directory generation, each row selection is evaluated over every
directory segment.

The current census reports script entries per generation but not the
distribution of `g` across globally distinct scripts. Consequently, no honest
average-generation-match figure is available yet. The useful statistics are the
mean, p50, p90, p95, p99, and maximum, because frequently reused scripts can
make the mean unrepresentative.

## Efficient generation-match measurement

The metric can be collected during the same journal pass as the geometry sweep.
Whenever a generation seals, emit one fixed-width record for every supported
script in that generation:

`(policy identifier, script length, padded script bytes)`

Externally sort those records by policy and exact script, then group equal keys.
Each group size is that script's generation-match count. This keeps memory
bounded and uses disk proportional to script-generation occurrences rather than
total journal events. Exact script bytes should be retained; a digest-only
aggregation would make the result probabilistic.

For a preliminary sweep that needs only the mean, a cardinality sketch per
policy is cheaper:

`estimated mean g = script-generation entries / estimated distinct scripts`

The exact external aggregation should be used for the final geometry decision
because it also provides the tail distribution that determines restoration
cost.

This is a chain-script distribution, not a real-wallet population distribution.
Wallets can own many unused scripts, while exchanges and reused addresses can
dominate the chain-level tail.

## Geometry sweep

For each candidate page-row count and script sealing target, seal on exact packed
row demand `R`, not on the v4 fragment count. The sweep should report:

- generation count and height-span distribution;
- directory and page segments per generation;
- ordinary generations requiring more than one page segment;
- total pinned directory and page bytes;
- packed row demand and byte fill;
- which limit sealed each generation;
- generation matches per exact script and the implied two-directory-query
  distribution;
- filter and setup bytes over full restoration;
- physical page and directory request counts; and
- oversized-block generations separately from ordinary generations.

The observed 4,856-row maximum makes 5,120 and 6,144 rows reasonable candidates
for the measured sample, but not defaults. Packed-demand sealing changes the
boundaries, and early-chain traffic may change which table binds. Smaller and
larger candidates should be included so the result does not merely confirm those
two values.

## Acceptance rule

Adopt the smallest global page segment for which all of the following hold over
coverage-matched data:

1. Total pinned plaintext bytes decrease from v4.
2. Ordinary generations remain single-page-segment; only indivisible outliers
   stack.
3. Actual directory placement does not introduce unexpected segments.
4. Common-case physical PIR request count and transferred bytes do not increase.
5. The generation-match distribution and resulting directory-query cost remain
   acceptable for restoration.
6. Exact replay, revision binding, deterministic construction, interruption,
   tail growth, and reorg gates pass.

A geometry that wins only by making ordinary generations stack should be
rejected. If no candidate produces a meaningful end-to-end win, retain v4 and
reconsider packed pages rather than weakening the privacy boundary.

## Current implementation boundary

The current work adds a measurement-only packed sealing basis, a
`--page-rows-per-segment` scoring override, segment reporting, and workflow
inputs for remote census sweeps. It also reports exact generation matches and
implied directory queries per supported script for the bounded study journal.
This is enough to evaluate candidate geometry without first building the codec.

It is not enough to publish v5. The packed codec, deterministic builder, wallet
decoder, manifest schema change, strict malformed-input tests, and emitted-row
equality checks remain implementation work.

## Next decision

Run the full-journal packed geometry sweep and exact generation-match
aggregation. Archive the command inputs, journal coverage, output, elapsed time,
and peak memory. Use that evidence to either select the new global page-row
count or stop before building the v5 codec.
