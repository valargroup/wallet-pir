# Plan: table utilisation through packed pages

Date: 2026-09-06. Status: amended implementation plan; new savings are unmeasured.

## Decision

Pack short histories into shared page rows while keeping one public generation
boundary, manifest chain, and provisional tail for the directory and pages.
Do not introduce independently routed page generations.

The priority order is correctness, no additional selection leakage, common-case
query cost, then storage utilisation. Some empty space is an acceptable cost of
preserving the private lookup boundary. This work does not promise to make both
tables full in every chain era.

## Context and scope

The existing measurement covers six generations from Ironwood activation to
height 3,473,474, archived under
[shard-utilisation](transparent-pir-evaluation/shard-utilisation/):

| Metric | Existing measurement |
|---|---:|
| Directory slots filled | 69.8% |
| Page rows used | 82.6% |
| Page slots filled | 45.2% |
| Live bytes of pinned bytes | 44.70% |
| Plaintext tables | 220.2 MB |

Earlier parameter changes narrowed page rows from 17,920 to 3,584 bytes and
raised the page-row count per segment. Remaining waste has two sources: one
script owns each used page row, and sealing either table also closes the other.
Packing addresses the first. Shared boundaries deliberately retain the second.

The measured history distribution is p50 2 events, p90 5, p95 8. The two newest
events remain inline in the directory. A three-event history therefore needs a
page row for just one older event. Sharing that row is the common-case storage
opportunity. Early-chain script/event distributions differ from recent-chain
ones; a full-journal census must establish how that changes the binding table.

The initial implementation keeps the existing geometry: 3,584-byte rows,
2,048 directory rows and 8,192 page rows per segment, two inline events, and 36
events per history fragment. Geometry tuning is a separate, measured decision
within this plan, not an assumed consequence of packing.

This is not a query-key reuse change, a large-history query-count optimisation,
or a stronger privacy protocol. Query budgets and resumable work remain required
for large or externally spammed histories. Exhausting a budget leaves coverage
incomplete; it must not silently truncate a history.

## Privacy contract

Inherit the trust and leakage model in
[the correctness and privacy contract](transparent_pir_design.md#correctness-and-privacy-contract):
the initial profile trusts the indexer for completeness and correctness; PIR
hides the selected row, not the truth of its contents. Current observable
behavior includes selected chain ranges, whether page retrieval occurs, query
volume, and timing. Public-chain information can make that behavior identifying.
This change does not remove those disclosures or establish security against an
actively malicious service.

The additional invariant for this work is:

> For a fixed public generation revision and request count, public table
> destinations and request/response sizes do not depend on the selected script
> or on where its history was packed.

Consequently:

- A directory entry locates pages only within the same generation revision.
  There is no script-dependent page-generation or size-class endpoint.
- Size classes are a builder detail. All classes occupy one logical page table
  with the same PIR geometry; their row indices remain private.
- If a table has multiple segments, the wallet queries every segment as today.
  It must not expose the segment obtained from a locator.
- Do not add script-dependent request deduplication when two histories share a
  row, or skip a scheduled fetch because an earlier packed response happened to
  contain another requested history. Such optimisations change the observable
  transcript and need a separate privacy review.
- Filters retain the existing download and local-matching policy. This work
  adds no public lookup of a script, locator, or packing class.

Independent page lanes are deferred because choosing a page container after a
private directory lookup can reveal a partition of the scripts in that directory
generation. Hiding that choice would require a separately costed retrieval
mechanism. Coarser public ranges, padded query counts, dummy page requests, and
scheduled retrieval are also separate privacy proposals; none is implied here.

## Record and identity contract

`SCHEMA` moves from v4 to v5. The directory record stays unchanged, including
`total_events`, the inline events, `first_page`, and `page_count`. Counts describe
one exact script's history within this generation. No page-generation field is
added, so the directory entry width and slot count remain unchanged.

A page row is exactly 3,584 bytes. Its encoding is:

- A four-byte unsigned little-endian entry count.
- That many consecutive variable-length entries, each a 64-byte header followed
  immediately by `event_count` events in the existing canonical 96-byte event
  encoding.
- Zero bytes through the end of the row. An empty row is entirely zero.

Each entry header has these fields, in order:

| Field | Bytes | Encoding |
|---|---:|---|
| Script length | 2 | Unsigned little-endian, at most 40 |
| Exact script | 40 | Script bytes followed by zero padding |
| Fragment ordinal | 4 | Unsigned little-endian, zero-based |
| Fragment count | 4 | Unsigned little-endian, nonzero |
| Event count | 4 | Unsigned little-endian, 1 through 36 |
| Minimum event height | 4 | Unsigned little-endian |
| Maximum event height | 4 | Unsigned little-endian |
| Reserved | 2 | Zero |

Entry size is `64 + 96 * event_count`; the count and bounded event count define
parsing unambiguously without another length field. A full 36-event fragment
uses 3,520 bytes, fitting after the four-byte row header. Put these constants and
offsets in one canonical module and pin them with assertions and frozen vectors.
A future event-width or script-limit change requires revalidating this schema.

Entry identity is `(accepted generation revision, exact script, ordinal)`.
Generation identity is supplied by the bound table/manifest context, not repeated
inside every entry. The client validates the entire row, rejects duplicate script
entries within it, and selects exactly one entry matching the expected script,
ordinal, and fragment count across all returned segments. It also checks:

- Bounded lengths/counts and checked locator arithmetic; every requested row is
  inside the declared logical page table.
- Canonical event encodings, ordering, and height bounds that agree with the
  events and lie inside the generation range.
- Zero reserved bytes, script padding, and unused row bytes.
- The complete retrieved history, including inline events, has exactly the
  directory's promised event count and satisfies existing replay validation.

Missing, duplicate, malformed, or mismatched fragments fail the affected work as
incomplete. Builder-assigned placement avoids hash-bucket overflow for pages;
it does not make locators infallible or prove index completeness.

## Deterministic packing and query-count contract

For each supported script, sort its events by the existing canonical event order.
Keep the newest two inline, or all events if fewer. Let `p` be the number of
remaining older events. The builder then uses these rules:

1. `p = 0`: no page entry or page query.
2. `1 <= p <= 36`: place the whole older history in size class `p`. It is one
   entry with ordinal zero and fragment count one. Never split it to fill a gap.
3. `p > 36`: split into ascending consecutive chunks of 36 events, with a final
   shorter chunk if needed. Allocate a dedicated contiguous run of rows, one
   entry per row. Do not share the final row in this version.

Emit short-history classes in increasing `p`. Within each class, sort by exact
script bytes and fill rows with whole entries. Then emit long histories in exact
script order, preserving each contiguous run. Each script occurs once in this
generation's directory and belongs to exactly one of these categories.

For a short-history class, the number of entries per row is exactly:

`entries_per_row(p) = floor((3584 - 4) / (64 + 96 * p))`.

This is size-class packing, not general bin packing. It deliberately forgoes
mixing different lengths or reusing long-history tails so placement and seal
accounting stay exact and inexpensive. The census must measure the resulting
waste, including partially filled final rows in each class. Do not carry forward
the earlier generic-packing estimate of 85% utilisation.

For a fixed generation range and script, page-query count remains exactly
`ceil(p / 36)`, as in v4. Multiple scripts can now name the same `first_page`,
but a wallet retrieving each history independently still makes the same number
of logical row requests. Long histories keep contiguous extents. Physical query
count also depends on segment count, which must be measured separately.

## Exact incremental sealing

Keep shared generation boundaries. The sealer tracks distinct scripts and page
rows, closes both tables when either target is reached, and closes before a
block that would breach capacity. Preserve the existing exception for a block
that exceeds capacity alone: publish it using additional segments rather than
stop publication or drop events.

Maintain each supported script's event count, short-history class counts `N[p]`,
and the sum `L` of dedicated rows for long histories. Exact packed row demand is:

`R = sum(p = 1..36, ceil(N[p] / entries_per_row(p))) + L`.

The contribution of a long history to `L` is `ceil(p / 36)`. Scripts outside
private-table coverage contribute no page entries; keep the existing public
filter coverage and excluded-script reporting rules.

To project a block, aggregate its events by script, remove each touched script's
old category contribution, and add its projected contribution. In particular,
handle transitions across the inline threshold, between short classes, and from
36 to 37 paged events. Evaluate on temporary deltas before deciding whether the
block belongs in this generation. Commit the deltas only when absorbing it;
reset all class state when sealing. No candidate-range sorting or repacking is
needed per block. Page accounting costs O(touched scripts + 36) per block, in
addition to existing event processing and directory accounting.

At build time, assert actual emitted row count equals `R`. Directory placement
still uses the real two-choice placer; distinct-script capacity alone does not
guarantee it fits one segment. Report placement-induced extra segments separately
from the oversized-block exception. Do not assert a 97% directory fill target.

## Publication and wallet behavior

Keep one map, one manifest chain, and one provisional tail. Build the filter,
directory, and page tables from the same event range and publish them as one
immutable revision. The manifest binds both tables, geometry, and chain anchors.
Make the complete revision available before exposing it through the map; a failed
publication must leave the prior complete revision usable.

A provisional rebuild may reorder rows, including when a history changes packing
class. It therefore produces a new revision of both tables together. A wallet
must finish against its pinned available revision or discard/restart affected
work against the replacement; it must never combine old directory locators with
new pages. Cache/setup identity and resumed work must distinguish revisions.
Retain the existing reorg rollback and provisional coverage rules. Sealed content
is immutable under its published identity, not immune to chain reorganization.

The wallet decodes the new packed row shape and preserves existing exact-key,
fragment, total-event, and replay checks. Shared rows do not change the meaning
of `first_page + ordinal`. The v5 set is published separately; clients and servers
must reject unsupported schemas rather than decode v5 bytes as v4.

## Implementation work

- `pir/transparent-shard/src/records.rs`: define the v5 row codec, canonical
  offsets, strict validation, and frozen encoding vectors.
- `pir/transparent-shard/src/build.rs`: implement deterministic class packing,
  dedicated long-history runs, and emitted-row/accounting equality checks.
- `pir/transparent-shard/src/layout.rs` and `seal.rs`: centralize fragment and
  class capacities; implement the incremental class counters. Distinguish
  per-history query count from aggregate packed row demand in APIs and names.
- `pir/transparent-shard/src/manifest.rs`: bump schema and accurately describe
  packed geometry. Retain coupled table digests and revision semantics.
- `pir/transparent-wallet/` and `server/transparent-shard-server/`: support the
  codec, verify revision binding, and preserve private segment selection and
  fixed request geometry. No second map or lane-lookup API is required.
- Census and measurement tools: report event payload, encoding overhead, used
  rows, pinned bytes, class-tail waste, segment counts, and wallet costs. Update
  descriptions that currently assume one script per page row.

## Measurement and acceptance gates

The earlier 60%/77% live-byte and 150/117 MB storage projections are withdrawn:
they did not model this packer, shared-boundary sealing, and actual directory
placement. The existing measurements above are a baseline, not a v5 forecast.

Compare three configurations over identical journal coverage: v4 baseline, v5
packing with unchanged geometry and seal policy, and any proposed v5 geometry
retuning. Also compare v4/v5 builds on identical fixed ranges to isolate packing
from changed generation boundaries. Archive inputs, schema, geometry, policies,
and output alongside the existing evidence.

Acceptance requires:

1. `make check`, codec vectors, and malformed-input tests pass. Cover truncation,
   oversized counts, duplicate entries, wrong ordinals/counts, nonzero padding,
   out-of-range locators, and arithmetic overflow without panics.
2. Exact replay equality against an independent journal traversal for UTXOs,
   spend sets, and per-transaction histories. Add generated histories covering
   every short class, full/partial rows, empty generations, repeated scripts in
   adjacent generations, and long-history boundaries.
3. Incremental projection equals the actual builder row count across randomized
   block sequences, threshold crossings, pre-block seals, and oversized blocks.
   Equivalent input ordering produces identical published bytes.
4. Fixed-range per-script page-query counts equal v4. Paired retrieval traces
   for different scripts with equal query counts expose identical destinations,
   message sizes, and segment schedules, excluding encrypted content and timing.
   This checks the routing invariant, not a cryptographic privacy proof.
5. Tail growth, sealing, stale/mixed revisions, interrupted publication, resume,
   and reorg tests never accept mixed tables or advance incomplete coverage.
6. Full-journal census reports storage and actual segment counts by chain era,
   including provisional tails. Total pinned plaintext bytes must decrease
   versus the coverage-matched baseline. Ordinary generations gaining segments
   require investigation and correction before accepting the chosen geometry;
   oversized-block cases remain explicitly supported and reported.
7. End-to-end measurement for every existing common-case workload shows no
   increase in physical PIR request count or total transferred bytes versus the
   coverage-matched baseline. Report filter/setup/query/response bytes separately.
   Report long-history costs and all changes caused by new boundaries as well;
   a fixed-range page-count guarantee is not an end-to-end cost guarantee.
8. Repeat baseline and candidate latency/build measurements on the same hardware
   and report distributions and peak memory. Do not claim a performance win
   from noisy single runs; an observed regression must be explained and resolved
   or accepted through an explicit amendment before rollout.

Keep the existing geometry for the first comparison. Select a different global
geometry only if the measured comparison satisfies these gates. Do not choose
script-dependent geometries or routing to rescue utilisation. If savings are
small, retain the privacy contract and reconsider the optimisation rather than
quietly introduce independent lanes.

## Sequencing

Implement and validate the codec, packer, and exact accounting together, then run
the unchanged-geometry census before considering retuning. Complete wallet and
publication validation and the measurement gates before publishing a v5 set.

A genesis republish is expensive, so completing this before it is desirable, but
the ingest/publish schedule is not a reason to waive the gates. Independent lanes, stronger traffic privacy, and query-key reuse each require
their own design and evidence.
