# Enhance PIR architecture update: immutable shards, composed routing, direct worker serving

Status: design proposal dated September 24, 2026. It does not describe deployed
behavior and does not amend [architecture](architecture.md) until accepted.
Every capacity figure below is a planning input that must be qualified on the
intended 8 GiB Linux hosts before it is treated as a limit.

## Motivation and measured baseline

The current architecture spends most of its complexity on shard lifecycle
(loans, return, restoration, lender counting, reorg undo across two shards)
while the measured limit is the data plane. The production admission
measurement on September 23, 2026 reached about 33 correct queries per second on
two replicas holding one shard, with a fleet-global cap of four active queries
and every query body proxied, decoded and packed on the coordinator. The
uncommitted batching path measured a 2.0x scan-efficiency gain at a batch of
eight on a local M4 Max (local evidence is not yet published on `main`),
but its 20 ms coalescing window sits on the coordinator and its gain on the
production hosts is unmeasured.

This proposal keeps the record format, the p16 q48 profile, the public setup
domain, the 24-shard wallet ceiling, the memory admission ledger and the
persisted operation phases. It changes what a shard is, how the population floor
is produced, where queries are served, and how sessions expire.

## Objects

The update separates five things the current document folds together.

| Object | Meaning | Identity |
|---|---|---|
| Storage shard | Fixed global row range of `max_shard_rows`; immutable once sealed | Shard ID = global row start / `max_shard_rows` |
| Query domain | Ordered mapping of stored rows into PIR coordinates under one setup seed, with logical rows and units | Domain ID, setup seed, unit identities, logical rows |
| Routing view | The manifest's assignment of every populated global row to exactly one query domain and local row | Routing revision plus chain anchor |
| Session | A query domain's content, geometry, setup and packing parameters as the wallet must use them | Content digest over the domain's unit identities and parameters |
| Placement | Which replicas hold which query domains and are ready | Placement revision |

The invariant is unique canonical routing, not unique physical presence or
unique queryability. A storage shard may physically hold rows the routing view
sends elsewhere.

## Decisions

### D1. Public shard selection stays; no whole-database fan-out

**Decision.** A wallet publicly selects one query domain and hides its row
within it. Fan-out across all domains with coordinator-side summation is not
the design, not even as an optional mode.

**Rationale.** Single-server PIR is a linear scan, so whole-history privacy
costs one full scan of the chain per query, forever. At the 24-shard wallet
ceiling that is 24 times the per-query work of one shard. Batching helps both
designs equally and does not change that ratio. Twenty-four independent q48
requests upload 6.78 MB at the measured 282,652 bytes per 32K query; only about
4.7 MB of that is query vectors, and a shared-key framing would need a protocol
that does not exist. Non-uniform shards are not the obstacle, since unit
summation already handles unequal unit sizes. Packing in the coordinator was a
consequence of fan-out, not a privacy property: the query is encrypted under
the wallet's key end to end, so a worker that packs learns nothing a
coordinator that packs would not.

**Cost.** The server learns which domain a wallet's notes fall in, at a
granularity of about a million actions. The timing prior on fresh notes is
identical under every design.

### D2. Fixed storage shards, immutable once sealed, no loans

**Decision.** A storage shard is the fixed range `[k * M, (k + 1) * M)` rows.
Once its last row is populated at confirmation depth it is sealed and its
runtime is never modified. Records never move between shards. The only
lifecycle event is the frontier reaching `M` rows and a successor opening.

**Rationale.** The loan exists solely to give a successor a population floor.
It costs two atomic two-shard publications, two rebuilds of the lender's last
unit, a restoration reservation, lender counting, the SETTLING role, and reorg
undo on both sides. D3 produces the same floor without any of it. Immutable
sealed shards are also what make D7's long-lived sessions sound.

**Cost.** None in density. The first shard of the database has no predecessor
and keeps today's bootstrap exception.

### D3. Population floor from a composed tail domain with manifest-owned routing

**Decision.** When successor B opens after A seals, the manifest publishes a
tail domain and a routing table. Using `n` for B's fully populated rows and the
defaults `M = 32,768`, `m = 4,096`:

| State | A's first `M - m` rows | A's last `m` rows | B's rows |
|---|---|---|---|
| B open, `n < m` | Domain A | Tail domain | Tail domain |
| `n >= m` | Domain A | Domain A | Domain B |

Wallets make no choice. The routing table assigns every populated global row to
exactly one domain and local row, and the wallet validates coverage, overlap,
bounds and revision before use.

**Tail domain layout.** The tail domain is B's own domain with one extra unit.
It uses B's setup seed and B's local coordinates:

```text
B's rows          at tail local rows [0, m)         B's own units, offsets unchanged
A's last m rows   at tail local rows [m, 2m)        one extra unit, prepared under B's seed
logical rows      2m during the compose window
```

Because `n < m` throughout the window, B's rows never reach local row `m`.
When `n >= m` the extra unit is dropped, the routing table sends A's suffix back
to domain A, and B's units keep the offsets they already had.

**Rationale.** The anonymity sets are identical to the loan design at every
stage: a tail query is any of `m + n` rows, an A query is any of `M - m` rows.
A's runtime is never touched, and ending the window is a routing change plus
dropping one unit. The layout was chosen so that B's units keep their
coordinates: the runtime derives each unit's setup slice from a full-size setup
generated from the shard's seed at the unit's local offset, and the code
comments that this keeps unit slices stable through logical growth
(`enhance/services/enhance-pir-server/src/runtime.rs`, `Engine::prepare`).
That is the property the composition relies on.

**Cost.** One extra preprocessed unit of `m` rows, 96 MiB at the defaults,
prepared once when B opens and dropped when the window ends. The coordinator's
packing state for the domain is rebuilt when its unit set changes, which is
already the per-publication cost for any growing domain.

**Status.** The coordinate-stable reuse rule is consistent with the setup
derivation in the code but has not been demonstrated with the PIR
construction. Until a test shows that B's unit artifacts and hints are byte-
identical inside and outside the tail domain, the fallback is bounded
preparation of plain B, including its packing state, before the routing switch.
That fallback is still far simpler than loans, but its CPU and overlap memory
must be budgeted. Do not claim "routing only" before the test exists.

**Accepted deviation.** A physically still holds its suffix, so a client that
ignores the routing table can query those rows through domain A, and a client
holding a retained tail session can query them through the old tail domain
after the switch. The server cannot detect either. The population claim
therefore applies to compliant clients using the current routing view, with
transition and stale-view limitations stated. The floor remains a population
policy, not a proof, as the current document already states for 4K.

### D4. Shard-selection privacy beyond the floor is a defined client policy

**Decision.** Cover traffic is optional, but when enabled it follows one
baseline policy: a persistent cover set chosen for a stated interval, fresh
query randomness per request, and identical timing, ordering and retry
behavior for every domain in the set. The guarantee is indistinguishability of
the target within the observable cover set under that policy, against an
observer of the whole service.

**Rationale.** Independently resampled decoys leak the target by intersection
across repeated requests, and sending the real request first, retrying only it,
or fetching several rows only from it leaks structure. A persistent set with
uniform behavior removes those channels. "All domains since birthday" is easier
to state precisely and reveals only the birthday range, which compact scanning
already reveals, at higher cost.

**Cost.** Cover queries multiply server work by the set size. This is the
fan-out trade chosen per wallet instead of imposed on the fleet.

### D5. Batching is measured and moved to the worker

**Decision.** Keep the batched evaluation path. Move its queue and coalescing
window from the coordinator to each worker, so a worker batches whatever
compatible queries it holds rather than waiting on a global window.

**Rationale.** Batching already exists in the working tree: the coordinator
coalesces up to eight queries with the same generation, shard and epoch for up
to 20 ms, and the worker streams each unit once for all of them. The local
measurement shows about 2.0x less scan time per query at a batch of eight, and
that the 20 ms window adds latency without gain when arrivals are sparse. The
gain is empirical: multiplication work grows with batch size while database
traffic is shared, so the saturation point on the production hosts is unknown.

**Cost.** Worker-local scheduling must respect the same per-request lifetime
accounting as D6.

### D6. Workers pack; the coordinator leaves the query path; admission is per replica

**Decision.** The public origin routes each query to a ready replica for its
domain using a routing table the coordinator publishes. The replica receives
the body, evaluates, packs with the query's own upload keys, and answers. The
coordinator ingests, publishes manifests, routing and placement, and never
buffers a query body.

**Rationale.** Today the coordinator buffers every 282 KB body, decodes it,
forwards to one replica at a time, packs, and gates the fleet with a global
slot count. Its sampled cgroup peak was about 8 GB. It is a single point of
failure and a fixed throughput ceiling that does not grow with the fleet.

**Cost.** Packing moves its CPU and memory onto the 8 GiB workers and must be
measured against the same worker budget. Worker admission must cover the complete
request lifetime: bounded body reception, decoding, queued batches,
evaluation, packing, and response buffering. An execution slot alone does not
bound queued bodies.

### D7. Session validity is separate from routing freshness and placement

**Decision.** Three independent identities, each versioned in the manifest:

- Session: the query domain's content, geometry, setup and packing parameters.
- Routing view: the manifest's assignment of rows to domains, with chain anchor.
- Placement: which replicas hold which domains.

A sealed domain's session is valid until its content changes or a recovery
epoch marks it noncanonical. Wallets refresh the routing view on a defined
cadence and reuse unchanged session material. A replica move never
invalidates a session.

**Rationale.** A sealed shard never changes, so binding its queries to a
generation that expires after five publications forces wallets to refresh
about every six minutes for no reason. But a cached sealed session cannot tell
a wallet whether its position currently routes through A or through the tail,
so routing freshness must be a separate, cheaper refresh.

**Cost.** Every admitted query still needs a lifetime pin, including queries
against sealed domains during eviction or a move. Only mutable domains need
repeated content revisions; sealed domains still need safe resource lifetimes.

### D8. Appends follow the tip; sealing and routing transitions wait for confirmation depth

**Decision.** The frontier domain appends rows as blocks arrive. Sealing a
storage shard, opening a successor, and ending a compose window happen only
for ranges fully covered by the confirmation policy. Every publication is
bound to its chain anchor.

**Deep-reorg contract.** Depth reduces the likelihood of a reorg crossing a
sealed boundary; it does not make one impossible, and a single block can add
enough rows to cross several units. If a reorg invalidates a sealed range:

1. Stop advancing publication.
2. Mark the affected domains and sessions noncanonical in a new recovery epoch.
3. Publish replacement content under new identities.
4. Never overwrite an existing sealed artifact or reinterpret its session.

**Rationale.** Shallow reorgs then only touch unconfirmed frontier revisions,
which are already rebuilt per publication. Boundary events are never urgent,
so delaying them costs wallets nothing. New records are still available at tip
latency; the cost of depth is paid only by boundary events.

**Cost.** Unconfirmed frontier revisions need explicit handling and retention.
Immutable artifacts are guaranteed; their canonical status is not.

**Depth and capacity evidence (2026-09-24).** No confirmation depth is justified
yet. The current capacity controller's one-row-per-second fallback, 21,600-second
readiness window, and 4,096-row burst allowance are [configured defaults](../services/enhance-pir-server/src/capacity.rs),
not Ironwood measurements. The [isolated expansion trace](../evidence/architecture-v4-live-threshold-2026-09-23/README.md)
requested a pair at 25,696 remaining rows, registered it 7 minutes 15 seconds
later, and first published the next shard 16 minutes 58 seconds after the
request. Its row growth was synthetic, its receipts remained unqualified, and it
did not preload the successor before that publication. A separate [sealed
preparation attempt](../evidence/architecture-v4-sealed-deadline-2026-09-23/README.md)
observed about 186 seconds of worker preparation and missed the former
180-second request deadline; it is a failure sample, not a worst-case bound.
Neither run establishes the lead time for the proposed confirmation-gated
successor path.

The current [RPC ingestion loop](../services/enhance-pir-server/src/bin/server.rs)
compares the journal's last block hash with the node's canonical hash and rewinds
until they match. It does not persist a reorg-depth observation. The current
canonical [journal manifest](../services/enhance-pir-server/src/store.rs) records
per-block `action_count`, but a rewind removes orphaned block entries. Historical
canonical action counts can measure observed block bursts; that manifest alone
cannot establish reorg history. Do not interpret an absence of recorded reorgs
as evidence for a depth.

### D9. Keep progressive frontier units with an 8K cap

**Decision.** Keep the current allocation progression: the growing unit starts
at 2,048 allocated rows, grows to 4,096 and then 8,192, and rolls over only
when the 8K unit is full. Do not cap every completed unit at 2K or allocate
8K from the first row.

**Rationale.** A matched full-size schema-11 / p16 q48 local comparison found
that six retained 2K frontier versions saved 0.873 GiB of process peak RSS
relative to six 8K versions. But the current hint assembly reads and sums all
units on every frontier change: 16 units at a 2K cap took a median 7.81 seconds,
versus 1.94 seconds for four 8K units. Savings in frontier preprocessing and
persistence did not compensate; the measured phases summed to about 9.37
seconds per 2K revision versus 4.34 seconds per 8K revision. Median encrypted
evaluation rose from 7.25 to 11.11 ms per sequential query. Packing rebuild
time was similar. The [D9 evidence in closed PR #100](https://github.com/valargroup/wallet-pir/pull/100)
retains the benchmark, commands, raw timings, and provenance.

**Qualification limit.** The comparison ran on a 128 GiB M4 Max, without an
8 GiB Linux cgroup or concurrent traffic. The observed memory saving is about
one 768 MiB sealed database image, not proof that another sealed domain passes
worker admission. Keep the worst-case six retained 8K revisions, about 1.1 GiB
of database bytes, in the ledger. Revisit a 2K cap only after incremental
hint assembly and a matched 8 GiB Linux load and memory run demonstrate a net
gain without a material publication or query latency regression. The separate
96 MiB tail unit remains a ledger component, not a complete admission budget.

### D10. Pool placement with a replication factor instead of rigid replica pairs

**Decision.** Each query domain is placed on `r` workers drawn from a pool,
with `r = 2` by default and a higher `r` allowed for the frontier. Groups are
replaced by per-domain placements; the reservation ledger and persisted
operation phases carry over.

**Rationale.** Rigid pairs force growth in units of two machines and give the
hot frontier the same replica count as an idle sealed shard. A pool improves
utilization, growth granularity and hotspot replication.

**Cost.** Two copies still give roughly 50 percent raw storage efficiency.
Pool placement does not remove duplication. This is the lowest-priority item.

### D11. Protocol semantics are versioned; packing-key reuse is deferred

**Decision.** Composed routing, the routing table, and content-bound sessions
are a new protocol revision even though record bytes, p16 q48 and the encrypted
query payload are unchanged. Legacy clients reject the new manifest. Session-
scoped packing keys are not part of this update; they need a separate protocol
review covering linkability and key-reuse safety, and transport-level
linkability alone is not sufficient justification.

## Memory ledger components per replica

These are the components the admission ledger must count. None is a complete
budget, and both replicas of a domain must fit independently.

```text
sealed domains           768 MiB each at 32K logical rows
frontier domain          allocated units + retained revisions of the growing unit
                         (up to 6 x 192 MiB at the retained 8K cap)
tail unit                96 MiB while a compose window is open
packing state            per domain, rebuilt when its unit set changes (D6 moves it here)
per-request lifetime     body, decoded keys, batch slot, intermediate, packed response
query pins               admitted queries on retained or moving domains
preparation transient    CRS construction and artifact cache charged to the cgroup
guard and host reserve   512 MiB resident guard below the 7 GiB soft limit, unchanged
```

## Open questions and what to quantify

1. **Tail removal reuse rule.** Demonstrate with the existing construction that
   B's unit artifacts and hints are identical inside and outside the tail
   domain, and that a wallet's query against the tail decodes B's rows at their
   B-local coordinates. If not, budget bounded preparation of plain B before
   the routing switch. Blocking for D3.
2. **Deep-reorg recovery epoch.** Specify the recovery epoch encoding, how
   wallets learn a session is noncanonical, and the test that exercises a
   reorg across a sealed boundary. Blocking for D8.
3. **Batching on production hosts.** Sweep offered rate and batch size on the
   8 GiB Linux workers with full-size shards, report successful QPS alongside
   latency, 429 rate and peak cgroup memory, and find the saturation point.
   Confirm the portable packed-query path, not the aarch64 NEON path, on AVX-512.
4. **Worker-side packing cost.** Measure packing CPU and memory per query on
   the workers and add it to the ledger before removing the coordinator from
   the query path.
5. **Routing refresh cadence.** Decide how often wallets refetch the routing
   view and how a stale view is detected, and test a query sent under a
   superseded routing view during a compose window.
6. **Cover policy adversary model.** State whether the baseline protects
   against one worker or an observer of the whole service, and test the
   intersection attack against the persistent-set policy.
7. **Wire encoding.** Version the routing table, domain descriptors and
   session identities, and extend the interop harness to the new manifest.
9. **Confirmation depth and transition lead time.** Capture each observed
   Ironwood reorg's old tip, common ancestor, replacement tip, affected block
   hashes, and depth before rewinding the journal. Preserve a bounded event log
   across restarts; compare it with node-side fork history over a stated date and
   height window. Report the maximum and distribution of canonical actions per
   block from a copied journal manifest, plus the row increment for each block
   as `ceil((first_position + action_count) / records_per_row) -
   ceil(first_position / records_per_row)`; include any observed orphaned-block
   bursts separately. Record request-to-qualified-pair, preparation, and
   publication times across retries on the intended hosts. Then choose the
   confirmation depth and burst allowance with an explicit observation window,
   reorg risk policy, and margin for blocks that cross multiple units or shards.
   The existing defaults and synthetic trace above are starting inputs, not a
   depth or worst-case lead-time claim.

## Implementation phases

**Phase 1: direct worker serving.** Workers receive query bodies, batch
locally, pack and answer; the public origin routes by domain; the coordinator
publishes a routing table and leaves the query path. Per-replica admission
covers the full request lifetime. Exit: production measurement with
concurrent publication, replica loss and cover-like traffic, reporting QPS,
latency, rejections and peak memory. No wallet change is needed for this
phase if the public route is preserved at the origin.

**Phase 2: immutable shards and composed routing.** Fixed storage shards, the
tail domain, manifest-owned routing, separated session, routing and placement
identities, confirmation-depth transitions with the recovery epoch, and the 2K
frontier cap. Exit: the reuse-rule test from open question 1, a reorg test
across a sealed boundary, full-size lifecycle tests through two successive
compose windows, and a versioned wallet interop run. This phase changes the
wallet protocol.

**Phase 3: pool placement.** Replace groups with per-domain placements and a
replication factor. Exit: consolidation and hotspot replication under the
existing operation phases, with the ledger counting both copies.

The only blocking design questions are the preprocessing reuse rule at tail
removal and the deep-reorg contract. Neither requires abandoning the
architecture above.

## What this removes and what it keeps

Removed: loans and both loan transitions, the lender and SETTLING roles,
restoration reservations, reorg undo across shard pairs, generation-bound
expiry for sealed domains, wallet-side domain choice, and the coordinator's
role in the query path.

Kept: unique canonical routing, now enforced by the manifest's routing table;
the 653-byte record and 33-record row; the p16 q48 profile and setup domain;
the 24-shard wallet ceiling; the memory admission ledger, 512 MiB guard and
7 GiB soft limit; the persisted operation phases for moves; and the rule that
no capacity claim stands until qualified on the intended hardware.
