# Enhance PIR architecture update: immutable shards, composed routing, direct worker serving

Design proposal, September 24, 2026. This describes proposed behavior and does
not amend [architecture](architecture.md) until accepted. Capacity figures are
planning inputs; they must be qualified on the intended 8 GiB Linux hosts
before they can be treated as limits.

## Motivation and measured baseline

The current architecture devotes much of its complexity to shard lifecycle:
loans, returns, restoration, lender counting, and reorg undo across two shards.
The measured bottleneck, however, is query serving. On September 23, 2026, the
production admission measurement reached about 33 correct queries per second
on two replicas holding one shard. The fleet had a global cap of four active
queries, and the coordinator proxied, decoded and packed every query body.
Separately, an isolated full-shard test on 8 GiB x86 workers found that batching
queries reduced throughput under load (D5).

This proposal replaces loans with composed query domains, makes sealed storage
shards immutable, moves query serving to workers, and separates session validity
from routing freshness. It keeps the 653-byte record, 33-record row, p16 q48
profile, public setup domain, 24-shard wallet ceiling, memory admission ledger,
and persisted operation phases.

## Objects and identities

The design distinguishes six objects:

| Object | Meaning | Identity |
|---|---|---|
| Storage shard | Fixed global row range of `max_shard_rows`; immutable once sealed | Shard ID = global row start / `max_shard_rows` |
| Query domain | Ordered mapping of stored rows into PIR coordinates under one setup seed, with logical rows and units | Domain ID, setup seed, unit identities, logical rows |
| Routing view | Manifest assignment of every populated global row to exactly one query domain and local row | Routing revision plus chain anchor; recovery epoch carried in the view |
| Session | Domain content, geometry, setup and packing parameters used by the wallet | Digest binding these identities, the protocol revision, domain ID and domain recovery epoch (D7–D8) |
| Placement | Replicas that hold query domains and are ready to serve them | Placement revision |
| Request router | Stateless service that reads the fixed query-binding prefix, selects an eligible replica, forwards the opaque query and returns the packed response | Deployed instance plus loaded placement revision |

Every populated row has exactly one canonical route. A storage shard may still
physically contain rows routed through another domain, and those rows may
remain queryable through retained sessions. D3 states the privacy consequences.

## Decisions

### D1. Public shard selection stays; no whole-database fan-out

A wallet publicly selects the query domain assigned by the routing view and
hides its row within that domain. This design excludes whole-database fan-out
with coordinator-side summation, including as an optional mode. D4 separately
allows wallets to issue cover queries.

Single-server PIR performs a linear scan. Hiding a row across the whole history
therefore requires a full-chain scan for every query, indefinitely. At the
24-shard wallet ceiling, that costs 24 times the work of one shard. At the
measured 282,652 bytes per 32K query, twenty-four independent q48 requests upload
6.78 MB. About 4.7 MB consists of query vectors; sharing keys across requests
would require a protocol that does not yet exist. Unequal shard sizes are
already supported by unit summation and do not prevent fan-out.

Coordinator packing was a consequence of that fan-out design. The query is
encrypted under the wallet's key end to end, so a worker that packs learns no
more than a coordinator that packs.

The privacy cost is public domain selection: the server learns where the
wallet's notes fall at a granularity of about a million actions. The timing
prior on fresh notes is the same under all the designs considered here.

### D2. Fixed storage shards, immutable once sealed, no loans

Storage shard `k` contains the fixed row range `[k * M, (k + 1) * M)`. Records
never move between shards. Once the last row is full and its completing block
has reached confirmation depth, the shard is sealed and its runtime is never
modified. Reaching `M` rows and opening a successor become durable after
confirmation; D8 defines provisional routing before then.

Loans exist solely to give a successor a population floor. They require two
atomic two-shard publications, two rebuilds of the lender's last unit, a
restoration reservation, lender counting, the SETTLING role, and reorg undo on
both sides. D3 supplies the same floor without these operations. Immutability
also supports the long-lived sessions in D7.

There is no density cost. The first shard has no predecessor and retains the
existing bootstrap exception.

### D3. Population floor from a composed tail domain with manifest-owned routing

When successor B first contains rows, the manifest publishes B's query domain
and the routes into it. Let `n` be B's occupied rows, including a partially
populated last row. With defaults `M = 32,768` and `m = 4,096`, routing is:

| State | A's first `M - m` rows | A's last `m` rows | B's rows |
|---|---|---|---|
| B open, `n < m` | Domain A | Tail domain | Tail domain |
| `n >= m` | Domain A | Domain A | Domain B |

B uses the tail layout while `n < m`; its opening may remain provisional until
A's seal confirms (D8). Wallets follow the manifest's assignment of each
populated global row to one domain and local row. Before use, they validate
coverage, overlap, bounds and revision.

The tail is B's own domain with one extra unit, using B's setup seed:

```text
B's rows          at tail local rows [0, m)         B's own units, offsets unchanged
A's last m rows   at tail local rows [m, 2m)        one extra unit, prepared under B's seed
logical rows      2m during the compose window
```

B's rows cannot reach local row `m` while `n < m`. When B first occupies its
`m`th row, the extra unit is dropped and A's suffix routes back to domain A.
B's units keep their offsets throughout.

During composition, the anonymity sets match the loan design: a tail query
could target any of `m + n` rows, and an A query any of `M - m` rows. A's
runtime is untouched. Ending composition requires a routing change and removal
of one unit. Stable B coordinates make this possible: `Engine::prepare` in
`enhance/services/enhance-pir-server/src/runtime.rs` derives each unit's setup
slice at its local offset from a full-size setup generated with the shard's
seed. Those slices remain stable as the domain grows.

The cost is one extra preprocessed unit of `m` rows: 96 MiB at the defaults,
prepared when B opens and dropped when composition ends. Changing the unit set
requires rebuilding the domain's packing state, as already happens on each
publication for a growing domain. That state currently belongs to the
coordinator and moves to workers under D6.

A construction test in the same runtime file independently prepares B in plain
and tail contexts under B's seed. B's unit identity, persisted database,
persisted partial CRS and partial hint are byte-identical. Fresh queries built
through the wallet construction for B-local rows 0, 2047 and 2048 decode the
same rows at the same coordinates with and without A's suffix. The
whole-domain hint differs because the tail includes A's extra unit, so packing
state must still be rebuilt when it is removed. This establishes unit reuse
for the current P16Q48 construction and tested 4K/8K shapes; it does not
implement or validate manifest routing. Run it with:

```sh
cargo test -p enhance-pir-server --lib tail_domain_reuses_b_units_and_decodes_b_at_unchanged_coordinates
```

The design accepts a limitation: A still physically holds its suffix. A client
that ignores the routing table can query those rows through A; a client with a
retained tail session can query them through that old domain after the switch.
The server cannot detect the selected row in either case. D7's public routing
checks constrain stale requests, but do not make these rows cryptographically
unqueryable. The population claim applies to compliant clients using the
current routing view, subject to transition and stale-view limits. As in the
existing 4K design, the floor is a population policy, not a proof.

### D4. Shard-selection privacy beyond the floor is a defined client policy

Cover traffic is optional. When enabled, the baseline policy uses a persistent
cover set for a stated interval, fresh randomness for each query, and identical
timing, ordering and retry behavior for every domain in the set. Under that
policy, the target should be indistinguishable within the observable cover set
to an observer of the whole service.

Independently resampling decoys lets an observer intersect repeated sets to
identify the target. Sending the real query first, retrying only it, or fetching
several rows only from its domain also reveals structure. A persistent set
with uniform behavior removes these channels. Using all domains since the
wallet's birthday is easier to specify precisely and reveals only the birthday
range already exposed by compact scanning, at greater cost.

Cover queries multiply server work by the set size. Each wallet chooses that
cost rather than imposing it on every query served by the fleet.

### D5. Use singleton query scans; do not move batching to workers

Each admitted query gets its own worker evaluation. Workers use bounded
admission queues and independent evaluation slots, with no multi-query
evaluation or wait to collect a batch. The batching prototype remains evidence
for this decision.

The Linux comparison used two isolated 8 GiB `c-4` workers, each holding a
768 MiB shard; the coordinator shared one worker host. Matched 30-second runs
at 16 offered QPS produced:

| Measurement | Singleton scans | Experimental SSE2 batches, up to eight queries |
|---|---:|---:|
| Completed queries | 458/480 | 416/480 |
| p99 latency | 2.4 seconds | 10.5 seconds |
| Scan time per query on the worker-only host | 86 ms | 131 ms |

An earlier scalar batch run completed 143/480 queries. It preceded a scheduler
fix, so it is a less direct comparison. Three repeated singleton runs completed
every query at 12 and 14 QPS; 16 QPS was marginal. At 0.1, 1, 2, 4 and 8 QPS,
singleton scans returned every answer correctly without a collection wait.
The [Linux measurement and raw reports](../evidence/batching-linux-2026-09-24/README.md)
record the topology and limits. A local M4 Max showed a twofold scan gain for
eight-query bursts, but that gain did not transfer to the target x86 hardware.

Revisit batching only if a different worker CPU or evaluation kernel sustains
more correct QPS at acceptable latency and memory cost on the intended
hardware. Moving the queue to workers alone does not demonstrate a gain.

### D6. Workers pack; a request router replaces the coordinator in the query path

Caddy continues to terminate TLS at the public wallet endpoint. It sends
manifest and control routes to the coordinator, and queries to a separate
request-router process. The router reads the fixed `EPQ4` query-binding prefix,
selects an eligible replica from an atomically loaded placement revision, and
forwards the otherwise opaque body. The worker evaluates, packs with the
query's own upload keys, and returns its response through the router.

The router does not decode PIR keys, evaluate, pack, own placement or make a
replica ready. The coordinator continues ingestion and publication of
manifests, routing and placement. It publishes an internal, revisioned
placement snapshot that the router loads and swaps atomically, without a
synchronous coordinator lookup for each query. Wallets neither select nor
learn the physical replica.

Phase 1 puts the router behind the existing Caddy origin on the coordinator
host:

```text
wallet -> Caddy on coordinator host -> request router -> selected worker
                                      ^
                                      |
                         atomic placement snapshot
                                      |
                                 coordinator
```

The coordinator process no longer receives query bodies. Query bytes still
cross its host in Phase 1. The router can later move to another host or run as
multiple instances without changing the wallet endpoint; its location is not
part of a protocol or identity.

| Responsibility | Owner | Rule |
|---|---|---|
| Eligibility | Coordinator placement | Publish replicas holding the exact domain session and ready at the placement revision |
| Selection | Request router | Choose the eligible replica with the fewest requests outstanding through that router; rotate equal-load ties |
| Admission | Worker | Admit or reject the complete request lifetime under local resource limits |
| Scheduling | Worker | Run one evaluation per query within local limits |

The router's outstanding-request counts guide selection; the worker decides
whether capacity is available. This requires no live worker queue-depth feed.
A fixed preferred replica would leave replicated capacity idle, including extra
capacity assigned to a hot frontier.

The worker protocol must distinguish rejection before accepting evaluation
from evaluation, packing and ambiguous post-acceptance failures. The router
may replay a request once, to one different eligible replica, only after an
explicit not-admitted/not-ready response or an upstream failure known to
precede acceptance. It must not automatically replay any other failure.
Although queries are read-only, this restriction bounds duplicate cryptographic
work and retry storms. Retaining bodies for replay requires a fixed body-size
limit, a bounded number of concurrently buffered bodies, and router overload
rejection.

Today the coordinator buffers every 282 KB body, decodes it, forwards to one
replica at a time, packs the result, and enforces a fleet-wide slot count. Its
sampled cgroup peak was about 8 GB. Its compute and memory ceiling does not grow
with the fleet, and it is a single point of failure. Moving decoding and packing
to workers removes that work from the coordinator while preserving the public
endpoint and private worker network. Phase 1 requires no wallet or host migration.

Worker-side packing must fit the same 8 GiB worker budget. Admission must cover
body reception, decoding, queued requests, evaluation, packing and response
buffering; evaluation slots alone cannot bound queued bodies. On the
coordinator host, router CPU, bounded body buffers, connections and TLS/network
overhead remain charged to that host and may contend with publication. All
query traffic still crosses its NIC, and host failure still removes the public
endpoint. Separate processes permit later relocation or replication, but Phase
1 does not provide origin high availability or horizontal network scaling.

The [direct worker serving investigation](worker_serving_investigation.md)
audits the current route and memory model, specifies request-lifetime and
origin-routing contracts, and defines the production measurement gate.
Worker-side packing has not yet passed that gate.

### D7. Session validity is separate from routing freshness and placement

The manifest versions session, routing and placement identities separately. A
session binds a domain's content, geometry, setup and packing parameters. The
routing view assigns rows to domains at a chain anchor. Placement identifies
ready replicas. Moving a replica does not invalidate a session.

A sealed domain's session remains valid until its content changes or recovery
marks it noncanonical. Wallets refresh routing on a defined cadence and reuse
unchanged session material. The current generation limit expires sessions after
five publications, forcing refreshes roughly every six minutes even for sealed
shards. Removing that expiry is sound for immutable content, but a cached
session cannot tell a wallet whether a row now routes through A or the tail.
Routing therefore needs its own, cheaper refresh.

The draft wallet contract has been validated as a standalone model and is not
deployed. A routing view contains a strictly increasing revision, recovery
epoch, chain anchor, domain descriptors, and ordered half-open row intervals.
Each route specifies a global interval, domain ID and local-row start. Wallets
reject gaps, overlaps in global or domain-local rows, unknown or duplicate
domains, intervals outside populated local spans, malformed hashes, and unknown
wire fields.

The session ID hashes a domain-separated encoding of domain ID, logical
geometry, populated spans, content digest, setup digest and parameter ID, with
the protocol revision and domain recovery epoch specified in D8. It binds the
domain's packing parameters. Publication revision, chain anchor and replica
placement are excluded. The content digest covers the entire queryable PIR
database, including padding and unit order; source-record hashes alone are
insufficient.

The candidate refresh policy fetches routing at sync start and at most every
30 seconds while querying. Each query carries its session ID and routing
revision/recovery epoch. The public router checks the current view at admission:

- A superseded routing view returns HTTP 409. The wallet refreshes and builds
  a fresh query under the new route.
- An invalidated session returns HTTP 410.
- A query admitted before a routing switch may finish against its pinned
  domain. The wallet checks the response binding and its locally accepted
  chain anchor. D8 additionally fences responses from revoked sessions.

This bounds routine staleness and detects routing switches immediately at
admission, provided every public query entry point enforces the revision check.
The 30-second interval remains a candidate pending wallet traffic and
transition measurements.

Every admitted query still needs a lifetime pin, including queries against
sealed domains during eviction or a move. Only mutable domains need repeated
content revisions, but all domains need safe resource lifetimes.

### D8. Tip routing is provisional until boundary confirmation

Ingest complete canonical blocks and publish every new record at the tip
anchor. Partition records by fixed global row range, even if one block crosses
several `M` boundaries. Retain excess records and the partial final row.

The block that fills a predecessor's last row starts that boundary's
confirmation clock. Seal the shard only when the tip is at least `1,000` blocks
above that block and its hash remains canonical. Measure depth from the crossing
block, not a later publication. The depth follows
[Zakura's local maximum reorg window](https://github.com/zakura-core/zakura/blob/8a88926d0bf7e75eaab2d26bd0ad90c0fc88787f/crates/zakura-chain/src/parameters/constants.rs#L22-L30).
It is a dependency on Zakura's local finality policy, not a measured Ironwood
reorg bound or a consensus guarantee. Reorgs within the window are ordinary
provisional rollbacks.

| State | Tip publication | After the boundary confirms |
|---|---|---|
| A first reaches `M` full rows | Keep A mutable; no successor domain is needed until B has a row | Seal A if those rows remain canonical |
| B has `0 < n < m` occupied rows | Open provisional B with D3's tail unit; route A's last `m` rows and B's rows through B | Mark B's opening durable; keep the tail while `n < m` |
| B reaches `m` occupied rows before A's seal confirms | Provisionally drop the tail unit, route A's suffix back to A and serve plain B | Finalize the already current routing state |

Each tip publication has one anchor and one complete routing view. Prepare and
admit every affected domain before publication. If preparation fails, retain
the previous anchored view so no uncovered tip rows are advertised. Process
multiple crossings within a block in order. Only the final occupied range is
the growing frontier; intermediate full ranges remain provisional until their
own confirmation clocks complete.

Promotion changes lifecycle status and routing revision; an unchanged domain
session can keep its identity. Ordinary appends never modify a finalized sealed
runtime. Confirmation delays durable lifecycle changes while provisional
routing serves the tip, subject to successful preparation and admission.

#### Ordinary rollback

Find the common canonical ancestor and discard later blocks before replay.
Withdraw invalidated provisional successors and tail routes, recompute
fixed-range coverage and pending confirmation clocks from the surviving chain,
and atomically publish a complete replacement view. Removed provisional domains
must not remain in canonical routing. Mutable domains receive new content
revisions as needed.

Keep a finalized seal if its completing block remains canonical, even when the
new tip is fewer than 1,000 blocks above it. Retained snapshots remain subject
to D3's stale-view limits, and wallets must reject orphaned anchors. Replayed
crossings use B-local coordinates again. Neither rows nor whole crossing blocks
are deferred to fit a boundary.

#### Deep-reorg recovery

An ordinary reorg accepted under the same Zakura policy should not cross a
sealed boundary. A node reset, source switch or history inconsistency can still
make a sealed range noncanonical. If rollback removes or replaces a record in
such a range, pause publication and fence every current and retained session of
affected domains on every serving replica. The affected set starts with the
domain of the first changed global row and includes downstream domains whose
content or coordinates can change. Earlier sealed domains remain valid.

The durable manifest recovery epoch starts at zero and increments once per such
rollback, independently of controller and PIR parameter epochs. Encode it as an
unsigned 64-bit value: a decimal string in JSON and a fixed-width unsigned
integer in the versioned binary protocol. Fail closed on exhaustion. The
manifest carries the current epoch; each domain descriptor carries the epoch
of that domain's last recovery.

Session identity hashes the protocol revision, domain ID, domain recovery epoch,
and content, geometry, setup and packing identities. Affected replacement
domains receive the new epoch even when replay produces identical bytes.
Publish revocation before replacement content. Old affected queries receive a
noncanonical-session error. Already admitted work keeps its resource pin, but
its response must be rejected if its session has been fenced.

On that error or an increased manifest recovery epoch, wallets refresh routing,
discard revoked sessions, and reuse unchanged sessions for unaffected domains.
Build replacement artifacts under new identities; never overwrite sealed
artifacts. Resume publication only when the replacement view and ready
placements can be committed atomically.

Provisional successors, tail units, retained mutable revisions and recovery
replacements all require memory admission, including when one block crosses
multiple boundaries. Sealed artifacts remain immutable even when their
canonical status changes.

### D9. Keep progressive frontier units with an 8K cap

Keep the current progression: a growing unit starts at 2,048 allocated rows,
grows to 4,096 and then 8,192, and rolls over only when the 8K unit is full.
Neither a 2K cap on every completed unit nor an initial 8K allocation is adopted.

A matched full-size schema-11 / p16 q48 comparison measured the tradeoff:

| Measurement | 2K unit cap | 8K unit cap |
|---|---:|---:|
| Units in a full shard | 16 | 4 |
| Median hint assembly | 7.81 seconds | 1.94 seconds |
| Sum of measured phases per revision | About 9.37 seconds | About 4.34 seconds |
| Median encrypted evaluation per sequential query | 11.11 ms | 7.25 ms |

With six retained frontier versions, the 2K cap saved 0.873 GiB of process peak
RSS. However, hint assembly reads and sums every unit on every frontier change.
Its added cost outweighed the savings in frontier preprocessing and persistence.
Packing rebuild time was similar. The
[D9 evidence in closed PR #100](https://github.com/valargroup/wallet-pir/pull/100)
contains the benchmark, commands, raw timings and provenance.

The comparison ran on a 128 GiB M4 Max, without an 8 GiB Linux cgroup or
concurrent traffic. Saving roughly one 768 MiB sealed database image does not
establish that another domain fits worker admission. The ledger must retain the
worst case of six retained 8K revisions, about 1.1 GiB of database bytes. The
separate 96 MiB tail unit is also only one component of the admission budget.

Revisit a 2K cap after incremental hint assembly and a matched 8 GiB Linux load
and memory run demonstrate a net gain without a material regression in
publication or query latency.

### D10. Pool placement with a replication factor instead of rigid replica pairs

Place each query domain on `r` workers from a pool, with `r = 2` by default and
a higher factor allowed for the frontier. Per-domain placements replace groups;
the reservation ledger and persisted operation phases carry over.

Rigid pairs require growth in units of two machines and give a hot frontier the
same replica count as an idle sealed shard. A pool allows finer growth, better
utilization and extra replicas for hotspots. Two copies still provide roughly
50 percent raw storage efficiency. This is the lowest-priority change.

### D11. Protocol semantics are versioned; packing-key reuse is deferred

Composed routing, routing tables and content-bound sessions require a new
protocol revision, even though record bytes, p16 q48 and the encrypted query
payload stay unchanged. Legacy clients reject the new manifest.

Session-scoped packing keys are deferred to a separate protocol review of
linkability and key-reuse safety. Existing transport-level linkability alone
does not justify key reuse.

`enhance/crates/enhance-pir/tests/routing_contract.rs` is an executable draft of
the v7 JSON contract. It tests strict decoding, canonical interval coverage,
composed A/B coordinates, session identity changes, and stale-view fencing
across tail removal and a recovery epoch. It does not change the v6 manifest,
HTTP framing, production server or wallet. Before migration, implement and
validate D8's recovery lifecycle, define the actual wire error payload and
response binding, and run the interop harness against a wallet implementing the
new revision.

## Memory ledger components per replica

Every replica must fit independently, including both copies at the default
replication factor. The ledger must count all of these components; no individual
entry is a complete budget.

| Component | Charge |
|---|---|
| Sealed domains | 768 MiB each at 32K logical rows |
| Frontier domain | Allocated units plus retained revisions of the growing unit, up to 6 x 192 MiB at the retained 8K cap |
| Tail unit | 96 MiB during composition, including a provisional window |
| Provisional domains | Prepared successors and retained revisions pending confirmation |
| Packing state | Per domain, rebuilt when its unit set changes; moves to workers under D6 |
| Per-request lifetime | Body, decoded keys, evaluation slot, intermediate and packed response |
| Query pins | Admitted queries on retained or moving domains |
| Preparation transient | CRS construction and artifact cache charged to the cgroup |
| Guard and host reserve | Existing 512 MiB resident guard below the 7 GiB soft limit |

Bound the router's retained request bodies, connection state and other resident
memory separately. While it shares the coordinator host, charge these resources
to that host's admission and measurements rather than a replica's ledger.

## Remaining validation and open decisions

D3's unit reuse rule is established for the tested construction; production
routing and lifecycle validation remain. No plain-B preparation fallback is
needed. D8's recovery epoch, revocation and replacement contract is settled at
the design level, and a transition-model test covers a reorg across a sealed
boundary. Production recovery and wallet interop remain Phase 2 work.

The remaining work is:

- Measure worker packing CPU and memory per query, include it in admission, and
  pass the D6 gate before removing the coordinator from the query path.
- Validate D7's stale-view detection during composition and choose the routing
  refresh cadence using wallet traffic and transition measurements. The
  proposed interval is 30 seconds.
- Test intersection attacks against D4's persistent cover-set policy under its
  stated adversary: an observer of the whole service.
- Complete the versioned wire encoding of routing tables, domain descriptors
  and session identities, including error payloads and response bindings, and
  extend wallet interop to the new manifest.
- Measure Ironwood block row bursts and successor prewarming and preparation
  time on intended hosts. Provisional B must be ready at the crossing, before
  any confirmation depth accrues. The 1,000-block depth remains a local policy
  choice rather than an empirical reorg claim.

## Implementation phases

### Phase 1: direct worker serving

Deploy the router behind Caddy on the coordinator host. Workers receive query
bodies, evaluate each independently, pack and answer. The router applies D6's
placement, selection and bounded-retry rules. The coordinator publishes routing
and placement and stops receiving query bodies. Worker admission covers the
complete request lifetime; router buffering is bounded independently. The
public route is preserved, so this phase requires no wallet change.

Before cutover, pass the worker-packing gate. For phase acceptance, sweep target
offered rates with concurrent publication and cover-like traffic. Verify that:

- Both replicas receive traffic, and admission saturation on one spills to its
  peer within the retry bound.
- Replica loss or a placement revision change stops new requests from reaching
  the removed replica.
- Router restart and stale placement fail closed rather than guessing.

Report successful QPS, latency, public 429/502 responses, worker rejection rate,
coordinator publication responsiveness, coordinator-host/router peak memory and
network throughput, and worker peak memory.

### Phase 2: immutable shards and composed routing

Implement fixed storage shards, tail domains, manifest-owned routing, separate
session/routing/placement identities, confirmation transitions and recovery
epochs, retaining the 8K frontier cap. This phase changes the wallet protocol.

Acceptance requires replaying D8's model cases against the implementation,
full-size lifecycle tests through two successive compose windows, and a
versioned wallet interop run.

### Phase 3: pool placement

Replace groups with per-domain placements and a replication factor. Validate
consolidation and hotspot replication under the existing operation phases,
with the ledger counting both copies at the default factor.

## Resulting architecture

This removes loans and both loan transitions, lender and SETTLING roles,
restoration reservations, reorg undo across shard pairs, generation-bound
expiry for sealed domains, wallet discretion over a row's canonical domain,
and coordinator query processing.

It preserves unique canonical routing through the manifest, the existing record
format and PIR profile, the public setup domain, the 24-shard wallet ceiling,
the memory ledger with its 512 MiB guard and 7 GiB soft limit, and persisted
operation phases for moves. Capacity claims still require qualification on the
intended hardware.
