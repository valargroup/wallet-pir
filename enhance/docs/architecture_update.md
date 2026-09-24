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
and every query body proxied, decoded and packed on the coordinator. An
isolated full-shard test on 8 GiB x86 workers found that multi-query batching
reduced throughput at load. D5 records the decision to use singleton scans.

This proposal keeps the record format, the p16 q48 profile, the public setup
domain, the 24-shard wallet ceiling, the memory admission ledger and the
persisted operation phases. It changes what a shard is, how the population floor
is produced, where queries are served, and how sessions expire.

## Objects

The update separates six things the current document folds together.

| Object | Meaning | Identity |
|---|---|---|
| Storage shard | Fixed global row range of `max_shard_rows`; immutable once sealed | Shard ID = global row start / `max_shard_rows` |
| Query domain | Ordered mapping of stored rows into PIR coordinates under one setup seed, with logical rows and units | Domain ID, setup seed, unit identities, logical rows |
| Routing view | The manifest's assignment of every populated global row to exactly one query domain and local row | Routing revision plus chain anchor |
| Session | A query domain's content, geometry, setup and packing parameters as the wallet must use them | Content digest over the domain's unit identities and parameters, with the domain's recovery epoch |
| Placement | Which replicas hold which query domains and are ready | Placement revision |
| Request router | Stateless data-plane service that reads the fixed query-binding prefix, selects an eligible replica from the current placement, forwards the opaque query and returns the worker's packed response | Deployed instance plus loaded placement revision |

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
ceiling that is 24 times the per-query work of one shard. Twenty-four
independent q48 requests upload 6.78 MB at the measured 282,652 bytes per
32K query; only about
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
durable lifecycle event is the frontier reaching `M` rows and a successor
opening after confirmation; D8 defines provisional tip routing until then.

**Rationale.** The loan exists solely to give a successor a population floor.
It costs two atomic two-shard publications, two rebuilds of the lender's last
unit, a restoration reservation, lender counting, the SETTLING role, and reorg
undo on both sides. D3 produces the same floor without any of it. Immutable
sealed shards are also what make D7's long-lived sessions sound.

**Cost.** None in density. The first shard of the database has no predecessor
and keeps today's bootstrap exception.

### D3. Population floor from a composed tail domain with manifest-owned routing

**Decision.** When successor B first has rows, the manifest publishes B's
query domain and a routing table. B uses the tail layout while `n < m` and may
be provisional until A's seal confirms (D8). Using `n` for B's occupied rows,
including a partially populated last row, and the defaults `M = 32,768`,
`m = 4,096`:

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
When B first occupies its `m`th row, the extra unit is dropped, the routing
table sends A's suffix back to domain A, and B's units keep their offsets.

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

**Status.** A construction-level test in
`enhance/services/enhance-pir-server/src/runtime.rs` independently prepares B
in plain and tail contexts under B's seed. B's unit identity, persisted
database, persisted partial CRS, and partial hint are byte-identical. Fresh
wallet-construction queries for B-local rows 0, 2047, and 2048 decode the same
rows at the same coordinates with and without A's suffix. The whole-domain
hint differs because the tail includes A's extra unit; its packing state must
be rebuilt when that unit is dropped, as D6 already requires. This proves the
unit reuse rule for the current P16Q48 construction and tested 4K/8K shapes;
it does not implement or validate manifest routing. Run with
`cargo test -p enhance-pir-server --lib tail_domain_reuses_b_units_and_decodes_b_at_unchanged_coordinates`.

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

### D5. Use singleton query scans; do not move batching to workers

**Decision.** Do not include multi-query evaluation or a coalescing window in
this architecture update. Each admitted query gets its own worker evaluation.
When packing moves to workers, use a bounded per-worker admission queue and
independent evaluation slots, without waiting to form batches. The measured
batching prototype is retained as evidence, not included in this design.

**Evidence.** Two isolated 8 GiB `c-4` workers each held a 768 MiB shard; the
coordinator shared one worker host. In
matched 30-second runs at 16 offered QPS, singleton evaluation completed
458/480 queries, versus 416/480 with experimental SSE2 batching up to eight;
p99 latency was 2.4 versus 10.5 seconds. The earlier scalar batch path
completed only 143/480, although that run preceded a scheduler fix and is a
less direct comparison. On the worker-only host, the SSE2 batch run used
131 ms of scan time per query versus 86 ms for singleton scans. Three repeated
singleton runs completed every query at 12 and 14 QPS; 16 QPS was marginal.
At 0.1, 1, 2, 4 and 8 QPS, singleton scans added no collection wait and
returned every answer correctly. The
[Linux measurement and raw reports](../evidence/batching-linux-2026-09-24/README.md)
record the topology and limits. A local M4 Max had shown a twofold scan gain
for eight-query bursts, but that did not transfer to the target x86 hardware.

**Revisit rule.** Consider batching only after a different worker CPU or
evaluation kernel demonstrates higher sustained correct QPS at an acceptable
latency and memory cost on the intended hardware. Moving the queue to workers
alone does not establish that gain.

### D6. Workers pack; a request router replaces the coordinator in the query path

**Decision.** Split the public origin, request routing and control plane into
explicit components. Caddy terminates TLS and preserves the public wallet
endpoint. It sends manifest and control routes to the coordinator and the
query route to a request-router process. The router reads the fixed `EPQ4`
query-binding prefix, selects an eligible replica from an atomically loaded
placement revision, forwards the otherwise opaque query and returns the
worker's packed response. It does not decode PIR keys, evaluate, pack, own
placement or make a replica ready.

Phase 1 deploys the router as a separate process on the coordinator host,
behind the existing Caddy origin:

```text
wallet -> Caddy on coordinator host -> request router -> selected worker
                                      ^
                                      |
                         atomic placement snapshot
                                      |
                                 coordinator
```

The coordinator publishes an internal, revisioned placement snapshot. The
router loads and swaps that snapshot atomically; it does not synchronously ask
the coordinator where to send each request. Co-location is a deployment choice,
not an identity or protocol dependency: the router can later move to another
host or run as multiple instances without changing the wallet endpoint. Thus
"the coordinator leaves the query path" means that the coordinator process
never receives a query body, not that query bytes bypass its host in Phase 1.
Wallets neither select nor learn a physical replica.

Within a domain, responsibilities are:

| Step | Owner | Rule |
|---|---|---|
| Eligibility | Coordinator placement | Publish replicas that hold the exact domain session and are ready at the placement revision |
| Selection | Request router | Choose the eligible replica with the fewest requests currently outstanding through that router; rotate equal-load ties |
| Admission | Worker | Independently admit or reject the complete request lifetime under local resource limits |
| Scheduling | Worker | Admit each request under local limits and run one evaluation per query |

Router-local outstanding-request counts are a routing hint, not an admission
claim, and require no live worker queue-depth feed. The worker remains
authoritative for capacity. A fixed preferred replica is not used because it
would leave replicated query capacity idle, including capacity added for a hot
frontier.

The worker protocol must distinguish rejection before evaluation is accepted
from evaluation, packing and ambiguous post-acceptance failures. The router may
replay once to one different eligible replica only after that explicit
not-admitted/not-ready response or an upstream failure known to precede
acceptance. It never automatically replays any other failure. Although queries
are read-only, this bound prevents duplicate cryptographic work and retry
storms. Replay requires the router to retain the body, so it enforces the fixed
body-size limit, a bounded number of concurrent buffered bodies and its own
overload rejection.

The selected replica receives the body, evaluates, packs with the query's own
upload keys, and answers. The coordinator continues to ingest and publish
manifests, routing and placement, but does not process query bodies.

**Rationale.** Today the coordinator buffers every 282 KB body, decodes it,
forwards to one replica at a time, packs, and gates the fleet with a global
slot count. Its sampled cgroup peak was about 8 GB. It is a single point of
failure and a fixed compute and memory ceiling that does not grow with the
fleet. A logically separate router removes decoding and packing from that
process while preserving the current public endpoint and private worker
network, so Phase 1 needs no wallet or host migration.

**Cost.** Packing moves its CPU and memory onto the 8 GiB workers and must be
measured against the same worker budget. Worker admission must cover the complete
request lifetime: bounded body reception, decoding, queued requests,
evaluation, packing, and response buffering. An execution slot alone does not
bound queued bodies. While co-located, router CPU, bounded body buffers,
connections and TLS/network overhead are charged to the coordinator host and
can contend with publication. All query bytes still cross that host's NIC, and
host failure still removes the public query endpoint. Process separation
permits later relocation or replication; co-location alone does not provide
origin high availability or horizontal network scaling.

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

### D8. Tip routing is provisional until boundary confirmation

**Decision.** Ingest complete canonical blocks and publish every new record at
the tip anchor. Partition records by fixed global row range even when one block
crosses several `M` boundaries; retain its excess records and partial final row.
The block that fills a predecessor's last row starts that boundary's confirmation
clock. Use `1,000` blocks, matching
[Zakura's local maximum reorg window](https://github.com/zakura-core/zakura/blob/8a88926d0bf7e75eaab2d26bd0ad90c0fc88787f/crates/zakura-chain/src/parameters/constants.rs#L22-L30).
Seal only when the tip is at least 1,000 blocks above that block and its hash remains
canonical. Depth is measured from the crossing block, not a later publication.
This is an explicit dependency on Zakura's local finality policy, not a measured
Ironwood reorg bound or a consensus guarantee. Reorgs wholly inside the window
remain ordinary provisional rollbacks.

| State | Tip publication | After the boundary confirms |
|---|---|---|
| A first reaches `M` full rows | Keep A mutable; no successor domain is needed until B has a row | Seal A if those rows remain canonical |
| B has `0 < n < m` occupied rows | Open provisional B with D3's tail unit; route A's last `m` rows and B's rows through B | Mark B's opening durable; keep the tail while `n < m` |
| B reaches `m` occupied rows before A's seal confirms | Provisionally drop the tail unit, route A's suffix back to A and serve plain B | Finalize the already current routing state |

Each tip publication has one anchor and one complete routing view. Prepare and
admit all affected domains before publishing it; if preparation fails, retain
the previous anchored view rather than advertise uncovered tip rows. Process
multiple crossings in order within a block. Only the final occupied range is
the growing frontier; intermediate full ranges remain provisional until their
own boundary clocks confirm. Promotion changes lifecycle status and routing
revision, but an unchanged domain session need not change identity. A finalized
sealed runtime is never modified by ordinary appends. Thus confirmation delays
durable lifecycle transitions, while provisional routing provides tip latency.

**Rollback.** Find the common canonical ancestor and discard all later blocks
before replay. Withdraw provisional successors and tail routes invalidated by
the rollback, recompute fixed-range coverage and pending confirmation clocks
from the surviving chain, and atomically publish a complete replacement view.
Keep a finalized seal when its completing block remains canonical, even if the
new tip is fewer than 1,000 blocks above it. Mutable domains receive new
content revisions as needed. A removed provisional domain
cannot remain in the canonical routing view. Retained snapshots follow D3's
stale-view rule; wallets must reject their orphaned anchors. A replayed crossing
uses B-local row coordinates again; neither rows nor an entire crossing block
are deferred to fit a boundary.

**Deep-reorg recovery.** A sealed boundary should not be crossed by an ordinary
reorg accepted under the same Zakura policy. A node reset, source switch or
history inconsistency can still make it noncanonical. If rollback removes or
replaces a record in a sealed range, pause further publication and fence every
current and retained session of affected domains on
all serving replicas. The affected set starts at the first changed global row's
domain and includes downstream domains whose content or coordinates can change;
earlier sealed domains remain valid. The durable manifest recovery epoch starts
at zero and increments once
per such rollback, independently of the controller and PIR parameter epochs.
Encode this unsigned 64-bit value as a decimal string in JSON and a fixed-width
unsigned integer in the versioned binary protocol; fail closed on exhaustion.

The manifest carries the current recovery epoch and each domain descriptor
carries the epoch of its last recovery. A session identity hashes the protocol
revision, domain ID, that domain recovery epoch, and its content, geometry,
setup and packing identities. Affected replacement domains take the new epoch,
even if replay produces identical bytes. Publish revocation before replacement
content; old affected queries receive a noncanonical-session error, not an
answer from an orphaned snapshot. Already admitted work retains its resource
pin but its response must be rejected if the session was fenced. Wallets
refresh the routing view on that error or an increased manifest recovery epoch,
discard revoked sessions, and reuse unchanged sessions for unaffected domains.
Build replacement artifacts under new identities and never overwrite a sealed
artifact. Resume publication only after the replacement view and its ready
placements can be committed atomically.

**Cost.** Provisional successors, their tail units, mutable revisions and
recovery replacements require retention and memory admission, including a block
that crosses multiple boundaries. Immutable artifacts remain immutable; their
canonical status can change. Tip latency remains subject to the same successful
preparation and admission requirement as any publication.

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
tail unit                96 MiB while a compose window is open, including a provisional one
provisional domains      prepared successors and retained revisions pending confirmation
packing state            per domain, rebuilt when its unit set changes (D6 moves it here)
per-request lifetime     body, decoded keys, evaluation slot, intermediate, packed response
query pins               admitted queries on retained or moving domains
preparation transient    CRS construction and artifact cache charged to the cgroup
guard and host reserve   512 MiB resident guard below the 7 GiB soft limit, unchanged
```

The request router is not a replica ledger component. While it is co-located
with the coordinator, separately bound its retained request bodies, connection
state and other resident memory and charge them to that host's admission and
measurement.

## Open questions and what to quantify

1. **Tail removal reuse rule (resolved).** The construction test demonstrates
   byte-identical B unit artifacts and partial hints, plus B-local wallet
   query decoding in the tail. Whole-domain packing still changes when A's
   extra unit is removed. D3's remaining work is routing implementation and
   lifecycle validation, not a plain-B preparation fallback.
2. **Deep-reorg recovery epoch (design resolved).** D8 specifies the epoch,
   revocation and replacement contract. The transition-model test covers a
   reorg across a sealed boundary; production routing and wallet interop remain
   Phase 2 work.
3. **Worker-side packing cost.** Measure packing CPU and memory per query on
   the workers and add it to the ledger before removing the coordinator from
   the query path.
4. **Routing refresh cadence.** Decide how often wallets refetch the routing
   view and how a stale view is detected, and test a query sent under a
   superseded routing view during a compose window.
5. **Cover policy adversary model.** State whether the baseline protects
   against one worker or an observer of the whole service, and test the
   intersection attack against the persistent-set policy.
6. **Wire encoding.** Version the routing table, domain descriptors and
   session identities, and extend the interop harness to the new manifest.
7. **Boundary capacity evidence.** The 1,000-block depth is a Zakura local
   policy choice, not an empirical reorg claim. Measure Ironwood block row
   bursts and successor prewarming and preparation time on intended hosts;
   provisional B must be ready at the crossing, before any depth accrues.

## Implementation phases

**Phase 1: direct worker serving.** Deploy the request router behind Caddy on
the coordinator host. Workers receive query bodies, evaluate them individually,
pack and answer; the router applies D6's placement, selection and bounded-retry rules;
the coordinator publishes routing and placement and leaves the query path.
Per-replica admission covers the full request lifetime, and router buffering is
bounded independently.

Exit: sweep the target offered rates with concurrent publication and
cover-like traffic. Show that both replicas receive traffic, that one
replica's admission saturation spills boundedly to its peer, and that replica
loss or a placement-revision change stops new
requests from reaching the removed replica. Restart the router and exercise a
stale placement so it fails closed rather than guessing. Report successful
QPS, latency, public 429/502 responses, worker rejection rate,
coordinator publication responsiveness, coordinator-host/router
peak memory and network throughput, and worker peak memory. No wallet change
is needed because Caddy preserves the public route.

**Phase 2: immutable shards and composed routing.** Fixed storage shards, the
tail domain, manifest-owned routing, separated session, routing and placement
identities, confirmation-depth transitions with the recovery epoch, and the 8K
frontier cap. Exit: replay the D8 model cases against the implementation, run
full-size lifecycle tests through two successive compose windows, and complete
a versioned wallet interop run. This phase changes the wallet protocol.

**Phase 3: pool placement.** Replace groups with per-domain placements and a
replication factor. Exit: consolidation and hotspot replication under the
existing operation phases, with the ledger counting both copies.

The D3 reuse rule and D8 recovery contract are resolved at the design level.
Their production routing and wallet behavior remain Phase 2 work.

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
