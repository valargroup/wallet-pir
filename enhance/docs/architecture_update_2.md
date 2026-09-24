# Enhance PIR architecture update 2: post-launch serving and placement

Design proposal, September 24, 2026. This follows the launch changes in
[the immediate architecture update](architecture_update.md); it describes no
deployed behavior and is not a launch prerequisite. A dedicated 8 GiB packing
router and 8 GiB evaluation workers are the sizing targets. The packing component
has a measured memory envelope below; the complete serving path remains unqualified.

## Scope and compatibility

After the immutable-shard and wallet-protocol launch, move query decoding and
packing to a separate packing-router tier, then replace replica groups with
per-domain pool placement. Workers continue to evaluate queries and return
intermediates. The launch retains coordinator decoding and packing, existing
replica groups, and existing admission limits unless separately qualified. Its
coordinator bottleneck remains until this follow-up is deployed.

Both changes preserve the launched wallet protocol, public endpoint, session
identities, request bindings and response format. Session identity binds packing
parameters, not the process or host performing packing. Wallets neither select
nor learn a physical replica. Session-scoped packing-key reuse is outside both
proposals and still requires a separate protocol review.

The immediate proposal owns domain composition, canonical routing, recovery
semantics and wallet behavior. This proposal changes where those contracts are
enforced, not their externally visible meaning. Decision numbers D6 and D10 are
retained for continuity with the original proposal.

## Deferred decisions

### D6. A separate packing router owns decoding, replica selection and packing

Caddy continues to terminate TLS at the public wallet endpoint. It sends
manifest and control routes to the coordinator, and queries to a packing router
on a separate host, initially targeting 8 GiB. The packing router validates the
launched request binding and upload keys, decodes the query, selects an eligible
evaluation replica from an atomically loaded placement revision, and sends the
bound evaluation request over the private network. The worker evaluates and
returns a bound intermediate. The packing router validates that result, packs
it with the query's own upload keys and the exact session's resident packing
material, and returns the existing wallet response.

```text
wallet -> public Caddy -> packing router -> selected evaluation worker
                              ^                        |
                              +----- intermediate -----+
                              |
                         packed response -> wallet

coordinator -> revisioned placement and session artifacts -> packing routers
            -> domain preparation and activation         -> workers
```

The coordinator retains ingestion, publication, placement and recovery control.
It publishes internal, revisioned snapshots and immutable session artifacts;
packing routers load and activate them without a synchronous coordinator lookup
per query. Routers neither own placement nor make workers ready. The coordinator
process no longer receives query bodies or performs per-query packing.

The initial deployment separates the packing-router host from the coordinator.
If Caddy initially remains on the coordinator host, all public query traffic
still crosses that host's NIC, and host failure still removes the endpoint.
Moving or replicating the public origin is separate deployment work; packing
router separation alone does not provide origin high availability.

#### Why packing belongs in this tier

The current `runtime::Packing` owns preprocessed domain hint material, published
parameters and top-key images. `coordinator::Snapshot` retains these objects;
workers already expose an evaluation API returning intermediates. Extracting
that serving path preserves the existing evaluation/packing split. Naively
adding packing to every worker would charge that resident state to each replica
and compete with database storage within its 8 GiB budget.

A packing router holds one packing-state object for each distinct domain session
it serves, shared across all evaluation replicas for that session. Retained
frontier sessions still require their own state where their material differs;
identical immutable material may be shared only under verified identity. Public
routing revisions alone must not cause a new copy. Per-query wallet upload keys
remain request-scoped and are separate from reusable server packing material.

For a session with resident packing material `P`, `r` evaluation replicas and
`k` packing-router copies, worker-side packing costs approximately `r × P` in
resident serving material; this design costs `k × P`, excluding preparation and
per-request allocations. Increasing `r` does not increase packing residency.
This is not a claim that the entire fleet needs only one copy: router replication
for availability or packing throughput requires additional copies of the sessions
those routers serve.

#### Scaling and admission

Packing-router assignment and evaluation-worker placement are separate maps.
Initially one router can serve all domains that fit its qualified budget. As the
resident session set grows, partition domains across routers. The query ingress
reads the launched protocol's fixed binding prefix (`EPQ4` in the investigated
framing) to select a router assigned to that exact session. It must enforce
bounded buffering and must not become another packing tier. The wallet URL and
framing remain unchanged. Ordinary Caddy load balancing alone does not provide
this domain-aware assignment; that ingress contract must be implemented before
partitioning the tier.

Replicate selected packing-router assignments when availability or hot-domain
packing throughput requires it. Do not load every domain on every router by
default: that would duplicate the full packing working set as the tier grows.
A single router per domain is a failure and throughput boundary for that domain,
even if many evaluation replicas are ready. Spare capacity and exact-session
readiness are required before a replacement router can receive its traffic.

| Responsibility | Owner | Rule |
|---|---|---|
| Eligibility and assignment | Coordinator placement | Publish exact-session-ready workers and packing routers at the placement revision |
| Worker selection | Packing router | Choose the eligible replica with the fewest evaluations outstanding through that router; rotate equal-load ties |
| Public request admission | Packing router | Reserve the complete request lifetime, including waiting for evaluation and packing |
| Evaluation admission and scheduling | Worker | Reserve local resources and run one evaluation per query within local limits |

The router's outstanding-evaluation counts guide selection; each worker decides
whether it has capacity. This requires no live worker queue-depth feed. Multiple
routers have independent counts, so local worker admission remains authoritative.
Router admission must reserve capacity before reading a public body, bound queued
requests and packing work, and retain charges until cancelled work actually ends.
Worker permits must cover internal body reception, decoded coefficients,
evaluation, serialization and bounded output buffering, not just the kernel.

The worker protocol must distinguish rejection before accepting evaluation from
ambiguous post-acceptance failures. The router may retry evaluation once, on one
different eligible worker, only after an explicit not-admitted/not-ready response
or an upstream failure known to precede acceptance. It must not automatically
replay evaluation after an ambiguous failure or after packing fails. Ingress must
likewise not replay a public query to another packing router after ambiguous
acceptance. Bound retained request data, deadlines, connections and retry counts
at both tiers to prevent duplicate cryptographic work and retry storms.

The September 23, 2026 baseline coordinator buffered every 282 KB body, decoded
it, forwarded to one replica at a time, packed the result, and enforced a
fleet-wide slot count. Its sampled cgroup peak was about 8 GB. That combined
process measurement does not establish how much memory a standalone packing
router needs, or that 8 GiB will suffice. This split makes packing capacity
independent of coordinator publication and evaluation replication; it does not
remove packing CPU or intermediate-transfer costs.

### D10. Pool placement with a replication factor instead of rigid replica pairs

Place each query domain on `r` workers from a pool, with `r = 2` by default and
a higher factor allowed for the frontier. Per-domain placements replace groups;
the reservation ledger and persisted operation phases carry over.

Rigid pairs require growth in units of two machines and give a hot frontier the
same replica count as an idle sealed shard. A pool allows finer growth, better
utilization and extra replicas for hotspots. Two copies still provide roughly
50 percent raw storage efficiency. This is the lowest-priority change. Worker
replication does not change packing-router assignment or its replication factor.

## Routing and recovery prerequisites

Packing routers enforce the launched routing revision and recovery epoch checks,
including stale-view and invalidated-session errors. Workers validate and pin the
exact evaluation session. Packing routers pin the matching packing state and
validate the intermediate's binding, shape and coefficient range before packing.
Responses retain the launched binding; recovery revocation must fence affected
responses at the packing router even when evaluation began before revocation.

Before cutover, define publication and acknowledgment ordering across coordinator,
ingress, packing routers and workers, how disconnected or stale participants lose
serving authority, and restart behavior. Publishing a route requires both packing
and evaluation readiness for the exact session. An atomic local snapshot swap
alone does not make every participant current. Test ordinary routing switches
separately from recovery revocation, where affected in-flight responses must be
fenced. Router assignment moves reserve both source and destination state until
preparation, activation and draining complete.

Packing preparation must have explicit ownership: the coordinator may produce
immutable hint/session artifacts during publication, while routers build or load
the serving representation before acknowledging readiness. Verify artifact
identity against the published session. The coordinator can retain public session
metadata needed by its wallet routes without retaining every serving `Packing`
object; any construction overlap remains charged to publication memory. Reusing
the current `Snapshot` unchanged would leave those objects resident there.

The `EPQ4` reference describes the investigated framing. The implementation must
use the binding of the launched protocol without a new wallet framing requirement
solely to relocate packing. Keep evaluation APIs private and restrict access to
the serving and control tiers under the existing trust model.

## Memory ownership and qualification

Workers inherit the immediate proposal's database, retained revision, tail-unit,
preparation and query-pin charges. They do not gain resident packing state or
public upload keys. Their internal request and intermediate-response allocations
still need explicit bounds, including concurrent publication and cancellation.

For a packing router, budget at least:

```text
resident material for every distinct retained/pinned session assigned here
+ packing-state construction/loading overlap and assignment-move overlap
+ admitted requests × (body + decoded keys/coefficients + intermediate
                      + transient packing allocations + buffered response)
+ bounded queues, connections, runtime overhead and host reserve
```

Peak overlap matters; per-stage maxima cannot replace this sum without proving
that allocations have disjoint lifetimes. Slow readers, cancelled work and
retained-session expiry must not release charges before their resources are
actually freed. Bound ingress/TLS buffers separately and charge them to their
own host. A fixed per-domain overhead or semaphore is not evidence of fit.

The [direct worker serving investigation](worker_serving_investigation.md)
records the current path and the earlier worker-packing alternative. Its
worker-packing budget is not the gate for this revised design. Before cutover:

1. On the intended 8 GiB Linux router, measure resident `Packing::new` output by
   domain geometry and session count, including retained frontier sessions and
   setup peaks. Record exact binary revision, assignments, allocation high water,
   cgroup current/peak memory, CPU and network utilization. Reset peaks between
   trials. Derive a maximum resident assignment and admission budget with host
   headroom; increase host size or partition assignments if it does not fit.
2. Exercise the actual ingress/router/worker HTTP path with fresh wallet queries,
   full-size domains, cover-like traffic and concurrent publication. Sweep offered
   rates; measure exact-answer successful QPS, p50/p95/p99 latency, 429/502/503,
   evaluation rejection rate, packing queue/CPU, intermediate transfer bytes and
   peak memory for each tier. Isolated packing timings do not qualify throughput.
3. Exercise slow uploads/readers, disconnects, worker and router loss, retained
   session expiry, assignment moves, stale placement, restart and recovery
   revocation during evaluation and packing. Verify bounded retry, fail-closed
   routing, pinned-resource accounting and coordinator publication responsiveness.

The [September 24 packing-budget experiment](../evidence/packing-budget-2026-09-24/README.md)
measures the production packing component from frozen commit `a3c7626` under Linux
cgroup caps. Each independently allocated packing object retains about 689 MiB
of live heap. Ten objects can be constructed under an 8 GiB cap, but constructing
the eleventh is OOM-killed. That hard boundary is not a serving assignment limit.

The initial assignment target is six retained serving objects, one additional
construction/activation slot, and four concurrent packing requests, within a
7 GiB process ceiling on the proposed 8 GiB host. The six-plus-one cold-cache
trial peaked at 6.27 GiB with 512 verified responses. Seven serving objects plus
one construction slot also passed, but its cold-cache peak was about 6.94 GiB;
keep that as a measured limit rather than the default assignment. Count distinct sessions,
including retained versions and request pins, rather than domains or evaluation
replicas. Six retained frontier sessions consume the default serving allocation;
adding sealed domains then requires another router assignment or explicit
qualification of the higher limit.
Release an expired/unpinned object before reusing the construction slot; overlapping
multiple publications or assignment moves requires additional reservation or
refusal. The 1 GiB host reserve must also cover any ingress/TLS service placed here.

These measurements exercise packing allocations, separately allocated copies and
construction overlap, not the extracted router's HTTP lifecycle, recovery, aged
allocator behavior or production throughput. Requalify the final binary and its
full path on the intended host before cutover. Keep the existing public query path
until those checks pass.

## Implementation phases

### Phase 1: extract the packing router

Extract the coordinator's decode/evaluate/pack serving path into a separate
process and host. Keep worker evaluation private. Add revisioned placement and
session-artifact loading, readiness acknowledgment, recovery fencing and
independent admission budgets. Route public queries to the packing router while
manifest/control routes stay on the coordinator. This requires no wallet change.

Pass the revised memory and end-to-end gate before cutover. Verify that both
worker replicas receive traffic, pre-admission rejection can spill to the peer
within the retry bound, and removed replicas receive no new evaluations. Verify
that router restart or missing session material fails closed. Report the maximum
qualified domain/session assignment and request concurrency for the router size.

### Phase 2: scale packing-router assignments

Add domain-aware ingress and revisioned router assignments when one router's
resident state or compute limit requires it. Qualify assignment moves, overlap,
draining and independent router replication before relying on them for capacity
or availability. Adding evaluation replicas must not allocate additional packing
objects on existing routers. Validate public-origin availability separately.

### Phase 3: pool placement

Replace worker groups with per-domain placements and a replication factor.
Validate consolidation and hotspot replication under the existing operation
phases, with the ledger counting both copies at the default factor. Also verify
interrupted moves and controller restart, exact-session readiness before placement
publication, and reservations for source and destination during moves. This work
can proceed independently of packing-router scale-out after Phase 1.

## Resulting architecture

The coordinator retains ingestion, publication and placement control. Separately
sized packing routers decode requests, select evaluation replicas and pack their
results using resident material for assigned domain sessions. Workers retain the
evaluation/database role. Evaluation replicas, packing capacity and publication
capacity can grow independently, with packing material duplicated only across
the packing-router copies assigned to serve each session.
