# Enhance PIR architecture update 2: post-launch serving and placement

Design proposal, September 24, 2026. This follows the launch changes in
[the immediate architecture update](architecture_update.md); it describes no
deployed behavior and is not a launch prerequisite. All capacity figures remain
planning inputs until qualified on the intended 8 GiB Linux hosts.

## Scope and compatibility

After the immutable-shard and wallet-protocol launch, move query decoding and
packing to workers behind a request router, then replace replica groups with
per-domain pool placement. The launch retains coordinator decoding and packing,
existing replica groups, and existing admission limits unless separately
qualified. Its coordinator bottleneck remains until this follow-up is deployed.

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

### D6. Workers pack; a request router replaces the coordinator in the query path

Caddy continues to terminate TLS at the public wallet endpoint. It sends
manifest and control routes to the coordinator, and queries to a separate
request-router process. The router reads the launched protocol's fixed
query-binding prefix (`EPQ4` in the investigated framing), selects an eligible replica from an atomically loaded placement revision, and
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

In the September 23, 2026 baseline, the coordinator buffered every 282 KB body,
decoded it, forwarded to one replica at a time, packed the result, and enforced
a fleet-wide slot count. Its sampled cgroup peak was about 8 GB. Its compute and memory ceiling does not grow
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

### D10. Pool placement with a replication factor instead of rigid replica pairs

Place each query domain on `r` workers from a pool, with `r = 2` by default and
a higher factor allowed for the frontier. Per-domain placements replace groups;
the reservation ledger and persisted operation phases carry over.

Rigid pairs require growth in units of two machines and give a hot frontier the
same replica count as an idle sealed shard. A pool allows finer growth, better
utilization and extra replicas for hotspots. Two copies still provide roughly
50 percent raw storage efficiency. This is the lowest-priority change.

## Routing and recovery prerequisites

The router must enforce the launched routing revision and recovery epoch checks,
including stale-view and invalidated-session errors. Workers validate the exact
session and pin its evaluation and packing resources. Responses retain the
launched binding; recovery revocation must fence affected responses even when
work was admitted before revocation.

Before cutover, define publication and acknowledgment ordering across coordinator,
routers and workers, how disconnected or stale participants lose serving
authority, and restart behavior. An atomic local snapshot swap alone does not
make every router current. Test ordinary routing switches separately from
recovery revocation, where affected in-flight responses must be fenced.

The `EPQ4` reference above describes the investigated framing. The implementation
must read the binding prefix of the launched protocol without introducing a new
wallet framing requirement solely to relocate packing.

## Memory ownership and qualification

Workers inherit the immediate proposal's database, retained revision, tail-unit,
preparation and query-pin charges. Add packing state for every retained domain
and the complete public-request lifetime: body reception, decoded keys,
evaluation intermediates, transient packing allocations and buffered responses.
Account for concurrent publication and retain charges while work continues after
client cancellation. A fixed overhead or evaluation semaphore is not evidence
that these allocations fit.

Bound router bodies retained for replay, connections and output buffering
separately. Initially charge them, Caddy and TLS/network overhead to the
coordinator host. Keep the existing public query path until the
[worker-serving measurement gate](worker_serving_investigation.md#production-measurement-required-before-the-cutover)
passes with exact-answer checks, slow uploads/readers, disconnects, replica loss,
retained-session expiry and concurrent publication. No worker-packing capacity
claim is established by this proposal.

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

Report successful QPS, latency, public 429/502/503 responses, worker rejection rate,
coordinator publication responsiveness, coordinator-host/router peak memory and
network throughput, and worker peak memory.

### Phase 2: pool placement

Replace groups with per-domain placements and a replication factor. Validate
consolidation and hotspot replication under the existing operation phases,
with the ledger counting both copies at the default factor.

For pool placement, also verify interrupted moves and controller restart under
the persisted operation phases, exact-session readiness before placement
publication, and reservations for both source and destination during moves.

## Resulting architecture

The coordinator retains ingestion, publication and placement control, while the
router forwards opaque public queries and workers evaluate and pack. Pool
placement permits independent replication of hot domains. Initial router
co-location retains the public origin's host and network failure boundary;
origin high availability requires separate future deployment work.
