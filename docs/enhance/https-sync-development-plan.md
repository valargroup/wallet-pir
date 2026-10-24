# Enhance HTTPS synchronization development plan

Status: implementation in progress. Development proceeds locally as cumulative
draft PRs against main, merged by the operator in dependency order. This document
does not establish the state of production.

## Outcome and ownership

Enhance workers, packing routers and query ingress independently follow complete
publications from the configured publisher over HTTPS using `pir-sync-manager`.
The publisher prepares serving material once and publishes it without contacting
consumers or waiting for their readiness. Wallet requests, sessions, cryptographic
parameters and record encoding retain their existing contracts.

Each serving role owns compatibility checks, downloads, loading, local activation
and refusal. Packing routers additionally own selecting workers that can evaluate
the exact data and setup bound to a request. Query ingress owns advertising and
routing only views supported by its downstream packing routers. There is no
publisher-driven activation barrier, consumer acknowledgment requirement, or
publisher-managed rollout of consumer software.

Success means a publisher can advance while consumers update independently,
without serving a request against different rows, setup or session bindings.

## Scope

Implement prepared publication, autonomous synchronization for all three Enhance
serving roles, local handover and recovery, focused correctness tests, and the
configuration and operator changes needed for a direct cutover on existing hosts.
Reuse the shared HTTPS transfer and polling library and Enhance's existing
prepared-artifact loaders. Keep product policy in Enhance.

Do not build a generic cross-product activation controller, dynamic fleet
membership, a new wallet protocol, publisher-managed consumer upgrades, new
machines, a second serving runtime, compatibility windows, shard repartitioning,
or a new publisher-signing protocol. The deliverable includes production cutover
and verification after the complete path is qualified and the operator confirms
the concrete deployment plan. Ownership-only shard promotion remains governed by the existing
[shard promotion specification](../spec/spec-shard-promotion.md); this work must
support its independent handover rules without implementing a new promotion policy.

## Current implementation and reuse

| Existing code | Current responsibility | Planned change |
|---|---|---|
| [`publisher.rs`](../../src/enhance/enhance-publisher/src/publisher.rs) | Durable generation/recovery ownership and complete local preparation | Publishes without consumer destinations or acknowledgments |
| [`distribution.rs`](../../src/enhance/enhance-publisher/src/distribution.rs) | Atomic current, immutable objects and real canonical authority | Read-only HTTPS distribution; withdrawals fence pending work and responses |
| [`worker_sync.rs`](../../src/pir-query-server/src/enhance/worker_sync.rs) | Verified row loading, backend admission and exact readiness | Autonomous worker activation and bounded peer pins |
| [`router_sync.rs`](../../src/pir-query-server/src/enhance/router_sync.rs) | Published packing, compatible workers and exact evaluations | Local handover, byte-budget admission and response fences |
| [`ingress_sync.rs`](../../src/pir-query-server/src/enhance/ingress_sync.rs) | Manifest/routing activation with compatible routers | Separate control/query origins and retained request bindings |
| [`prepared_packing.rs`](../../src/enhance/enhance-shared/src/prepared_packing.rs) | Verified persisted/mapped packing | Reuses the existing format and bounded loaders |
| [`service.rs`](../../src/pir-query-server/src/enhance/service.rs) and root hosts | Configure/supervise one role | Requires synchronization settings; validates actual role JSON without opening state |
| [`pir-sync-manager`](../../src/shared/pir-sync-manager/src/lib.rs) | HTTPS transfer accounting, sequential polling and backoff | Enhance authority, recovery and activation policy stays product-owned |

The current native and reference builds have different prepared-packing formats.
A successful HTTP transfer or an equal source commit does not establish format
compatibility. Existing verified file handles, memory mapping, memory budgets and
in-flight ownership should remain the loading foundation.

## Publication contract

Define an Enhance-owned, bounded distribution schema with a product-specific
HTTPS prefix, provisionally `/provider/enhance/v1/`. Keep distribution versions
separate from existing wallet generations and session identifiers.

The current pointer identifies a durable publication revision and inventory
hash, network, supported product/profile identity, recovery authority and
eligibility. The immutable inventory describes:

- Coverage, shard/domain membership, existing revisions and session bindings.
- Ordered database units, their geometry, content hashes and byte lengths.
- Prepared packing objects, public parameters, format versions and hashes.
- The data/setup relationships needed to prove that worker evaluation and router
  packing belong to the same published view.
- Existing product authority and revocation metadata, or references to the
  bounded authority response needed to establish it.

Use existing identities where they already bind these facts. Add fields only
with validating producer and consumer code. Keep deployment placement and peer
addresses in operator-owned consumer configuration; the publisher does not need
a consumer inventory to produce a publication.

Finish, hash, sync and atomically install every referenced artifact before
advancing the current pointer. An unchanged unit or sealed shard keeps its
content identity and is reused across publications. Current/authority responses
must never extend the validity of stale source observations merely because a
poll, heartbeat or conditional request succeeded.

New consumers bootstrap from the current publication. If that target is
superseded or an object disappears during loading, reconcile current and retry;
never activate an incomplete candidate. Existing consumers can retain a valid
active view locally while following the new current publication. Publisher
advancement alone is not revocation. Immutable objects still referenced by the
current inventory remain distributable regardless of their creation revision.

## Consumer compatibility and handover

Each role tracks the publisher's current publication, its own candidate and its
active view separately. Reject unsupported schemas, formats, geometry, networks
or profiles before expensive allocation. Verify lengths and hashes before
loading and validate the relationships between rows, setup, packing and sessions.
Persist monotonic publication/recovery floors and invalidations before allowing
new service under them. On restart, cached bytes are reusable material, not
permission to serve; reacquire current publisher authority.

Worker readiness identifies the exact loaded domain/unit revisions, setup and
recovery identity, together with its process incarnation. It must describe
material that is actually admitted for evaluation, not merely downloaded.
A packing router checks that readiness before advertising a candidate and binds
each evaluation to the requested identity. Ingress similarly checks router
readiness for the sessions and coverage it advertises.

Compatibility is about exact required material and authority, not necessarily
identical whole-publication counters: an unchanged shard may be usable across
successive inventories. Define that predicate once and test it. A newer worker
must never reinterpret an older request using its newer rows or setup.

A router or ingress may switch only after its local material and required
serving destinations are ready. Activate its advertised metadata and routing
view atomically. Preserve active and already-admitted request bindings during
handover. Use bounded pins or an equivalent consumer-to-consumer handover so a
worker/router cannot evict material still promised upstream. Release those pins
on completed handover, expiry or invalidation; preserve existing capacity and
backpressure limits. If a peer restarts or loses readiness, retry a compatible
peer or return the existing refusal; never substitute incompatible material.

No role waits for publisher permission to activate a fully validated candidate.
The publisher neither receives consumer acknowledgments nor chooses their
activation time.

## Timing and authority

Use the shared manager's existing successful-poll cadence and bounded error
backoff. Treat an available peer still loading as deferred coordination rather
than a transport failure. Keep passes sequential; do not add overlapping timers
or a second product-specific retry loop.

Audit the existing Enhance refresh watchdog, source validity, session expiry and
recovery fences separately. Replace publisher-pushed liveness refreshes with
consumer-obtained authority while preserving their refusal guarantees. Document
which existing durations have an externally observable contract and which are
internal scheduling choices. Align scheduling with Transparent where possible;
do not copy Status's twenty-second observation limit into Enhance or change
wallet/session validity as an incidental synchronization refactor.

An ordinary outage or failed candidate load preserves an otherwise valid active
view. Explicit revocation, disconnected source authority, expired validity, or
failure to persist safety state must close the affected admission path. Recheck
authority after slow transfers/loading and immediately before activation. Retain
post-evaluation and response-release checks so an in-flight result cannot escape
a revocation that occurred during its computation.

HTTPS authenticates the configured TLS endpoint; object hashes bind bytes.
This plan does not claim independent end-to-end publisher signatures.

## Development phases

Start with phase 1: make the publisher produce a complete, verifiable publication
with every consumer stopped. This proves the ownership boundary before adding
network synchronization. Each phase targets one focused PR with an observable
result; split further only when each smaller change has its own working consumer
or test. Do not introduce unused schemas or configuration ahead of their use.

| Phase | Increment | Completion gate |
|---|---|---|
| 1 | Complete local publisher artifacts | Produce and load a complete publication with no consumer processes or outbound preparation requests |
| 2 | HTTPS publication distribution | Fetch and verify a complete publication over real TLS; incomplete publication is never current |
| 3 | Autonomous worker | Bootstrap, update and restart from publisher publications without publisher commands |
| 4 | Autonomous packing router | Load published packing and activate locally only with compatible worker readiness |
| 5 | Autonomous ingress and removal of push control | Complete wallet flow works with independently updating consumers and no publisher orchestration |
| 6 | Qualification and cutover preparation | Resource, hardware, recovery and independent-answer evidence supports a reviewable direct cutover |

Phases 1–2 establish the publication contract. Phases 3–5 move synchronization
and activation to consumers. Intermediate changes are validated locally; deploy
only after the complete path is ready. Incremental PRs do not require two
production synchronization modes or a compatibility window.

### 1. Produce complete local publisher artifacts

The first PR extracts/reuses the existing local hint and packing constructors in
the publisher and produces a complete publication without contacting a worker
or packing router. Introduce the product-owned inventory with the code that
writes and loads it. Bind ordered database units, geometry, prepared packing,
public parameters, formats, hashes and lengths to the existing domain and session
identities. Reuse unchanged units and domain material.

Define and test the material-compatibility predicate, including the distinction
between publication counters and unchanged shard identities. Inventory existing
wallet identity, revocation and expiry checks and map each to its intended owner.
Keep query-serving packages free of chain-reader and publisher dependencies.
Reject malformed geometry, unsupported formats and inconsistent identity bindings
before expensive allocation.

Validate this increment through existing loaders: produce the artifacts with
all consumer processes stopped, load worker and router material in the fixture,
and check an encrypted answer against an independent expected record. Verify
that unchanged material keeps its identity and is reused. Exercise the applicable
reference and native formats with small focused fixtures.

Measure cold and incremental preparation time, peak RSS and scratch disk early.
Full production sizing belongs to phase 6, but an impractical publisher footprint
must be discovered before building the consumer integration. Keep acquisition
and authority observation responsive during preparation through bounded blocking
work and the existing supervised pipeline. Worker/backend database construction
on a query host is allowed; computing publisher hints or packing there is not.

This first increment does not add HTTPS routes, consumer polling, peer handover,
activation coordination or deployment changes. Remove dependencies on remote
preparation from the extracted producer; do not add a selectable production push
mode to make the increment deployable in isolation.

Done when a complete publication can be produced and verified without consumers,
its encrypted answer matches the independent oracle, unchanged material is
reused, and initial preparation resource measurements are available.

### 2. Distribute complete publications over HTTPS

Serve bounded current/authority metadata and streamed immutable artifacts through
the root publisher host. Specify metadata limits, per-object lengths and total
transfer budgets using the phase 1 formats. Persist the inventory and atomically
advance current only after every referenced object is durable and canonical
validation succeeds. A preparation failure keeps the previous valid publication
and cannot grant fresh authority to stale data.

Implement the publisher's pull-based authority and recovery contract without
consumer registration or acknowledgments. Test replay/recovery transitions,
withdrawal, unchanged responses and superseded bootstrap targets. Use a real TLS
fixture to fetch and verify the full inventory and referenced material. An
interrupted publication must never expose an incomplete current target.

Done when a generic test consumer can retrieve a complete, authorized publication
over HTTPS and verify its bytes, while publisher progress remains independent of
consumer availability. No production serving role switches in this phase.

### 3. Add autonomous worker synchronization

Wire the shared manager into worker lifecycle configuration. Download only
assigned material, verify it, reuse unchanged units and construct the configured
CPU/CUDA evaluation backend without hint generation. Persist monotonic
publication/recovery floors and invalidations, activate locally and expose exact
loaded-material readiness and process incarnation. Handle interrupted transfers,
exclusive state ownership, restart, storage failure and retention backpressure.

Test bootstrap, incremental advancement and restart without publisher commands.
Cached files cannot authorize service after restart. Unsupported formats,
incompatible identities and invalidated material must be refused. A failed load
must preserve a still-valid active view.

Done when a worker independently follows publisher changes and recovers safely,
with precise readiness that a router can use in the next phase.

### 4. Add autonomous packing-router synchronization

Load verified published packing through the existing mapped loader. Implement
consumer-owned worker readiness checks and bounded handover, then atomic local
activation. Bind every evaluation to the requested data, setup and recovery
identity. Never reinterpret a request using a worker's newer material.

Preserve wallet framing, sessions, query admission, routing privacy and
response-release fences. Test workers and routers starting and advancing in
different orders, a slow or unavailable replica, peer restart and a lost handover
response. Waiting for a loading peer uses the ordinary successful-poll cadence.
The old valid view remains available while its replacement loads; pins and
capacity backpressure prevent eviction of material still promised upstream.

Done when the packing router serves encrypted queries through compatible workers
and activates independently, without a publisher-issued activation barrier.

### 5. Add autonomous ingress and remove push control

Make ingress pull publication metadata and check router readiness for the exact
sessions and coverage it advertises. Activate its manifest and routing view
atomically. Preserve existing wallet endpoints, upload limits, stale-session
behavior and in-flight request bindings. Test the complete wallet flow before,
during and after independent consumer updates.

Delete outbound publisher control, production hint-fetch/router-preparation
round trips and their obsolete handlers, config fields, fixtures and tests.
Replace them with tests of the new behavior; do not retain an unused push mode.
Keep private evaluation, readiness, metrics and required peer handover routes.
Local tools and fixtures must exercise the same ownership boundary.

Update root host configuration validation, role templates, examples and the
existing deployment tool. Consumers need the publisher HTTPS origin, optional
trusted CA, their local responsibilities and configured peers. Preserve private
listeners, credentials, process limits and existing host identities. Update the
architecture, deployment and serving-contract docs to describe the resulting
behavior rather than leaving conflicting push-control instructions.

Done when the complete wallet path works with independently starting/updating
consumers, static configuration and dependency-boundary checks pass, and the
publisher has no dependency on reaching a consumer control endpoint.

### 6. Qualify and prepare direct cutover

Retain focused correctness evidence, then run hardware and deployed-size
qualification separately. Prepare a locked plan/preflight/deploy/status/rollback
path through `tools/scripts/wallet-pir-deploy.py`, pinning exact artifacts,
configuration and baseline service identities. Verify the actual live topology
at execution time; source examples do not establish production readiness.

Use existing hosts and one direct cutover. Keep the previous executable/config
and the safety state needed for recovery; rollback must not lower new monotonic
floors or erase invalidations. If old and new state contracts differ, use
separate state directories and define rollback's authority/recovery behavior
explicitly. Reject reactivation that would restore invalidated authority.

Start the one-hour recovery window at the first serving interruption, recorded
conservatively immediately before the first interrupting action. If readiness,
fresh publication advancement and independent encrypted answers for all affected
products have not passed within it, stop forward progress and begin rollback.
Restoration may finish after the hour. Retries and partial recovery do not reset
the clock. A persistent, transaction-owned watchdog must execute recovery even
if the invoking shell disappears; a saved deadline alone is not an executing
rollback. Serialize the commit/deadline decision under the production lock.
Because the publisher process is shared, verify all enabled products after both
cutover and restoration. Production deployment remains a separate operator gate.

## Acceptance evidence

| Scenario | Required observable result |
|---|---|
| Publisher with no reachable consumers | Publishes complete material without waiting for acknowledgments |
| Cold bootstrap over real TLS | Each role verifies and loads before advertising readiness; unchanged wallet retrieves independent expected records |
| Incremental publication | Only changed material is transferred/prepared; unchanged shard identities and setup remain usable |
| Independent updates and slow peer | Existing valid service continues; local activation waits for compatible downstream readiness |
| Mismatched revision, setup, profile or native format | Refused before serving; no evaluation against substitute rows or keys |
| Corrupt/truncated object or superseded candidate | No completed corrupt cache entry or partial activation; retry follows current |
| Publisher outage or unchanged response | Valid active view survives only within existing authority limits; no artificial freshness renewal |
| Reorg or explicit revocation during loading/evaluation | Durable invalidation closes affected service and guards response release |
| Consumer restart, peer restart or lost handover response | Fresh authority and exact incarnation/readiness checks; bounded pins; no stale restored service |
| Storage failure, capacity pressure or cleanup | Refuse unsafe activation; retain safety records and in-flight material; memory/disk remain bounded |
| CPU/reference and native/CUDA configurations | Applicable loaders and encrypted answers pass; hardware-only claims have hardware evidence |
| Cutover failure or recovery-window expiry | Restore verified executable/config, preserve safety state and independently verify all enabled products |

Keep CPU correctness fixtures small and individual tests below the repository's
thirty-second budget. Run focused package tests and affected fast/hygiene checks;
use real TLS boundaries, an independent plaintext oracle, negative identity cases
and restart tests. Publish the reviewable change before expensive qualification,
monitor its exact-head CI, and report pending gates accurately. Fast CI is not
publisher capacity, GPU, sustained-load or production qualification.

The implementation is ready for cutover when all three serving roles synchronize without
publisher orchestration, old production control paths are removed, focused
acceptance and CI pass, and the operator cutover is reviewable. The deliverable is
complete only after deployment and sustained production qualification pass with
separate evidence. A recovered failed attempt does not complete the deployment.
