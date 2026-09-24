# Enhance PIR v7 architecture

This is the implemented architecture for `ironwood-enhance-pir-v7`, schema 11.
The accepted design is [architecture_update.md](architecture_update.md).
Deployment evidence and remaining hardware qualification are tracked separately;
implementation is not a capacity certificate. The [v6 design](archive/architecture-v6.md)
is retained for historical context.

## Data and query path

Each commitment-tree position identifies a 653-byte suffix record. There are
33 records per row and 32,768 rows per fixed storage shard: 1,081,344 records.
Storage shard `i` owns `[i * 1,081,344, (i + 1) * 1,081,344)` forever. There
are no loans, returns, lender counts, or settling assignments. Wallets support
at most 24 storage shards, including exactly 24 full shards.

The query path is wallet → Caddy → coordinator → worker evaluation → coordinator
packing → wallet. The coordinator owns routing, admission, query decoding and
response packing. Workers evaluate singleton queries. Whole domains are assigned
to replica groups; each replica holds the full assignment. A separate packing
router and pooled placement remain deferred to [the later proposal](architecture_update_2.md).
This revision makes no throughput improvement claim.

## Composed domains and canonical routing

A successor B with fewer than 4,096 occupied rows has an 8,192-row query domain.
B's owned rows start at local row 0; its predecessor A's final 4,096 rows appear
at local row 4,096. Unoccupied owned rows are zero padded. A keeps its complete
storage and query domain throughout. The canonical manifest routes A's suffix
through B while the composition is active. Clients do not choose between two
routes for the same position.

At the first occupied record in B's 4,096th row, B uses its plain owned domain
and A's suffix routes back to A. This threshold uses occupied rows, not a count
of completely filled rows. Manifest routes are validated against the exact
geometry, coverage, local offsets and lifecycle; gaps, overlap and noncanonical
alternatives are rejected. A single ingested block can cross multiple boundaries.
Publication follows the complete block and never exposes an intermediate boundary.

Progressive frontier units retain the 8,192-row cap. Tail units refer to the
predecessor's source rows without transferring storage ownership. Their hashes
commit to the padded bytes and their recovery epoch.

## Confirmation and recovery

A shard is growing, full but provisional, or sealed. Sealing requires 1,000
canonical successor blocks after the block that completes the shard. Confirmation
alone does not change its session or padded contents. Provisional full shards
count toward active placement until confirmation. The existing six-sealed-shard
policy remains the default; seven requires separate hardware qualification.

Ordinary rollback affects provisional coverage and advances routing. It preserves
sealed sessions. A deep rollback that invalidates a seal durably increments the
recovery epoch and revokes affected and downstream session identities before
journal truncation or replay. Retrying the same pending rollback does not increment
the epoch twice. Unaffected domains retain their own recovery epochs.

Workers persist revocations, reject revoked sessions and drop revoked runtime
assignments. Every future reservation requires the worker to acknowledge the
current revocation set, so a disconnected replica cannot silently rejoin with stale
state. Coordinator publication refuses to commit across a concurrent recovery
fence. The coordinator checks revocation again after packing and before returning
a response. Requests already evaluating retain their resource pins until completion,
but their results cannot cross the final revocation fence.

## Session identity and routing freshness

`generation` is the routing revision. It binds a query to the current accepted
anchor and routing table. `placement_revision` changes with actual ready replica
routes. Neither field contributes to the content-derived session ID.

A session ID commits to the protocol, domain, domain recovery epoch, logical row
count, owned record count, ordered padded unit commitments and packing material.
See [the wire contract](protocol.md) and its independent frozen vector. Identical
material is reused across routing and placement updates. Deep recovery changes
identity even when replay reconstructs identical bytes.

Clients refresh routing on a 30-second cadence and on structured 409/410 errors.
The wallet must independently accept each new anchor using scanned chain state.
Only then may it rebind cached material. Each query uses fresh PIR randomness and
a fresh request ID. Responses must match routing, domain, packing material,
recovery epoch, session, request and anchor.

## Resource ownership

Worker ledgers account for resident runtimes, candidates, request work, pinned
retained resources and the 96 MiB tail-construction reserve, alongside the existing
frontier reserve. Reservations precede allocation; cancellation does not release
resources still owned by admitted CPU work. These estimates are admission controls,
not substitutes for Linux RSS and cgroup measurements.

Coordinator packing uses a separate admission budget. Each resident packing object
is charged 768 MiB and construction reserves another 1 GiB. The default budget is
24 GiB, capped by the physical/cgroup limit after a 1.5 GiB host and request reserve;
`ENHANCE_COORDINATOR_MEMORY_BYTES` can lower or configure it. Cached identical domain
keys share packing objects after restart. Retired/revoked objects are removed from
the publication cache while outstanding requests retain their pins. Health reports
expose charged memory.

## Wallet privacy and trust

PIR hides the row within the selected public domain under the existing q48 IPIR
assumptions. This lifecycle change does not introduce a new cryptographic primitive
or upgrade the underlying PIR security claim. Session hashes and response bindings
prevent accidental cross-context acceptance; they are not proof of canonical chain
membership or authentication of indexer metadata. Canonical anchor acceptance and
note authentication remain wallet responsibilities.

Default operation exposes selected domains, timing and query counts. Optional
birthday-based cover traffic queries every domain since the wallet birthday with
uniform round counts, fresh dummy queries and randomized order. A transient failure
retries a complete round; partial round results are not returned. Cover is off by
default and does not conceal timing, the birthday window, transport identity or
server availability. See [wallet integration](integration.md).

## Validation and deployment

The validation suite includes independent server/wallet session vectors, malformed
bindings and routing, composition thresholds, maximum coverage, canonical sealing,
ordinary and deep rollback, an admitted-response recovery fence, worker restart,
real HTTP wallet reuse/cover, and transactional SQLite application. The focused
Linux workload exercises composition growth, tail removal, frontier expansion and
rewind using independent process/cgroup sampling. Its 30-minute run does not replace
the outstanding six-hour hardware qualification.

SSH rollout uses fresh v7 controller and worker directories. A stopped, validated
schema-11 canonical journal can be copied from v6; caches and publications must be
rebuilt. Old binaries, service overrides and data are preserved for rollback. The
protocol is intentionally incompatible with v6 clients. Deployment and measured
results belong in [deployment](deployment.md) and [qualification](qualification.md).
