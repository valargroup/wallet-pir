# Decentralized PIR query serving

This is a proposed architecture and phased implementation plan. It builds on
the existing services; it does not establish deployment, capacity or external
operator qualification. The [serving contract](serving-contract.md) and product
contracts continue to govern protocol behavior and trust.

## Goal

Let independent operators expose complete wallet-facing PIR endpoints without
running indexers or performing heavy publication construction. A few capable
organizations, potentially ValarGroup and Stardust, can run indexers and builders.
Many providers can serve recent data, where operator diversity and query
performance matter most. Fewer providers can maintain archive coverage.

The default external deployment embeds a router and worker in one serving
process. Operators can also run routers and workers separately when independent
scaling or hardware placement is useful. Both deployments use the same routing
and execution components.

A wallet can use a provider's endpoint directly. Initialization, manifests,
setup and queries must not require a shared query ingress operated by us.
Publication authority and distribution remain separate dependencies: distributing
query endpoints does not itself remove trust in indexers or guarantee publisher
availability.

## Responsibilities and boundaries

| Responsibility | Owns |
|---|---|
| Indexer and builder | Chain reads, record extraction, durable indexing, publication construction and heavy PIR preprocessing |
| Publication distributor | Authenticated revision discovery and immutable artifact delivery, directly or through mirrors |
| Local publication manager | Synchronization, verification, activation, retirement and serving eligibility for the operator's holdings |
| Router and worker | Wallet-facing API, coverage routing, query admission, encrypted evaluation and response packing |
| Service discovery | Provider announcements and candidate selection by product, protocol and actual coverage; separate from serving authorization |

```mermaid
flowchart LR
    I["Indexers and builders"] --> D["Publications and mirrors"]
    D --> R1["Provider A: router + recent worker"]
    D --> R2["Provider B: router + recent worker"]
    D --> A["Provider C: router + archive worker"]
    W["Wallet"] --> R1
    W --> R2
    R1 -->|"Configured archive coverage"| A
    R2 -->|"Configured archive coverage"| A
```

Storage policy and execution topology are independent choices. Recent, archive
and combined holdings apply only where the product supports their semantics.
Enhance and Status packing routers perform computation, beyond forwarding HTTP;
their preparation, packing material and resource use belong in the provider's
requirements alongside evaluation.

Reuse these foundations:

- [External worker roles](operations.md#external-workers) provide product-owned
  configuration, installation and readiness reporting.
- [Transparent architecture](../transparent/docs/architecture.md) separates
  publishers and query workers and describes recent/archive assignments and
  prepared runtimes.
- [Enhance architecture](../enhance/docs/architecture.md) separates ingestion,
  ingress, packing and evaluation.
- [Status architecture](../enhance/docs/architecture_status.md) defines rolling
  observations and their freshness and authority rules.
- [Receiver](../receiver/README.md) has a staged-publication serving mode without
  indexing, with canonicality checks and explicit publisher-completeness trust.

These components do not yet establish autonomous provider operation. In
particular, existing controller-managed activation and optional local rebuilding
need explicit treatment before a node can promise lightweight operation.

## Implementation phases

### Phase 1: establish reusable service boundaries

Complete and reuse the service modularization already underway. Separate
ingestion, publication preparation, serving state and query execution. Keep
protocol definitions and cryptographic implementations in their existing owning
libraries, and keep release dependencies one-way from wallet-pir to
wallet-libraries.

Define routing and execution interfaces usable through local calls or remote
workers. The embedded path must preserve validation, authority, admission,
cancellation and response ownership. Keep product-specific limits, retention,
freshness, refusals and retries explicit.

**Deliverable:** independently buildable serving libraries with dependency checks
that exclude ingestion and other products' service implementations, including
development, build and target-specific dependencies.

**Exit criterion:** a serving installation has documented inputs and authority
requirements, and embedded and decoupled execution can compose the same components.

### Phase 2: publish independently consumable serving material

Turn publisher output into a documented distribution interface. Provide
authenticated revision discovery identifying coverage, required runtime/profile
versions, artifact identities and serving eligibility. Reuse existing product
manifests and runtime formats rather than creating a universal PIR format.

Distribute immutable, checksum-bound files. Support initial snapshots and
incremental synchronization by reusing unchanged artifacts. Include the material
needed for evaluation and response packing. A mirror may deliver identical bytes
without gaining authority to publish or declare them current.

Use request/response over HTTPS for the first synchronization protocol. Query
servers poll a small authenticated current-state record, fetch an immutable
manifest when its publication changes, and download only missing artifacts.
Support conditional requests, bounded timeouts, jitter and retry backoff. Choose
refresh intervals from each product's freshness budget, allowing time for transfer,
verification and loading; do not impose one interval on every product.

Current-state reconciliation must let a node recover after downtime without
receiving every intermediate update. It includes the publication and applicable
revocation/recovery state. Deltas require an exact verified base; a full snapshot
remains available when that base is missing. A failed poll or unchanged HTTP
response must not extend expired serving authority.

Keep artifact retrieval, verification and activation independent of how an update
is detected. Subscriptions or streaming notifications may later wake the same
reconciliation path if measurements justify them. They are outside the first
version, which requires no subscriber registration or publisher-owned per-node
delivery queue.

Preserve product-specific reorg, revocation and freshness behavior. Publication
identity is separate from download location; downloaded material is separate from
authority to serve it. Products that currently require controller renewal retain
that requirement through an authenticated control path. A feed update alone must
not silently grant indefinite authority.

#### Hierarchy generation handover

Once a publisher has built and finalized an amended hierarchy, it atomically
advances its current generation and stops offering the previous generation as a
publication/bootstrap target. New query servers bootstrap against the current
generation. Existing query servers detect it through polling and initiate their
update. Unchanged artifacts referenced by the new generation remain reusable.

Each query server keeps its old generation active, including the metadata and
setup needed by wallets, while downloading, verifying and loading the replacement.
It switches the router and worker's serving view atomically only when the new
generation is ready. Discovery continues to advertise its actual active generation
until that switch. A superseded download whose artifacts are no longer available
reconciles current publisher state and retries without stopping the active view.
Before activating a candidate, check that it has not already been superseded.

Routine supersession is not revocation. Retiring the publisher's old generation
must not by itself invalidate query servers still serving it during a normal
handover. Preserve product-specific authority renewal, freshness and reorg checks;
confirmed invalidation or expired authority may still require refusal. Already
admitted requests retain their generation under existing request-lifetime rules.
After a switch, stale wallet sessions use the product's existing refresh/refusal
behavior; never interpret an old encrypted request against new-generation data.

The normal handover has no gap in the node's serving readiness. Budget the
candidate alongside the active material so preparation cannot evict the serving
view. Treat hierarchy generations separately from routine per-block publication
revisions, which retain their product-specific behavior.

**Deliverable:** a publication source interface and verifier that operate without
our source checkout, private paths, indexer credentials or SSH deployment machinery.

**Exit criterion:** another organization can obtain and verify every artifact
needed for an exact publication, including compatible prepared runtime material.

### Phase 3: deliver the default embedded provider

Pilot with Transparent recent serving, which already has recent/archive
assignments and prepared-runtime mechanisms. Provide one operator-managed
installation with an embedded router and worker and a complete wallet-facing
endpoint. The operator configures the service, holdings, approved publisher,
resource budget and archive peers, and owns TLS and upgrades.

A local publication manager follows the configured holdings independently of
wallet requests. It downloads, stages and verifies candidates, then activates them
atomically while continuing to serve the previous valid generation. Interrupted
downloads and restarts resume safely without making partial publications available.

Lightweight mode requires compatible prepared material. Missing or incompatible
material produces a clear preparation refusal instead of silently starting an
expensive local build. Keep the last valid publication only while its existing
freshness, authority and revocation rules allow it.

Keep administrative interfaces private. Joining must not require our root access
or a fleet-wide credential. Avoid requiring a local archival chain node, while
preserving access to an appropriate canonicality source wherever the product
requires it.

**Deliverable:** an embedded provider installation with synchronization, bounded
resource use, restart recovery and separate process, verification, warmth,
authority and readiness reporting.

**Exit criterion:** an independent operator bootstraps, follows updates and serves
encrypted wallet queries directly within measured resource requirements, without
our administrative access or heavy local preprocessing.

### Phase 4: route across recent and archive providers

Use the embedded worker when it holds the exact requested publication and
qualifies to serve it. Route other coverage to explicitly configured providers
that support that publication and profile. Registration alone does not establish
loaded coverage, readiness, authority or encrypted-answer correctness.

Preserve encrypted framing, session binding, response validation and existing
retry restrictions, including uncertain evaluation acceptance. Bound forwarding
and prevent loops. Missing local data or unavailable archive coverage must never
become a successful empty result or an implicit public lookup.

Support separate routers and workers through the same interfaces. The operator
can move evaluation or packing to other hardware without changing the wallet API.
Optional shared ingress remains possible, but is unnecessary for direct operation.

**Deliverable:** coverage routing that supports local recent execution and remote
archive execution, with explicit refusal and peer-removal behavior.

**Exit criterion:** a wallet configured against one independent recent provider
can query its local recent holdings and configured archive coverage. Losing a peer
or receiving stale assignment information produces bounded failures under the
existing protocol.

### Phase 5: discover providers through capability announcements

Define a versioned service discovery protocol so providers can announce their
identity, endpoints, products, protocol support and available coverage. A provider
may serve Transparent alone or several products. Describe each capability
separately: supporting Transparent history does not imply support for transaction
display, Enhance, Status or Receiver.

Distinguish software capabilities from data that is loaded and eligible to serve.
A useful announcement says more than "recent from height X": it identifies the
network, protocol/profile, current publication and covered-through anchor, and
states any gaps or multiple covered ranges. Separate locally held coverage from
coverage the embedded router offers through peers. A recent node must not claim
to hold archive data merely because it can forward archive queries.

Use signed, expiring announcements bound to an operator identity, with a
monotonic announcement revision and explicit withdrawal. Update them when
publications, coverage or readiness change, including after a reorg; renew them
while the provider remains available. Use existing vetted signing libraries.
Announcement freshness never replaces a product's serving-authority checks.

Start with capability documents fetched over HTTPS from configured providers and
multiple bootstrap directories. Direct endpoint configuration remains supported.
Directories index announcements rather than authorize publications; no single
directory is required for established providers to keep serving. Cache verified
announcements and refresh discovery independently of individual wallet queries.
Permissionless admission, a DHT and gossip are not prerequisites.

The router filters candidates by operator policy, network, product, protocol and
exact publication coverage, then applies readiness and encrypted-answer checks.
A valid signature proves who made a claim, not that the advertised data is
complete, current or correctly served. Unverified or expired announcements cannot
add eligible routes. Discovery does not grant administrative access or allow
operators to choose a different wallet protocol silently.

**Deliverable:** provider announcement publication, verification, refresh and
withdrawal, plus router candidate selection. Existing wallets may continue using
a configured provider endpoint; wallet-side automatic discovery is separate work.

**Exit criterion:** two independent providers with different product and height
coverage are discovered and selected correctly, including when a provider changes
coverage or disappears and a bootstrap directory is unavailable. Claims alone
cannot make an unqualified provider eligible for queries.

### Phase 6: add publisher diversity and broaden service support

Introduce a second independent publisher after qualifying the first provider
workflow. Distinguish a mirror of identical bytes from an independently built
publication lineage. Keep manifests, runtime material and serving authority bound
to the selected lineage. Publisher switching is deliberate and validated; never
mix artifacts or sessions merely because their block ranges overlap.

Use the distribution machinery for archive holdings. Demonstrate that qualified
archive providers retain required coverage before recent providers retire local
copies. Extend the provider model to the remaining products using their existing
coverage semantics.

Publish reproducible installation instructions and measured requirements for
supported hardware variants. CPU qualification does not establish GPU readiness.

**Deliverable:** multiple independently operated publishers and serving endpoints,
with operator-owned configuration and documented coverage and resource requirements.

**Exit criterion:** demonstrate publisher outages and switching, mirror failure,
provider replacement and recent-to-archive continuity without changing wallet
protocols.

## Product coverage

| Product | Coverage constraint |
|---|---|
| Transparent history/display | Preserve each product's recent/archive boundaries and publication identities |
| Enhance | Recent holdings may cover a bounded set of query domains; older domains need qualified archive routes |
| Status | Preserve rolling observations, mempool coverage and freshness; archive mode is not an equivalent service |
| Receiver | Preserve complete history from Ironwood activation; truncating recovery history is outside this plan |

## Interfaces and validation

Add interfaces with their consumers in the owning phase:

- **Publication source:** authenticated revision discovery and immutable artifact
  retrieval through request/response HTTPS, initially using polling and conditional
  requests for current state.
- **Local publication manager:** synchronization, validation, activation,
  retirement and status.
- **Execution interface:** equivalent serving semantics for embedded evaluation
  and remote workers.
- **Coverage routing:** operator-controlled destinations qualified for exact
  publications.
- **Service discovery:** signed, versioned provider announcements and a candidate
  source for routing, without replacing publication validation or authority.

### Discovery capabilities

Each announcement binds an operator identity and announcement revision to its
issue/expiry times and advertised endpoints. Each product capability describes:

- The network/genesis identity, product and supported protocol/profile versions.
- Its service role: recent, archive or combined where supported, and whether the
  endpoint performs queries or forwards them.
- Actual local coverage: inclusive start and covered-through heights, gaps or
  multiple ranges as needed, and the publication/manifest identities and chain
  anchors that bind those holdings.
- Readiness and freshness observations, separate from configured capabilities and
  from coverage available through forwarding.

Height ranges are discovery filters, not a substitute for exact session matching.
Use each product's existing coverage model: Status also describes its observation
freshness, while Receiver cannot advertise truncated history as complete recovery.
Keep the first protocol focused on capability and coverage; reputation, payments
and claimed performance rankings are outside it.

Each phase requires focused tests and an independently reviewable result before
later phases depend on it. Preserve existing wallet-library clients and
independent correctness oracles. Exercise:

- Encrypted queries through the provider's complete HTTP endpoint, with equivalent
  answers and refusals in embedded and decoupled deployments.
- Corrupt, incomplete, incompatible and interrupted artifact downloads, and
  restart during synchronization, preparation and activation.
- Stale authority, revocation during preparation or evaluation, and chain reorgs.
- Client cancellation, slow transfers, overload and resource retention.
- Unavailable peers, outdated holdings, routing loops and safe retry boundaries.
- Publisher switching, mirror failure and rejection of mixed publication material.
- Discovery of single-product and multi-product providers, incompatible protocols,
  wrong networks, coverage gaps and local versus forwarded holdings.
- Forged, expired, replayed and withdrawn announcements; coverage changes after a
  reorg; stale readiness; and loss of a provider or bootstrap directory.
- Publication polling with unchanged responses, timeouts and retry backoff,
  recovery after missed revisions, and unavailable delta bases. Qualify up to 100
  consumers, including simultaneous bootstrap and continued updates under query
  load; retain product-specific freshness and authority gates.
- Hierarchy generation handover under continuous client load: old-generation
  service continues while the publisher offers only the new generation; activation
  and advertised coverage switch together. Exercise failed preparation, interrupted
  downloads, a superseded candidate, admitted old-generation requests and a new
  node joining during the transition. Check reorg revocation separately.

Measure startup and update cost, steady-state memory, disk, synchronization
bandwidth and query performance together. Include packing costs, temporary
overlap between publications, verification and continued updates under query
load. Publish minimum requirements from measurements; retain existing product
freshness and qualification gates.

Artifact authentication establishes origin and integrity, not completeness of
indexed chain data. Preserve independent wallet validation and existing
completeness assumptions. PIR hides the selected record under the existing
protocol; providers and routers still observe the timing, traffic and coarse
routing information those protocols expose. This plan adds no new privacy claim.

## Defaults and exclusions

Start with explicitly approved publishers and configured peers, then introduce
discovery under operator-controlled admission policy. Direct provider endpoints
must keep working without automatic discovery or permissionless registration.
Start publication synchronization with request/response polling; subscriptions
remain an optional later optimization of update detection.
Keep wallet protocols, record layouts, session identities, validation and
persisted formats unchanged. Introduce no new consensus mechanism, incentive
system, PIR primitive or universal ingestion framework.

This plan does not authorize deployment or expand an existing refactor task.
Implement phases as focused changes; keep source completion, qualification and
production activation separate. Production activation requires its own explicit
authorization under the [fleet lifecycle](operations.md).
