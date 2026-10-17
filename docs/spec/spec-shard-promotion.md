# Shard promotion specification

Status: draft. This document specifies promotion by changing ownership of
unchanged immutable shards. It does not claim that the behavior is implemented
or deployed.

The [decentralized query-serving architecture](../decentralized-query-serving.md)
explains the design and implementation phases. This specification owns the
normative promotion rules; update both documents when those rules change.

## Scope and terminology

The keywords MUST and MUST NOT express requirements. MAY expresses a permitted
choice.

- **Publisher:** the service that constructs and publishes a hierarchy and its
  serving artifacts.
- **Hierarchy:** the published organization of shards and their coverage,
  including recent and archive holdings.
- **Promotion:** moving an already sealed, immutable shard out of recent holdings
  into archive holdings, without changing the shard itself. Full Query Servers
  may retain the shard throughout that move.
- **Generation:** a version of that hierarchy within a publisher's publication
  stream, used by publishers and query servers to synchronize holdings and routes.
  It does not introduce a new wallet-visible generation or replace existing
  product revision and session identities. Routine per-block data revisions are
  outside this specification.
- **Current generation:** the finalized generation the publisher currently offers
  for synchronization and bootstrap.
- **Active generation:** the generation a query server currently uses to answer
  wallet requests.
- **Ready generation:** a generation whose required artifacts have been fetched,
  verified and loaded for that query server's serving responsibilities.
- **Full Query Server:** a provider covering the full history, with shards held
  in memory on one machine when they fit or distributed across multiple machines.
- **Recent Only Query Server:** a server holding the recent tier for its supported
  service configuration, without retaining the full archive locally.

Different query servers may temporarily have different active generations while
they update. A publisher's current generation and a query server's active
generation are distinct state.

## Promotion and wallet compatibility

The first version of promotion MUST change holdings and routing without rewriting
the promoted shards. It MUST preserve their coverage boundaries, shard identifiers,
content and revision digests, PIR geometry, serving bytes and public setup, and
existing wallet request and session bindings. Promoting a shard MUST NOT split,
merge, renumber or repack it. Sealing a mutable tail and re-cutting existing shards
are separate operations outside this specification.

The synchronization generation MUST remain a publisher/query-server concern.
Wallets MUST NOT need to send or interpret it. Existing wallet endpoint formats,
manifest schemas, cryptographic parameters, and refusal and refresh behavior MUST
remain unchanged. Holdings and routing metadata MAY change through those existing
interfaces, provided they continue to identify the same immutable shards and
preserve the product's validity requirements. This does not require successive
manifests to be byte-for-byte identical.

A router MUST preserve the exact shard, revision and session binding of an
encrypted request when selecting a local worker or forwarding destination.
Moving a shard to another owner MUST NOT cause a router to reinterpret or rewrite
the request for different data or setup. Any future shard reorganization requires
a separate specification and explicit wallet compatibility qualification; it MUST
NOT be treated as ownership-only promotion.

## Server capacity and promotion policy

A Full Query Server MAY distribute its shards across machines as history grows.
Its full dataset is not required to fit on one 64 GB host. This specification does
not require disk-backed archive queries as the way to provide full coverage.

The complete recent holdings for a supported Recent Only service configuration
MUST be serviceable on one 64 GB RAM host. The host budget MUST include resident
PIR data and prepared runtime material, the embedded router, query workspace at
the supported serving load, generation-handover overlap and scratch space, and
system overhead. Shared unchanged artifacts need not be counted twice. A machine
serving multiple products MUST account for their combined footprint.

The publisher's promotion policy MUST keep recent holdings within that supported
host budget, including ordinary generation handover. The archive's growth or a
Full Query Server's total footprint MUST NOT imply that Recent Only servers need
larger hosts. Exact recent-window sizes, promotion batches and cadence remain to
be qualified; the 64 GB requirement is not a claim of measured capacity.

At the advertised supported serving load, query p99 during promotion MUST be no
more than 1.10 times the matched no-promotion baseline. Qualification MUST cover
transfer, verification, loading, activation and cleanup, using the same hardware,
dataset, offered load and query mix. Errors and timeouts MUST be reported alongside
latency so rejecting requests cannot establish compliance.

## Publisher requirements

1. The publisher MUST finish constructing and finalizing the amended hierarchy
   and its required serving artifacts before making its generation current.
2. Once finalized, the publisher MUST atomically advance its current generation
   and stop offering the previous generation for synchronization or bootstrap.
3. Query servers joining after that change MUST be directed to the new current
   generation.
4. Publisher advancement MUST NOT require a simultaneous switch by all query
   servers. Existing query servers update independently.

Stopping distribution of an old generation does not require deleting immutable
artifacts that the current generation also references. It does not revoke copies
of the old generation already held by query servers.

## Existing query-server requirements

1. Query servers MUST discover the publisher's current generation through the
   request/response synchronization protocol.
2. On detecting a newer generation, an existing query server MUST initiate its
   update against the current generation.
3. The query server MUST keep its active generation available to wallets while
   fetching, verifying and loading the replacement. The old generation's metadata
   and setup needed to answer those requests MUST remain available locally.
4. The query server MUST NOT activate a partially fetched or unready generation.
   Readiness MUST include eligible serving destinations for shards whose routes
   change, verified under the product's existing routing and authority rules.
   Publisher finalization alone does not establish destination readiness.
5. Once the replacement is ready, the query server MUST switch its router and
   worker serving view atomically. Its advertised active generation MUST reflect
   the serving view actually in use.
6. A failed or interrupted update MUST NOT by itself make an otherwise valid
   active generation unavailable. The query server MUST retry synchronization
   against the publisher's current generation.

The ordinary promotion process MUST NOT introduce a gap in a query server's
serving readiness. This requires preparation of the replacement while the old
generation remains active, rather than unloading the old generation first.

## New query-server requirements

1. A new query server MUST obtain the publisher's current generation and
   bootstrap against it.
2. It MUST fetch, verify and load the required artifacts before advertising
   readiness or accepting wallet queries for that generation.
3. If its bootstrap target is withdrawn before it is ready, it MUST reconcile
   the publisher's current generation and continue against that generation.

## Wallet requests and validity

Wallets MUST be able to continue using the existing wallet protocol against a
query server's active serving view while that server prepares the new generation.
A publisher advancing its generation MUST NOT, by itself, invalidate those
requests or their existing shard, revision and session identities.

After activation, requests already admitted under the old generation retain
their binding and the product's existing request-lifetime rules. A query server
MUST NOT evaluate a request against data or setup other than those to which it is
bound. Stale wallet sessions follow the product's existing refusal and refresh
behavior; no new wallet generation negotiation is introduced.

These rules cover ordinary hierarchy replacement. They do not authorize serving
data that has been invalidated by a reorg, whose serving authority has expired,
or that otherwise fails the product's existing validity requirements. Preserve
those requirements and the [serving contract](../serving-contract.md).

## Conformance scenarios

A conforming implementation demonstrates:

- A publisher finalizes a new hierarchy, advances its generation and stops
  offering the old generation, without waiting for every query server to update.
- Existing servers continue answering old-generation queries while preparing the
  new generation, and switch independently without a gap in serving readiness.
- A new server joining during that transition bootstraps against the publisher's
  new current generation.
- Interrupted downloads or failed preparation preserve service from an otherwise
  valid active generation and lead to another synchronization attempt.
- Router and worker activation do not mix generations; admitted requests preserve
  their binding and stale sessions receive the existing product behavior.
- An unchanged wallet client completes metadata discovery, setup and encrypted
  queries before, during and after promotion through the existing HTTP interface.
  Exercise local and forwarded serving, independently updating providers, and a
  destination that is not ready. Activation waits for eligible destinations while
  the old serving view remains available.
- Promoted shards retain their exact coverage, identifiers, digests, geometry,
  serving bytes and setup. A candidate that rewrites a shard is rejected as
  ownership-only promotion, even if its hierarchy generation is newer.
- Reorg invalidation and expired authority still cause the required refusals.
- Recent Only service and generation handover fit on a 64 GB RAM host at the
  supported serving load, including transient memory and system overhead.
- Matched promotion measurements satisfy the 10% query-p99 interference bound.
- Full coverage can place in-memory shards across machines without changing the
  recent-tier capacity contract.

The publisher/query-server wire encoding, endpoint names, polling intervals and
promotion thresholds remain to be specified. Shard rewriting and changes to the
wallet protocol are outside this version. No additional generation-retention
window is introduced by this specification.
