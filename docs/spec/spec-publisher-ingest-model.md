# Publisher ingest model

Status: draft. This document specifies the common publisher ingest, storage,
verification and publication model. It does not claim that all products implement
the model or that it is deployed.

## Scope and terminology

The keywords MUST and MUST NOT express requirements. SHOULD expresses a
recommended practice. MAY expresses a permitted choice.

This specification applies to publisher workflows that turn source observations
into prepared PIR publications. Products retain their own extraction rules,
coverage, shard geometry, cryptographic parameters and freshness requirements.
The common model is:

1. Store immutable bytes once, under an identity derived from their contents.
2. Publish references to those bytes when the source advances.
3. Reuse verification of unchanged files; recheck changed or replaced files.
4. Make a publication current only after preparation succeeds.
5. Delete stored bytes only when nothing still needs them.

An **object** is a complete immutable file, such as a table segment or prepared
runtime. An **object reference** identifies its content digest and exact byte
count. A **publication** describes source coverage and the objects required to
serve it. A **candidate** is a publication being prepared. The **current
publication** is the finalized publication the publisher authorizes consumers to
synchronize. A query server's active publication is separate state.

**Verification evidence** records a successful check of particular bytes in a
particular file and the context in which the check is valid. A **pin** keeps an
object available for an operation that still needs it; it need not be an extra
filesystem link.

The [publisher-query server protocol](spec-publisher-query-server-protocol.md)
governs transfer and query-server validation. The [shard promotion
specification](spec-shard-promotion.md) governs ownership changes of unchanged
shards. This document changes neither policy and specifies no new public wire
format, wallet API or numerical freshness threshold.

## Source ingestion and candidate boundaries

The publisher MUST ingest source data into a coherent committed view before
preparing a candidate. For a chain source, the candidate MUST bind its coverage
to a block height and hash, the network, and the applicable recovery or withdrawal
state. A height alone does not identify a chain prefix.

Preparation MAY run while ingestion advances, but MUST use a consistent snapshot
or equivalent stable view. A concurrent append MUST NOT silently change a
candidate's recorded coverage. A rollback or rewrite MUST invalidate any
candidate whose inputs no longer agree with its source binding.

An unchanged source or unchanged serving material SHOULD reuse existing objects.
A product MAY publish a fresh observation or new authority metadata without
rebuilding identical serving bytes. It MUST preserve the true observation time;
renewing metadata MUST NOT make an old observation appear fresh.

## 1. Store immutable bytes once

The publisher MUST identify each object by SHA-256 of its complete stored bytes
and record its exact byte count. Products MAY retain additional identities for
source tables, runtime parameters or logical roles; those identities do not
replace the object's whole-file digest.

Object construction MUST use private staging. Before installing a completed
object, the publisher MUST finish writing it, establish its length and digest,
and make its contents durable. Installation MUST be atomic, and the containing
directory MUST be synced before a durable publication can reference the object.
An interrupted write MUST NOT appear as a completed object.

Installed objects MUST NOT be modified in place. Different bytes require a new
object identity. An existing object with the expected digest and size MAY be
reused only after verification or valid verification evidence establishes its
contents. A conflicting existing object MUST fail preparation; it MUST NOT be
silently overwritten or accepted because its filename looks correct.

Large objects MUST be written and hashed with bounded working memory. A common
storage abstraction MUST NOT require buffering a whole table or runtime merely
to install or identify it. Products SHOULD share suitable storage and durability
primitives instead of implementing different versions of these guarantees.

## 2. Publish references when the source advances

A candidate MUST describe its source binding, required objects and the metadata
needed to interpret them. Objects shared with earlier publications MUST remain
at stable physical locations. Publication advancement MUST NOT create or remove
hard links to those objects merely to express membership.

For example, if only an open table and its runtime change, the next publication
references the existing sealed tables and runtimes plus the newly prepared
objects. Reuse MUST work at the smallest independently immutable object boundary,
including unchanged page segments inside a changing shard revision.

Publication metadata MAY map existing logical artifact paths to stored objects.
All publisher readers, validators, restoration paths and HTTP serving paths MUST
use the same checked resolution rules. Resolution MUST bind the expected object
identity and size, stay within publisher-owned storage, and reject traversal,
missing objects, unexpected file types and unchecked symbolic-link redirects.

Preparing or storing an object MUST NOT authorize its distribution. Public
serving MUST be limited to objects authorized by the applicable retained
publication. Files left by a failed candidate MUST NOT become publicly available
through an arbitrary-path or object-directory listing endpoint.

## 3. Reuse verification of unchanged files

The publisher MUST establish both byte integrity and the product's preparation
consistency requirements before making a new candidate current. Hashing a runtime
proves its bytes match an expected digest; it does not establish that the runtime
was correctly derived from its source and parameters.

Reusable evidence MUST bind the expected digest and size, the exact file's
identity and change metadata, and the semantic-check context. That context MUST
include the source identity and runtime parameters when they affect correctness.
A matching path, filename, modification time alone, or claimed digest MUST NOT
be sufficient evidence.

On filesystems where device, inode, size, modification time and change time are
used for evidence, all MUST match. Evidence MUST respect timestamp granularity:
a file changed too recently for those observations to distinguish another write
MUST NOT become reusable merely because two timestamp samples compare equal.
Implementations MUST NOT ignore change time to accommodate link-count churn.

Verification MUST check the opened file and its named identity before and after
the read. A changed identity, changed metadata, unexpected byte count or digest
mismatch MUST fail that preparation attempt. Merely declining to cache evidence
while accepting the attempted verification is insufficient. Semantic checks MUST
likewise use the verified files and refuse evidence if their inputs change.

When valid evidence still holds, later preparation and inventory construction
MUST reuse it instead of hashing unchanged object bodies again. Reuse MUST cover
both source tables and prepared runtimes. Changed or replaced files MUST be
reverified; a changed semantic context MUST undergo the corresponding checks.
Evidence caches MUST be bounded to retained work rather than grow with every
publication ever produced.

A previously received file's local download receipt MUST NOT, by itself, supply
publisher preparation evidence. Query-server storage trust and background
validation follow their separate protocol. Saved object references or successful
checks from another process MUST NOT automatically establish current verification
evidence after a publisher restart.

## 4. Make the completed publication current

The current valid publication MAY remain available while its successor is being
prepared, subject to existing authority, canonicality, withdrawal and freshness
rules. Preparation failure MUST NOT expose a partial successor or advance its
reported coverage. Keeping old coverage available does not make it fresh.

Before advancement, the publisher MUST establish that every required object and
publication record is complete and durable, all required preparation checks
passed, and the candidate still satisfies its source and authority bindings.
Cancellation, shutdown, reorganization and withdrawal checks MUST occur after
awaited operations and immediately before durable activation effects. A check
performed only before an await is insufficient.

Object retention MUST be established before the candidate's references become
visible as current. The current-publication record MUST advance atomically and
durably, so consumers observe a complete predecessor or complete successor.
Consumers MUST NOT participate in publisher preparation completion: a slow or
disconnected query server MUST NOT prevent an otherwise valid publication from
becoming current.

If a source reorganization or withdrawal invalidates the predecessor, the
publisher MUST apply the existing product refusal and recovery rules. It MUST
NOT retain authority merely because the predecessor's bytes are intact.

## 5. Delete only objects that are no longer needed

Collection MUST account for every retained publication, candidate under
construction, durable activation or recovery intent, validator and in-flight
response. An object's absence from the newest publication alone MUST NOT make it
collectible. Opening or pinning an object MUST be coordinated with collection so
it cannot disappear between the ownership check and the operation's acquisition.

Dropping one publication MUST release its references without changing shared
object bytes or inode metadata. A stored object MAY be removed only after its
last owner releases it. Collection MUST NOT invalidate evidence for objects that
remain in use elsewhere. Protection MAY use retained references, open handles or
equivalent lifetime ownership; creating hard links per publication is not the
required mechanism.

After a crash, recovery MUST restore durable publication and activation ownership
before collecting objects. Incomplete staging files and unreferenced objects MAY
then be collected. Recovery MUST expose complete state, preserve authority and
replay floors, and distinguish available bytes from verified bytes.

Existing publications MAY be imported into the object store through checked
staging. Import MUST verify identity and length, preserve live readers, and avoid
requiring a second full copy of the dataset. Import MUST NOT rewrite served
objects in place or weaken steady-state verification. Existing serving-first
restart and background-validation policies remain governed by their product
contracts; restart availability does not authorize unchecked new publication.

## Conformance and freshness evidence

Conformance MUST include tests for:

- Multiple source advances through real publisher preparation and HTTP serving,
  with unchanged objects reused and newly changed objects verified. Count bytes
  read for hashing and semantic checks; timing alone cannot establish reuse.
- Same-size corruption with restored modification time, truncation, replacement
  inodes, wrong digests, wrong runtime parameters and source/runtime mismatches.
- Changes during reads or semantic checks, cancellation before finalization, and
  a source reorganization while preparation is running. Failed candidates MUST
  leave no successful evidence or newly authorized publication.
- Missing or conflicting references, traversal and symbolic-link escapes.
- Collection with shared objects, live candidates, validators and HTTP responses,
  including a changing revision that shares unchanged page objects.
- Restart and crashes around object installation, candidate completion and
  current-publication advancement, including incomplete imports.
- Independently checked decoded data and encrypted answers against a retained
  fixture or source oracle; agreement between publisher-generated metadata and
  publisher-generated objects alone is not an independent correctness check.

Operational evidence MUST separate source observation, committed ingest,
construction, verification, runtime preparation, inventory finalization and
publication advancement. It MUST report source-to-publication delay, source and
published heights where applicable, and bytes hashed versus reused. Failed
attempts and reorganization retries MUST remain visible.

Representative-size qualification MUST establish the existing product freshness
target under repeated updates and supported load. Small fixture tests establish
correctness and bounded repeat work, not deployed-size latency. A green test run,
listening socket or healthy process MUST NOT be reported as proof of freshness,
independent encrypted-answer acceptance or deployment.
