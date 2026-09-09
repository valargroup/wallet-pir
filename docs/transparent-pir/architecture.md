# Transparent PIR architecture

See [status](status.md) for source and live implementation evidence and [deployment](deployment.md) for target parameters.

## Data and recovery

The service indexes confirmed receive and spend events under exact raw locking scripts. Spend extraction resolves each consumed previous output to its script. The wallet reconstructs a ledger, applies its own confirmation and spendability policy, and accepts its chain anchor independently of shielded scanning. The [contract](contract.md) defines completeness and privacy assumptions.

A shard is a consecutive height range (called a generation in historical research). Each shard has a public activity filter, a private two-choice script directory and private packed event pages. Short histories may fit inline. A table can have multiple equal-geometry segments for an indivisible oversized block. Both directory candidate rows and every required page are retrieved privately across all table segments; routing must not expose the chosen row or segment.

```mermaid
flowchart TD
  N[Archive node] --> I[Event ingest and durable journal]
  I --> P[Publisher: immutable shard revisions]
  P --> O[Public map, filters and setup origin]
  P --> R[Recent replica pool]
  P --> A[Archive assignment pool]
  W[Wallet: accepted anchor and local scripts] --> O
  W --> F[Local filter matching]
  F --> Q[Private retrieval router]
  Q --> R
  Q --> A
  R --> L[Validate events and commit ledger with coverage]
  A --> L
```

The router, assignment pools, external artifact origin and atomic fleet activation in this diagram are target components, not all implemented. The current service loads a whole published set.

## Component ownership

| Component | Source | Responsibility |
|---|---|---|
| Events | `pir/transparent-events` | Canonical chain event representation |
| Ingest, census, publish | `server/transparent-filter-server` | Resolve events, persist journal, seal and build shards, serve filters |
| Shard protocol | `pir/transparent-shard` | Geometry registry, deterministic packing, manifests and sealing |
| Retrieval service | `server/transparent-shard-server` | Verify publication, bound runtime cache, revision-addressed setup/query |
| Wallet | `pir/transparent-wallet` | Local matching, private retrieval, validation, ledger replay |
| Deployment | `ops/` and `.github/workflows/` | Build, distribute, preflight and activate artifacts |

## Identity and geometry

The source schema is `transparent-shard-v7`. A shard declares a named geometry from the compiled registry. A manifest commits layout and table digests; the public map names manifest revisions. Sealed contents remain immutable and manifests chain through parent digests. A tier boundary must be a shard boundary. Profile names must never be reused with different row shapes.

Revision identity must be preserved through setup, query, response, runtime cache and wallet coverage. The server verifies manifests when opening a set. The wallet retrieves and verifies revision manifests, their map fields, parent digests and registry geometry before private requests. Hash consistency does not prove completeness or authenticate a malicious publisher's index merely because its terminal block hash is accepted.

The publisher supports recent/archive geometry and a forced `--recent-from` height. Census supports bounded inclusive ranges. A combined mixed-tier cost report must use the same boundaries and aggregate exact scripts across tiers; separate tier percentile summaries cannot be added.

## Serving and publication

The current cache retains prepared runtimes by byte reservation, builds on demand, and keeps active handles pinned. Plaintext sources are verified files rather than permanent copies in memory. Source defaults allow one construction, two evaluations and three superseded revisions per shard beyond the current one. An expired revision returns 409; cache pressure can return retryable 503. The wallet distinguishes these responses and bounds revision refreshes within one sync.

Cold builders hold their construction slot and queue fairly for shared scratch-memory admission. Each builder retains its turn while waiting for memory, so later work cannot repeatedly overtake it. Already-admitted queries wait at most 250 ms, bounded by their remaining deadline, for memory; restore reservations remain nonblocking; the shared memory ceiling and cancellation ownership rules apply to every path.

After cold construction drops its plaintext input and completed scratch buffers,
the worker shrinks the transient reservation to the retained runtime allowance
plus 2 MiB of serialization scratch and releases the construction slot. The
verified runtime becomes available immediately. A separate blocking snapshot writer
retains the reservation, runtime reference and cache pin through snapshot locking,
writing and durability, including after request cancellation. Snapshot failure
increments the cache error counter without invalidating the serving runtime.
`transparent_shard_disk_save_pending` tracks outstanding writers. Linux workers advise
the kernel that consumed source/cache files and synced snapshot files can leave
page cache; this is advisory and never deducted from measured admission usage.
Non-Linux workers omit the advice. Checksums and snapshot format remain unchanged.

Runtime construction reuses the fixed public mask images already shared by online packing. The pinned library (`61dc83e`) computes the independent left/right reference collapse halves in parallel and preserves their digit order. Database-dependent preprocessing is rebuilt for each changed table; client secrets and uploaded key bodies are never shared. Differential and frozen-vector tests require identical output to the preceding implementation, so existing public runtime snapshots remain compatible. Snapshot reads and writes batch the same little-endian coefficients into 8 KiB buffers and retain their checksum, atomic rename and durability barriers.

The target recent pool fully replicates its assigned hot set so the newest shard can use every worker's bandwidth. Archive ownership is disjoint and balanced by prepared bytes. Public routing uses set identity, shard, revision and table; selected script, row and page locator never determine a plaintext route. Public immutable filters/setup belong on an artifact origin; map discovery has refreshable cache semantics.

Prepare and verify artifacts at owners before publishing routing/map state. `/v1/ready` reports assignment and revision identity and, in warm mode, requires every current assigned runtime to finish warming. The explicit loaded-only pilot mode has a weaker readiness condition. Completion of a prewarm task alone does not prove full warm readiness.

## Wallet state and aging

A returning wallet must retain old outputs while applying new spends. Moving the birthday forward is not ledger continuation. Every sync takes an explicit wallet-accepted height and hash, independently of the publication tip. A shard crossing that height is retrieved and validated in full; only its accepted prefix enters the ledger and discovery. Coverage retains both the full source revision endpoint and the accepted prefix endpoint. Ledger changes and coverage commit together; the completion anchor advances only after all required work succeeds. Replacing a provisional revision replaces its covered range; reorg recovery rolls back to the accepted ancestor.

Freeze the initial geometry cutoff. Aging may change worker ownership after verified copying, but it does not change shard geometry, ids or history. Re-cutting into wider archive geometry is a future epoch transition requiring explicit identity and coverage recovery; do not implement a rolling cutoff that silently rebuilds old shards.
