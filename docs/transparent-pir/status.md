# Transparent PIR status

Source inspection: 2026-09-07, commit `8514863`. This documentation change does not deploy or measure a service.

## Source-verified implementation

| Capability | Observed state |
|---|---|
| Shard schema | `transparent-shard-v7` in `pir/transparent-shard/src/manifest.rs` |
| Registry | `recent-8k` 8192/8192; `recent-4k` 4096/4096; `archive-32k` 32768/32768; `archive-wide` 32768/65536 |
| Desired optional recent pairing | 4096/8192 absent; add a new name, never reinterpret `recent-4k` |
| Census ranges | `--start-height`, `--end-height`, geometry overrides, `--placement`, `--per-shard`, exact script matches exist in `shard-census.rs` |
| Two-tier publisher | `--recent-geometry`, `--archive-geometry`, `--recent-from` exist in `shard-publish.rs` |
| Workflow exposure | `publish-transparent-shards.yml` exposes commit, journal, output directory, anchor, `recent_from` (re-derived and checked) and both geometries; the backfill workflow's `inventory` action records journal identity, cutoff and an independent event spot-check |
| Loading/cache | Whole-set loader verifies manifest/table identity; bounded runtime cache and file-backed plaintext sources exist |
| Revision handling | Revision-addressed setup/query, 409 refresh, retryable cache pressure, bounded wallet refresh exist |
| Retention | Three superseded revisions per shard in addition to current; disk pruning and warm retention require explicit operational policy |
| Query validation | The query route refuses a body whose declared length is not the selected table's exact length before buffering, queueing or building; a missing length is 411; the runtime re-checks length and binding as defence in depth; the global HTTP cap remains the ceiling for the widest geometry |
| Admission | `admission.rs` bounds waiting requests (`--query-waiters`), buffered body bytes (`--body-bytes`), upload time (`--upload-deadline-secs`) and total wait (`--query-deadline-secs`); a full queue, exhausted body budget or expired deadline is 503 with `retry-after`, a stalled upload 408; a dropped connection releases its place and is counted |
| Readiness | Nonempty loaded set; does not establish warm assigned runtimes |
| Fleet assignment/router | Not implemented as the target pool architecture |
| Wallet manifest binding | `GET /v1/shards/:id/revisions/:digest/manifest` serves canonical manifest bytes; before any private request on a matched shard the wallet recomputes the digest, checks every field against the map entry, the previous entry's digest and the registry, and takes the geometry from the verified manifest |
| Persisted continuation | `sync()` builds a ledger for a sync; caller-side continuation, rollback and provisional replacement remain |

Relevant sources: [publisher](../../server/transparent-filter-server/src/bin/shard-publish.rs), [census](../../server/transparent-filter-server/src/bin/shard-census.rs), [service](../../server/transparent-shard-server/src/service.rs), [runtime](../../server/transparent-shard-server/src/runtime.rs), [loader](../../server/transparent-shard-server/src/shardset.rs), [wallet](../../pir/transparent-wallet/src/sync.rs).

## Live deployment: unverified

Historical documents conflict: one says a v6 pilot is live; another says v7 with bounded cache is deployed. They also contain incompatible ingest progress snapshots. No live endpoint, SSH, workflow or provider inventory was queried for this consolidation. Do not treat either narrative as current operational truth.

Before deployment, attach a dated observation containing:

- Binary commit/checksum and unit arguments for retrieval and filter services.
- Schema, set/map digest, exact height coverage, anchor hash, profile counts, revision retention and publication directories.
- Host identifiers, region, actual RAM/disk, memory limits and available storage.
- Both public map bytes/digests, readiness, and a real revision-bound query result.
- Journal metadata: network/genesis, pinned start height, committed coverage and anchor.
- Last successful workflow run and rollback binary/set/config identities.

Record observations without secrets. Keep public hostnames/IPs in this operational record only after verification; do not copy stale addresses from the archive.

## Decision and evidence state

The user accepted the two-tier target on 2026-09-07. [Deployment](deployment.md) owns its settings. Full-chain uniform geometry and single-c8 evaluation/residency evidence exist; mixed publication costs, target-host HTTP capacity, cross-host scaling and mobile wallet latency remain unmeasured. [Remaining work](remaining-work.md) is the authoritative checklist.
