# Transparent PIR status

Source inspection: 2026-09-08, commit `6ff2bfe`. Live state is observed separately below.

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

## Live deployment: observed 2026-09-08

Recorded by [inventory-transparent-dataset.yml run 34181566124](https://github.com/valargroup/enhance-pir/actions/runs/34181566124); raw files and findings in [evidence/inventory-2026-09-08](evidence/inventory-2026-09-08/README.md). This is the pilot, not the accepted fleet.

| Field | Observed |
|---|---|
| Journal | `/srv/zakura/transparent-event-data`, mainnet, start 0, committed through 3,473,686, 352,873,356 events, hashes agree with the node |
| Anchor / cutoff | 3,473,686 (`0000000000755137…be1d`, 2026-09-06T07:39:01Z); cutoff 3,262,749 (`0000000000b300ab…e8d9`); archive `[0, 3262748]`, recent `[3262749, 3473686]` |
| Spot-check | 11 blocks re-derived from verbose RPC, 0 disagree |
| Coordinator | `enhance-pir-coordinator-01`, 8 vCPU, 62 GiB, `/srv/zakura` 447 GB free; filter service unit reads `/srv/zakura/transparent-shards-v7`, binary `ddac2cde…` |
| Pilot worker | `transparent-pir-worker-01`, 4 vCPU, 15 GiB, release `d1f86c3`, binary `b95baba2…`, unit `--shard-dir /srv/transparent-pir/shards --cache-bytes 8589934592 --build-slots 1 --query-slots 2`, `MemoryMax=12G`, 3 `recent-8k` shards through 3,473,474, 3 revisions held, rollback binary/unit/Caddyfile present |
| Public map | Served digest `581cb0f5…` identical at worker loopback, `https://transparent-pir.valargroup.dev/v1/shards` and `https://enhance-pir.valargroup.dev/v1/filters/shards` |
| Not yet observed | A revision-bound query result on the public path; the full-chain publication (does not exist yet) |

Record later observations the same way: a dated evidence directory from the workflow's artifact, and this table updated to the newest run. Keep secrets out; keep hostnames only after verification.

## Decision and evidence state

The user accepted the two-tier target on 2026-09-07. [Deployment](deployment.md) owns its settings. Full-chain uniform geometry and single-c8 evaluation/residency evidence exist; mixed publication costs, target-host HTTP capacity, cross-host scaling and mobile wallet latency remain unmeasured. [Remaining work](remaining-work.md) is the authoritative checklist.
