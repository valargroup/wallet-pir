# Transparent PIR status

Source inspection: 2026-09-08 through worker hardening `9af5c12` and catch-up correction `7b76b39`. Live state is observed separately below; [remaining work](remaining-work.md) owns the outstanding release and capacity gates.

## Source-verified implementation

| Capability | Observed state |
|---|---|
| Shard schema | `transparent-shard-v7` in `pir/transparent-shard/src/manifest.rs` |
| Registry | `recent-8k` 8192/8192; `recent-4k` 4096/4096; `archive-32k` 32768/32768; `archive-wide` 32768/65536 |
| Desired optional recent pairing | 4096/8192 absent; add a new name, never reinterpret `recent-4k` |
| Census ranges | `--start-height`, `--end-height`, geometry overrides, `--placement`, `--per-shard`, exact script matches exist in `shard-census.rs` |
| Two-tier publisher | `--recent-geometry`, `--archive-geometry`, `--recent-from` exist in `shard-publish.rs` |
| Workflow exposure | `publish-transparent-shards.yml` exposes commit, journal, output directory, anchor, `recent_from` (re-derived and checked) and both geometries; the backfill workflow's `inventory` action records journal identity, cutoff and an independent event spot-check |
| Loading/cache | `ShardSet::open_with` loads the whole set or an assignment's subset: every shard's manifest and filter, tables only for assigned ids, global ids and manifest chain intact; bounded runtime cache and file-backed plaintext sources |
| Continuous publication | Controller, incremental suffix publisher, worker prepare/activate/invalidate control, and shadow/activate deployment workflow implemented and deployed; see the dated rollout below. RPC supplies non-finalized tip blocks; the RocksDB secondary remains for historical backfill. |
| Revision handling | Revision-addressed setup/query, 409 refresh, retryable cache pressure, bounded wallet refresh exist |
| Retention | Three superseded revisions per shard beyond current, with optional byte bound (`--retain-bytes`); live collection preserves the newest three retired snapshots and individually held readers, collects other idle snapshots, and trims unpinned retired runtimes before preparation. Current-only prewarm preserves build headroom |
| Observability | Every metric series labelled with map and assignment digests, worker id and role; cold-build histogram; process RSS and cgroup memory gauges; queue depth and body bytes in flight; unassigned refusals; prewarm progress |
| Query validation | The query route refuses a body whose declared length is not the selected table's exact length before buffering, queueing or building; a missing length is 411; the runtime re-checks length and binding as defence in depth; the global HTTP cap remains the ceiling for the widest geometry |
| Admission | `admission.rs` bounds waiting requests (`--query-waiters`), buffered body bytes (`--body-bytes`), upload time (`--upload-deadline-secs`) and total wait (`--query-deadline-secs`); a full queue, exhausted body budget or expired deadline is 503 with `retry-after`, a stalled upload 408; a dropped connection releases its place and is counted |
| Deployment acceleration | Persistent public runtime snapshots, stable per-worker assignment identity, binary identity in readiness, selective staging/restarts, healthy replica pairs and transaction-scoped rollback implemented; cache-enabled recent and archive canaries deployed. Broader timing acceptance remains in [remaining work](remaining-work.md). |
| Readiness | `/v1/ready` reports mode, map and assignment digests, warm and target runtime counts; loaded-only mode (`--pilot-cold`, whole-set default) is ready once loaded, warm mode (assignment default) only once every assigned runtime is prewarmed |
| Fleet assignment/router | `transparent-assignment-v1` planned by `shard-assign` from a roster; router is Caddy rendered from the assignment, routing on the shard id in the path only; fleet deploy modes prepare, verify with the staged binary, activate owners then replicas, switch the router, verify publicly and over the VPC, prune, and roll back on failure; filter service has its own deploy workflow. Exercised on the fleet; failure and capacity rehearsals remain separately tracked |
| Wallet manifest binding | `GET /v1/shards/:id/revisions/:digest/manifest` serves canonical manifest bytes; before any private request on a matched shard the wallet recomputes the digest, checks every field against the map entry, the previous entry's digest and the registry, and takes the geometry from the verified manifest |
| Accepted-anchor recovery | `sync_into` and the facade require a wallet-accepted height/hash. Partial-shard coverage retains a distinct source endpoint, events above the target are discarded after validation, and rollback accepts an exact ancestor. SQLite schema 2 migrates legacy progress conservatively. See [tests](testing.md); deployed regression remains a separate gate. |
| Persisted continuation | `sync_into` over a `WalletStore` (reference `MemoryStore`; SQLite in `pir/transparent-wallet-store`): per-script per-shard coverage with the block hash it rests on, settled or provisional; atomic idempotent shard commits; reorg rollback to the accepted ancestor via the wallet's `ChainView`; provisional tail truncation and promotion; durable pending page work; scripts added by the wallet's rules discovered over their required range; a budget or outage ends a call incomplete with no anchor. `sync()` is a wrapper over a fresh memory store. `TransparentSync` facade takes plain data for a host binding. The store contract suite is exported under the `testing` feature and `parse_init` is available without `reqwest` (`5758ffd`); the Zakura wallet (`valargroup/wallet-libraries`) implements the store over its own database and passes it |

Relevant sources: [store](../../pir/transparent-wallet/src/store.rs), [publisher](../../server/transparent-filter-server/src/bin/shard-publish.rs), [census](../../server/transparent-filter-server/src/bin/shard-census.rs), [service](../../server/transparent-shard-server/src/service.rs), [runtime](../../server/transparent-shard-server/src/runtime.rs), [loader](../../server/transparent-shard-server/src/shardset.rs), [wallet](../../pir/transparent-wallet/src/sync.rs).

## Hardening and adapter: observed 2026-09-08, 23:04 UTC

[Hardening evidence](evidence/hardening-2026-09-08/README.md) separates committed
changes from rollout state. Shared transient memory admission, allocator
reclamation, disk runtime collection and background recent-replica reconciliation
are implemented. Only recent-01 has the new worker binary; reconciliation is
restricted to that canary. The other five workers retain their previous binary.
Both public origins agreed at height 3,476,721, matching node and journal. All
workers reported warm; recent-02 and recent-03 were still on older publications.

The Zakura adapter is committed to `zakura-core/wallet-libraries` main at
`bca43b343`, pinned to client `22e6bec`: fixed accepted scan target, exact rollback
anchor, distinct coverage source anchor, durable validated page progress,
explicit incomplete reasons, and atomic balance/coverage reads through the app.
Its new layout 4 refuses older layouts without deleting wallet data. The 282-test
wallet suite, schema rejection test, 49 UI tests, generated binding analysis and
macOS debug build passed. Live recent/archive adapter queries decoded correctly;
a fresh recovery using independent node hashes also reached its accepted target of 3,476,726 and completed on repeat. These test fixtures do not prove a particular user's balance.

A 60-second canary smoke passed 662 exact private queries. The initial sustained
run exposed excessive reclamation at a 5 GiB MemoryHigh and was stopped. The
second run found a catch-up scheduling gap during a block burst. Commit
`7b76b39` corrected it without relaxing the gate; the third run uses the
[deployment target](deployment.md) and began at 23:15:22 UTC. **The six-hour/300-block canary, wider rollout and 24-hour monitoring
are not yet accepted.** See [remaining work](remaining-work.md).

## Live deployment: observed 2026-09-08, continuous publication

The current rollout is recorded in [continuous-publication evidence](evidence/continuous-publication-2026-09-08/README.md). The post-fix 20-block service acceptance passed; the subsequent wallet adapter changes and worker canary are recorded below. Earlier [dataset inventory](evidence/inventory-2026-09-08/README.md), [full-chain publication](evidence/publication-2026-09-08/README.md), [warm fleet measurements](evidence/runs/fleet-warm-2026-09-08-c/README.md) and [cache canaries](evidence/deployment-runtime-live-2026-09-08/README.md) remain historical evidence at their recorded sources and workloads.

| Field | Observed |
|---|---|
| Source | Controller `c676fb6`; fleet hook `08e2261`; all six workers at `6e0c65c` |
| Handoff at 16:02:05 UTC | Node, journal and public coverage matched at 3,476,386; both origins agreed, all workers were warm, and no controller publication error was present |
| Chain and geometry | Mainnet journal starts at 0; 174 shards, comprising 160 `archive-wide` and 14 `recent-8k`; verified cutoff remains 3,262,749 |
| Public routing | `transparent-pir.valargroup.dev` resolves to router 209.38.42.220; both public map URLs and filters use the coordinator's active authority. The old pilot forwards wallet paths to the router during cached-DNS overlap |
| Fleet | Four recent replicas (10.142.0.10/.8/.7/.12), two disjoint archive owners (10.142.0.6/.9), one router (10.142.0.11), all ams3; runtime and memory budgets unchanged |
| Publication | RPC ingest follows non-finalized best-chain height/hash. Immutable suffix preparation and warm activation replace the old morning-only static tail. Publication preparation resumed after all six workers were verified warm; the temporary repair pause is removed |
| Memory correction | Live churn exposed runtime headroom and startup-snapshot retention bugs; both corrected. The final private canary completed 12 activations with at most three retired snapshots after collection and no OOM events or restarts |
| Reorg | An actual one-block journal reorg at 3,476,290 triggered public withdrawal during the first repair pause and recovered after resumption. It was above then-public coverage. Published sealed-history replacement and cross-tier wallet rewind are deterministic test evidence, not an injected mainnet reorg |
| Block visibility | Twenty consecutive new blocks, 3,476,357–3,476,376, observed 15:28:55–15:48:55 UTC: p50 17.562 s, p95 24.356 s, maximum 26.687 s; no HTTP errors, origin mismatches or orphaned endpoint samples |
| Memory during that window | Recent worker sampled RSS 4.604–5.858 GiB; owners 45.955–46.526 GiB. At most four retired snapshots between activation and collection; zero new OOM events or restarts on all six workers |
| Active quorum | Both archive owners and one recent replica per accepted generation. Other replicas were retried but excluded from that generation's routing; this is not four-replica capacity evidence |
| Wallet adapter | Existing `5758ffd` pin passed fresh empty-wallet recovery through 3,476,342 and recent/archive HTTPS private queries. A later recovery raced map/filter activation and failed safely. Reference-library fix `87226c3` passes race/corruption and broader recovery tests; adopting it in the adapter requires accepted-anchor store/API migration, as recorded in remaining work |

## Decision and evidence state

The user accepted the two-tier target on 2026-09-07. [Deployment](deployment.md) owns its settings. Full-chain uniform geometry and single-c8 evaluation/residency evidence exist; mixed publication evidence and limited target-host HTTP measurements are recorded; sustained capacity, cross-host scaling and mobile wallet latency remain open. [Remaining work](remaining-work.md) is the authoritative checklist.

## Deployment runtime follow-up

[Further live validation](evidence/deployment-runtime-followup-2026-09-08/README.md)
confirmed four-slot startup prewarm at 17.644 seconds on recent-01 and 125.416
seconds on archive-01 from current-process journals. Three new encrypted public
row probes passed. Four workers still lack disk-cache configuration; recent-01
has exhausted its disk-cache budget and rebuilds new tails without persisting
them. A full-fleet deployment under ten minutes remains unverified.
