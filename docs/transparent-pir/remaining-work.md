# Remaining work to deploy transparent PIR

Updated 2026-09-08 against source `6ff2bfe`. The user accepted the [deployment target](deployment.md). This is the authoritative execution checklist, not a claim that unchecked work is absent in every future commit. Update each item with its implementing commit and verification artifact when it closes.

## Sequence and critical path

Deliver documentation first, then verified inventory and dataset, mixed census, full-chain correctness pilot, fleet/wallet prerequisites, target-host load tests, and staged activation. The optional narrower recent directory must not block the initial deployment. Do not provision the full fleet before mixed-set sizing and target-host measurements justify it.

Suggested reviewable changes:

1. Documentation consolidation: `fe72ecc`; link validation `make check-docs`.
2. Inventory/provenance report and workflow input reconciliation: `3969f96`, `2a3c5d3`, `b574681`, `113b22d`, `abe297b`; Gate 0 closed.
3. Mixed-tier measurement and publication tooling parity: census tooling `0aaf25c`, `2a3c5d3` (`shard-census --recent-from`, `--compare-map`), two-tier census recorded `b2c91cc`; publication and comparison pending the full-chain run.
4. Wallet manifest validation and early request admission: `6ff2bfe`; Gate 3 closed.
5. Assignment-aware loading, router, prewarm/readiness and activation: `142c193`; code and tests complete, host rehearsals in Gate 7.
6. Durable wallet continuation, provisional replacement and reorg recovery: `ba9680e`; the Vizor wiring remains a separate deliverable.
7. Full-chain correctness evidence and host/fleet load evidence.
8. Reviewed infrastructure configuration, rollout and recovery evidence.
9. Optional `recent-4k-8k` measurement and separate promotion decision.

Some implementation work can proceed independently after the dataset is pinned, but public fleet activation depends on every gate below. These are work packages, not instructions to launch background agents.

## Gate 0 — Verified inventory and dataset

- [x] Record the live-state fields in [status](status.md), checking both retrieval and filter services rather than resolving v6/v7 conflicts from prose. Observed by run 34181566124; [evidence](evidence/inventory-2026-09-08/README.md).
- [x] Record the journal's actual network/genesis, pinned start height, committed end height, anchor hash and timestamp, event count, source and tool SHA, and read-only open behavior. `journal-inventory` (`3969f96`), record in the same evidence directory; ingest tool is the state-backed path landed in `71e60c9`.
- [x] Resolve the historical output-count discrepancy with independent block extraction: sample dense early-chain blocks, recent blocks and spend-heavy blocks; compare output/input event identities and previous-output scripts. A live UTXO count is not cumulative history. Preserve the independent extraction report. `event-spotcheck` (`3969f96`, `113b22d`, `abe297b`): 11 blocks including the three densest agree exactly; `spotcheck.json`.
- [x] Derive the six-calendar-month cutoff from the pinned UTC anchor timestamp. Specify month-end clamping and deterministic block timestamp selection, accounting for non-monotone block timestamps; record the resulting height and hashes. Verify coverage is `[genesis, cutoff-1]` plus `[cutoff, anchor]` with no gap or overlap. `shard-cutoff` (`3969f96`); rule in [deployment](deployment.md); cutoff 3,262,749 recorded in `cutoff.json`.
- [x] Capture disk/RAM headroom on publisher and pilot worker. Preserve rollback binary, set, schema and map/config identifiers. `coordinator.txt`, `worker.txt`.
- [x] Inspect CI inputs and deployed units: publish CLI supports tiers but the workflow does not yet expose them. Add validated explicit source, anchor/cutoff and geometry controls with a durable input record. `publish-transparent-shards.yml` and `--through`/`--record` (`3969f96`); `inventory-transparent-dataset.yml` (`2a3c5d3`).

Deliverable: dated nonsecret inventory and dataset manifest, reproducible cutoff calculation, independent event spot-check report. Exit: a reviewer can identify exactly which chain data and running state subsequent work uses. A discrepancy in event extraction blocks publication, not merely the benchmark report. **Closed 2026-09-08**: dataset pinned at anchor 3,473,686, cutoff 3,262,749, tools on main, evidence linked above.

## Gate 1 — Mixed-tier census and geometry validation

- [x] Use existing census `--start-height`/`--end-height` over both inclusive ranges; retain actual policy derivation. Do not implement range controls that already exist. Run as one two-tier census with `--recent-from 3262749 --archive-geometry archive-wide --geometry recent-8k --end-height 3473686`, policies derived by `SealPolicy::for_geometry` ([census 2026-09-08](evidence/census-2026-09-08/README.md)).
- [x] Run `--placement`, `--per-shard` and exact script-match aggregation. Save network/anchor, full command, tool SHA, geometry, policy, elapsed time and peak RSS with raw output. All in [census 2026-09-08](evidence/census-2026-09-08/README.md): 174 shards (160 archive, 14 recent), 3 min 37 s, 715 MiB peak.
- [x] Add a combined report or shared sealer orchestration that reproduces publisher boundaries and aggregates exact script identity across both tiers. Merge per-script query/byte costs before computing percentiles; do not add tier p99s or assume scripts occur in only one tier. `shard-census --recent-from` runs the publisher's sealer through the geometry change and merges matches by script identity across tiers (`fn tiers`, `report_cost_by_tier`); 66,305 scripts occur in both.
- [x] Compare predicted and emitted shard boundaries (`shard-census --compare-map` against the full-chain publication: agrees on every shard, two oversized scripts reconciled through the manifests; [record](evidence/census-2026-09-08/README.md)), segment counts, placement and table lengths. Report reasons for sealing, occupancy, ordinary versus oversized-block shards, filters/setup, reserved runtime and separately projected/measured RSS.
- [ ] Measure ordinary recent catch-up and old-birthday restoration across the same mixed set. Include unused scripts/filter false positives, reused scripts and multiple-script wallets.
- [ ] Size archive assignments by resident bytes and recent complete replicas by their actual working set, including revision overlap. Revisit deployment budgets if they do not fit; do not conceal cache eviction as all-resident serving. Reservation arithmetic is in the census README (80 archive shards per owner, 45.0 GiB of 48; 14 recent shards, 3.5 GiB of 5); measured process RSS on the target hosts is Gate 6.

Exit: ordinary shards have one segment per table, oversized blocks remain correctly retrievable, complete coverage is exact, and measured/projection labels are unambiguous. Mixed-set sizing replaces the uniform 162-shard proxy before fleet procurement.

## Gate 2 — Full-chain publication and correctness pilot

- [x] Publish both tiers into a new directory with a tested pinned binary; preserve the current serving set. Record peak publisher RSS, temporary disk, total bytes and elapsed time under realistic ingest coexistence. [Publication 2026-09-08](evidence/publication-2026-09-08/README.md): 174 shards, 54 GiB, 15 min, 2.5 GiB peak; the ingest unit was inactive during the run, so coexistence is not yet measured.
- [x] Validate every manifest/table/filter digest and parent chain, schema/profile agreement, deterministic construction, exact range and anchor. Load the entire candidate before activation. `shard-verify` (`verify-transparent-shards.yml`): 13 checks, 0 failures, six shards rebuilt byte-identical ([record](evidence/publication-2026-09-08/README.md)); the full rebuild has not been run.
- [ ] Verify filter origin and retrieval origin return identical map bytes. Confirm filter-service binary and configured set schema agree before any Enhance-coupled deployment.
- [ ] Stage on the existing worker and run recovery against an independent journal replay at the same anchor. Compare exact event multiset, UTXOs and transaction history, not balances alone.
- [ ] Include unused and zero-balance histories, receive-then-spend while offline, old output spent recently, multiple owned scripts, self transfers, coinbase, false positives, collisions, packed-page boundaries and a genuine multi-segment fixture.
- [ ] Measure cold pilot restoration separately from warm serving. Do not infer fleet latency from a small cache that rebuilds most of the archive.

Exit: full-chain retrieval is correct and publication/recovery is reproducible. This pilot validates a bounded sync; it does not establish persisted returning-wallet correctness or fleet capacity.

## Gate 3 — Identity validation and bounded admission

- [x] Serve immutable revision manifests. Wallet recomputes their digests, compares them with its accepted map, verifies parent links and range/profile/table shape consistency, and derives geometry from the verified manifest. Manifest route and `verify_manifest` (`6ff2bfe`).
- [x] Preserve the trusted-indexer assumption explicitly: matching a manifest hash supplied by the same indexer is not an independent completeness proof. A chain anchor alone does not authenticate the index. [Contract](contract.md), `6ff2bfe`.
- [x] Keep runtime exact-length request validation for each selected profile/table. Add early route-specific length checks before queueing/build work; the global maximum body limit remains a coarse ceiling, not the whole validation policy. `6ff2bfe`.
- [x] Bound waiting requests and retained body bytes as well as active evaluations. Add deadlines, cancellation and explicit retryable overload behavior. A semaphore with unlimited waiters can still exhaust memory. `admission.rs` (`6ff2bfe`).
- [x] Preserve revision/table query and response binding, setup reuse binding, exact script checks and uniform access across segments. Unchanged and re-asserted by the Gate 3 tests.

Validation: unknown/substituted geometry, altered manifest/table, mismatched map, stale setup, mixed revision, undersized/oversized body, slow uploads, cancelled queries and overload. All refuse explicitly without advancing coverage or making a public locator fallback. Verify oversized requests are rejected before expensive work. **Closed 2026-09-08**: `tests/round_trip.rs` (oversized before any work, no declared length, slow upload, full queue and cancellation, deadline, manifest route), `tests/wallet_sync.rs` (altered manifest refused with no private query, genuine manifests verified once per matched shard, existing geometry/schema/stale/overload cases), unit mutation table in `sync.rs`.

## Gate 4 — Assignment, routing, readiness and publication

- [x] Represent global set identity separately from worker assignment. Preserve global shard ids, ranges and manifest chain; do not renumber shards to make a local subset satisfy the current whole-set loader. `assignment.rs`, `LoadScope::Assigned` (`142c193`).
- [x] Implement assignment-aware artifact validation/loading and ownership metadata. Retain enough authenticated global metadata to validate a subset and reject wrong-set/wrong-revision requests. `ShardSet::open_with` keeps every shard's manifest and filter, verifies assigned tables, refuses an assignment for another map, answers 421 for unassigned shards (`142c193`).
- [x] Generate routing from public assignment data: recent full replicas, archive owners, revision availability and health. Never route on plaintext scripts, private rows or page locators. Keep table queries uniform across segments. `router.rs`, `shard-assign caddyfile`, golden `ops/fixtures/transparent-shard/Caddyfile.router` (`142c193`).
- [x] Prewarm all current assigned runtimes and the next advertised recent tail. Readiness reports assignment/map identity and warm state; retain a distinct loaded-only pilot mode. `ReadinessMode`, `spawn_prewarm`, `--pilot-cold` (`142c193`). Retained superseded tails are warmed only into free budget.
- [x] Bound retained revision bytes and define pruning order. Default retention is three superseded revisions beyond current, not three total or an hour of guaranteed service. Retain required runtime state according to measured churn and budget. `--retain-bytes`, `--prune-excess`, `shard-prune` (`142c193`); churn measurement is Gate 6.
- [x] Implement prepare/verify/activate with coherent discovery map and routing. During rollout, old requests reach a compatible retained revision or receive explicit recovery responses. Copy before ownership changes. `deploy-transparent-shard.sh fleet-preflight/fleet-deploy/fleet-rollback` (`142c193`); not yet exercised on real hosts (Gate 7).
- [x] Add queue, cache/build, cold-start, RSS/cgroup, revision, assignment and sync-completion observability. Keep operator endpoints private; expose only required wallet paths publicly. `metrics.rs`, `procmem.rs` (`142c193`); sync-completion is a client-side figure the Gate 6 harness reports.
- [x] Preserve transparent/Enhance service boundaries. Add a filter-service-only preflight/deploy path or document and test the coordinated Enhance rollout until decoupled. `deploy-transparent-filter.sh`, `deploy-transparent-filter.yml`, `--check-shard-dir` (`142c193`); the Enhance workflow's filter staging is removed after the first successful filter-only deploy.

Validation: concurrent cold requests for one runtime, eviction with active requests, second/third/fourth tail publication, missing owner, replica loss, router restart, interrupted copy, corrupt artifact, worker reboot, and map activation failure. Rollback restores coherent binary/set/routing. Archive-owner loss is explicit unavailability in the initial disjoint topology. Covered in tests: `tests/assignment.rs` (subset load, wrong map, 421, warm and pilot readiness, over-budget refusal, balancing, golden router, byte retention), `tests/revisions_and_cache.rs` (concurrent cold builds, in-use eviction, bound). Remaining as Gate 7 rehearsals on real hosts: missing owner, replica loss, router restart, interrupted copy, worker reboot, map activation failure, rollback.

## Gate 5 — Durable wallet continuation and integration

- [x] Define wallet-owned persistence for ledger, discovery scope, accepted anchor, settled coverage, provisional revision coverage and unresolved private work. `WalletStore` in `pir/transparent-wallet/src/store.rs`; `MemoryStore`; SQLite reference `pir/transparent-wallet-store` (`ba9680e`).
- [x] Implement sync into an existing ledger. Moving the birthday forward cannot substitute for retaining an old receive when applying a recent spend. `sync_into`; `required_from` is never raised (`ba9680e`).
- [x] Commit ledger and coverage atomically; make retries idempotent. Newly derived/imported scripts receive their required historical discovery coverage. One commit per shard and per page; `ScriptProvider::on_activity` (`ba9680e`).
- [x] Implement truncate/rollback and replay for replaced provisional ranges and reorgs. Reject contradictory duplicates; do not append replacement ranges as disjoint history. `rollback_above`, provisional reconciliation, `ChainView` reorg detection with per-shard coverage hashes (`ba9680e`).
- [x] Persist setup/filter reuse only under verified chain, set, revision and profile identity. A refresh must not silently accept an unrelated publication or a lower anchor. `SetupKey`, `SetIdentity`, `SyncError::AnchorRegressed` (`ba9680e`).
- [x] Keep recovery bounded with durable pending work; an exhausted limit or outage never becomes a synchronized balance or plaintext address/outpoint lookup. `WorkLimits`, `PendingPages`, `Completion::Incomplete` with no anchor commit (`ba9680e`).
- [ ] Integrate first with Vizor through wallet-owned adapters. Run alongside existing sync until the wallet owner accepts the documented trusted-indexer completeness model. Separately account for pending transactions and mixed-pool display/enhancement. The adapter boundary and a plain-data facade (`TransparentSync`) exist (`ba9680e`) and the [adapter contract](wallet-adapter.md) states what the wallet supplies and must not do; the Vizor repository change is a separate deliverable outside this repository. The first wallet integration landed in the Zakura wallet (`valargroup/wallet-libraries`, crate `zakura-wallet-transparent`, 2026-09-08): `WalletStore` implemented over the wallet's own SQLite database with the shard commit projected into the balance in the same transaction, the wallet's rewind and the library's rollback on one path, the exported contract suite (`transparent_wallet::testing`, `5758ffd`) passing over that store, and the wallet's own recovery suites run against `transparent-shard-server` in process. Vizor's wiring remains.

Validation: crash before/after persistence boundaries, old receive/new spend across separate sync calls, restarted wallet, offline receive/spend, imported keys/gaps, empty history, tail replacement, reorg across tier boundary, stale revision refresh and prolonged overload. Compare full ledger/history with independent replay after each recovery. **Covered**: `pir/transparent-wallet-store/tests/store_semantics.rs` over both stores; `server/transparent-shard-server/tests/wallet_continuation.rs` (old receive/new spend, required height raise, query budget resuming pages exactly, restart, crash before and after the pending commit, gap advance and imported script, unused wallet, superseded tail truncated and replayed, reorg below the tier boundary, other set and lower anchor refused, unrelenting overload), each compared with an independent traversal.

## Gate 6 — Target-host and fleet capacity

Use actual selected regional SKUs, including their CPU sharing model. Test recent candidates on shared and dedicated small hosts before selecting a sustained-load configuration. The existing c-8 microbenchmark is not a measurement of either proposed host family.

- [ ] Measure one recent and one archive target host over real HTTP/TLS: cold construction, warm query service, cache residency and peak RSS with revisions and in-flight requests.
- [ ] Run 1, 2 and 4 recent replicas concurrently to test independent aggregate bandwidth and newest-shard hotspot distribution. Concurrent droplets may share physical-host bandwidth; do not assume linear speedup.
- [ ] Run both archive assignments while recent catch-up and ingest/publication continue. Report skew and archive demand separately; old nine-host restore capacity does not transfer.
- [ ] Step client concurrency through 8, 32, 128 and 512, stopping safely at saturation. Distinguish simulated clients, active requests, physical PIR queries, wallet syncs and daily active users.
- [ ] Workload classes: unused wallets, small active wallets, daily/weekly/30-day catch-up, six-month restore, old-birthday restore, multi-script wallets and reused/spammed-script tails. Include warm/cold filters and setup, tail churn and retries.
- [ ] Collect at least 100 completed traces per ordinary class in each of three independent runs, plus sustained saturation and recovery runs. Retain failures and incomplete jobs in denominators.
- [ ] Record upload, response, setup, filter and HTTP/TLS totals; completed syncs/s, p50/p95/p99 latency, queue wait/depth, errors/retries, RSS, peak client memory and cold misses. Measure mobile device/network performance separately from desktop loopback.
- [ ] Establish proposed product latency objectives from those reports and record them before release. No current numerical wallet latency SLO is established by the evidence.

Exit: exact recovery still passes under load; queues and memory remain bounded; no OOM or hidden failed work; normal admitted demand is at most 50% of measured sustainable completed-sync throughput and meets the recorded workload SLOs. Repeat only after meaningful code/configuration changes or unresolved results.

## Gate 7 — Infrastructure and staged activation

- [ ] Confirm provider availability, prices, project and hardware selection against deployment.md and Gate 6. Review actual mixed-set RAM/disk headroom and transfer budget.
- [ ] Reconcile current Terraform state, SSH reachability, volume attachment/mount identity and unintended resources. Save and inspect the plan; avoid replacing the shared node/volume or unrelated Enhance workers.
- [ ] Parameterize tier, assignment, replica group, cache/memory limits and readiness settings in infrastructure and units. Treat configuration examples as target settings until this change lands.
- [ ] Stage a recent replica and archive ownership path; preflight and warm before advertisement. Expand to the accepted fleet only after each canary passes ledger, revision, routing and resource checks.
- [ ] Rehearse archive-owner loss/rebuild and router recovery with measured interruption time. If interruption violates requirements, revise topology before launch.
- [ ] Rehearse coherent rollback, including filter service, map, router, worker binary and wallet revision recovery. Preserve publication artifacts needed by clients already holding a map.
- [ ] Publish dated live-state and capacity reports; link workflow runs and source SHA, record remaining limitations and operational alert thresholds.

Exit: deployed inventory matches verified state, real wallet recovery passes on public paths, required rollback/recovery has been exercised, and every outstanding limitation has an explicit product/operational disposition. This documentation approval does not itself assert these gates passed.

## Optional optimization and deferred work

- [ ] Add `recent-4k-8k` without mutating existing profile meanings; add meaningful wire/placement/client compatibility checks. Score against the initial recent geometry over exactly the same range and workload distribution.
- [ ] Promote only if total sync bytes improve and completion latency/tails remain within recorded objectives, with no ordinary segment stacking or material shard-match regression. Store the decision and evidence; otherwise retain the baseline.
- [ ] Track growth of aged recent-shaped shards. Move ownership without reshaping; re-census capacity before each planned expansion.
- [ ] Design epoch re-cutting only when growth warrants it: new lineage identity, cache invalidation, replay costs, migration overlap and rollback all need explicit semantics.
- [ ] Archive redundancy, redundant routing, key reuse and stronger traffic-pattern privacy are separate decisions. Do not import key-reuse experiment figures into current capacity estimates.

## Documentation completion rules

Each completed gate links source commits, exact commands and evidence. Update status from observation and deployment only when the selected target changes. Delete superseded prose and update inbound links instead of retaining duplicate documents or appending a contradictory correction below an active recommendation. Preserve raw evidence and its scope. Run repository-required checks and documentation link validation before submitting changes.

## Accepted-anchor regression rollout

- [ ] Record the first complete deployed run of the [accepted-anchor regression suite](testing.md) against its frozen full-chain fixture after fleet activation. Local conformance and source implementation do not establish this live gate. The [2026-09-08 preflight](evidence/regression-2026-09-08/README.md) refused the still-served three-shard pilot before any private queries.

## Deployment-time improvements — 2026-09-08

- [ ] Complete offline checks for persistent runtime restore, stable per-worker
  deploy identity, healthy replica pairs and transaction-scoped rollback.
- [ ] After the current load measurement finishes, activate a cache-enabled
  replica and owner canary; verify cache population, compatible restart, memory,
  disk headroom and correct wallet recovery.
- [ ] Measure a compatible-binary fleet deployment below ten minutes, excluding
  build/CI; verify unchanged rollouts cause zero restarts and router-only changes
  leave workers serving. Preserve decisions, phase timings and regression results.
- [ ] Rehearse failed-batch and explicit transaction rollback with the cached
  previous release. Keep these gates open until host evidence exists.
