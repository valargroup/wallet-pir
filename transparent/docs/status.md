# Transparent PIR status

Updated 2026-09-27 (release rollout below); milestone text last updated 2026-09-13. The target remains an opt-in, recovery-only macOS beta on
existing infrastructure. **M0, M1 and M2 are accepted; M3 is partially validated;
M4–M6 are open.** This records observed progress, not a new live fleet health
check. [Remaining work](remaining-work.md) is the authoritative outstanding
checklist; [deployment](deployment.md) owns operating targets.

## Current milestone evidence

| Milestone | Result and evidence | Scope and limits |
|---|---|---|
| M0 — Baseline | [Accepted September 9](../evidence/productionize-m0-2026-09-09/README.md): source, native/UI capability and checkpoint-write inventory | The later M1 record resolves the baseline's fleet unknowns |
| M1 — Fleet | [Accepted six-hour window](../evidence/productionize-m1-six-hour-acceptance-2026-09-13/README.md), reconciled with Roman's updated requirement | Matching loaded canary, corrected rollout and six-hour observation; later freshness failure remains an M5 incident |
| M2 — macOS recovery | [Accepted September 9](../evidence/productionize-m2-2026-09-09/README.md), wallet source `0ce6d158f` | Real native recovery-only application, isolated profile, sending disabled; not a signed distribution or beta acceptance |
| M3 — Correctness | [Fixture validation](../evidence/productionize-m3-2026-09-10/README.md) at wallet `7937d48df`; [real-wallet native recovery and resume](../evidence/productionize-m3-private-wallet-2026-09-13/README.md) compare exactly with independent reduction | Release-app UI and stronger live interruption evidence remain outstanding; public regression passed as recorded below |
| M4 — Whole-wallet benefit | [80 exact paired recoveries](../evidence/block-comparison-2026-09-09/README.md) give a derived 99.19% reduction in additional transparent payload | Representation-size comparison is not whole-wallet incremental latency or capacity |
| M5 — Capacity and recovery | Historical [fleet series](../evidence/runs/fleet-series-2026-09-08-r1/README.md) and M1 observations exist | No accepted sustained beta operating envelope or complete failure-recovery rehearsal |
| M6 — Release and beta | No acceptance evidence | Review, versioned distribution and tester observation follow M3–M5 |

## Release rollout, 2026-09-27

Observed by the operator (Claude, for Roman) on 2026-09-27; times UTC.

**Public metadata outage, 14:18–18:55.** The Enhance deploy re-rendered the
coordinator Caddyfile from `ops/deploy/coordinator/Caddyfile`, which lacked the
continuous-publication route. `/v1/shards`, `/v1/shards/init` and manifests
returned 404, and `/v1/filters/shards` served the stale static map. The route
was restored by hand at 18:55 and added to the template in `2c658d43`.

**Worker and publisher release.**
- Workers were on source `a5f79ed` (binary `200ca850…`, ipir-sp `accc424`).
- `9d47b05b` passed full CI and was rolled out with `deploy-transparent-publisher.yml`
  in shadow mode (recent-01, archive-01, archive-02, recent-02..04, 22:26–22:40)
  and activated at 22:40:42. All six workers now report binary `c735a6b3…` and
  are warm with every runtime. The publisher serves source `9d47b05b`, with
  directory choice tables **off**.
- Before the rollout:
  - A local test restored a runtime cache written by the `a5f79ed` worker in the
    `9d47b05b` worker. All 28 runtimes restored, and 52/52 syncs were exact.
  - A rc.6 client synced against the old fleet with no transport failures.
- After activation, 17 public syncs completed with no failures. Archive restores
  were 6/6 exact. The recent mismatches are consistent with activity since the
  sample's 3,473,686 anchor: 30/120 restore-6m and 32/120 catch-up-30d sample
  clients have later events.

**Failed first attempt, 22:20–22:25.**
- The shadow run failed before touching workers. The rotated
  `WALLET_PIR_DEPLOY_SSH_KEY` (`enhance-pir-deploy`) was not authorized on any
  transparent host.
- By then the script had already stopped the controller and overwritten its
  working credentials, so public metadata returned 502 until the previous
  controller config was restored by hand.
- The rotated key's public half was then appended to `authorized_keys` on the
  router and all six workers; each host keeps an
  `authorized_keys.before-deploy-key-2026-09-27` backup.
- The deploy script now:
  - checks every host accepts the identity before stopping anything;
  - keeps the previous controller config;
  - restores the controller if it fails before any worker changes.

Shadow mode itself withdraws public metadata while workers upgrade, here about
14 minutes, as the maintenance path does.

**Directory choice tables enabled, 22:43.**
- `directory_choice: "all"` was added to `/opt/transparent-publisher/controller.json`
  (backup `controller.json.before-directory-choice`) and the controller restarted.
- The first new tail revision (shard 175, revision 1172, height 3,498,503)
  carries a 4,321-byte table for 27,989 scripts. Workers loaded it, and loading
  verifies every entry's route.
- 25 public syncs against it completed with no failures.
- Existing sealed shards keep their table-free manifests until a full
  republication. Wallets built before the field cannot read tabled manifests;
  none is shipped.
- To revert: remove the field and restart the controller. The next tail
  revision is then published without a table.

## Router health and worker drain rollout, 2026-09-28

Times UTC.

- **Rollout.** `3317cd01` (C1 router policy, worker upload drain, A3 client
  concurrency) went out in shadow mode 10:45–11:00 and was activated at 11:12
  with `directory_choice: all`. All six workers now report binary `3e9bae97…`,
  warm with every runtime.
- **Activation failure.** Activation missed its 180 s deadline. The new router
  policy used `health_fails` and `handle_errors 502 503 504`, which the router's
  **Caddy 2.6.2** rejects, so every route attempt failed validation. Public
  metadata returned 503 from the start of the shadow rollout until 11:16.
- **Recovery.**
  - The live `/opt/transparent-publisher/transparent-live-fleet.py` was patched
    to 2.6-compatible directives (original kept as `…3317cd01-orig`). The
    controller then routed and served at 11:16.
  - The long-running replica reconciler still ran the fleet code it had loaded
    before the upgrade, and re-routed with the old policy at 11:16:18. It was
    restarted, and the controller's next activation installed the new policy
    at 11:19:10.
  - Both renderers were fixed in `24b988db`, with an ops test against newer
    directives, and activation now restarts the reconciler. On 2.6.2, a bare
    `respond` inside `handle_errors` keeps the proxy's status (verified 503
    with `Retry-After`).

## M1 accepted observation

The matching loaded canary ran for more than six hours and 300 blocks with
336,603 exact private responses. The corrected six-worker upgrade passed at
September 11 22:10:59 UTC. The replacement fleet observation's first six hours,
September 12 approximately 00:49:59–06:49:59 UTC, recorded **300 public and 300
replica blocks on each of six workers**, maximum public visibility **20.080 s**,
maximum replica visibility **22.915 s**, minimum available host memory **24.387%**,
and no OOM or service restart. Roman confirmed acceptance under the revised
six-hour requirement on September 13.

Worker source `a5f79ed`, binary
`200ca85065c8096d344749d5e51a2db569ec369c71bd2ffff8cf0e9fd014ff62`,
operations `0e2c003`, router and monitor identities, query logs and all worker
samples are retained in the [acceptance bundle](../evidence/productionize-m1-six-hour-acceptance-2026-09-13/README.md).
The fleet comprises four recent replicas and two archive owners in Amsterdam;
this observation does not measure its sustainable wallet throughput.

The supervisor still requested 24 hours. At September 12 08:05:01 UTC, recent-01
failed replica freshness at block 3,480,596 after **60.715 s** against its 60 s
budget; the other monitors were cancelled. That command remains failed. The
six-hour decision is a retrospective operator acceptance of the completed window,
not a rewritten terminal result. The later failure must receive an M5 disposition.
Earlier failed canaries and their corrections remain in the
[evidence ledger](../evidence/README.md); they earn no additional acceptance credit.

## M3 public regression and HTTP correction

The [first September 13 deployed run](../evidence/productionize-m3-live-2026-09-13/README.md)
attempted all eleven cases: six passed, five failed on transient public-path
transport or availability. It found no ledger mismatch in completed comparisons.
The repaired runner explicitly uses up to three attempts for eligible HTTP
failures, with one total deadline per logical request, exact replay of buffered
requests and a record of every failed and successful attempt. Identity checks,
fixture contents, accepted-anchor comparisons and case deadlines are unchanged.
See [testing](testing.md) for the policy and its limits.

The [frozen-executable repaired run](../evidence/productionize-m3-suite-fix-2026-09-13/README.md)
passed **all eleven cases and 68 checkpoints**. Its 4,320 logical requests used
4,324 attempts: 4 failed attempts, 4 recovered requests, zero terminal request
failures and no ledger mismatch. The source/binary manifests, full checks and
per-attempt evidence are retained. An earlier all-pass repaired run had its
build output replaced by concurrent checks; it remains diagnostic evidence.

The wallet release candidate still pins client `22e6bec` and does not inherit the
suite's explicit retry configuration. Suite correctness under bounded retries
is not proof of uninterrupted availability or a deployed wallet change.

## M3 native wallet and application boundary

The M3 wallet source `7937d48df` extends M2 with independently reduced fixture
histories, reorgs and forks, persistence interruption tests, imported-script and
unsupported-script handling, and shadow comparison. Its fixture-loopback kills
are separate from the real public-service trial.

For the user-supplied recent-history wallet, the actual release bridge (sending
and fixture-loopback disabled) recovered into a new isolated profile from
birthday **3,480,000**, a conservatively earlier height than the requested
September 12 date. Recovery completed at **3,482,317** with no pending pages or
unresolved spends. Independent compact-block reduction matched exact events,
UTXOs and balance. A second fresh profile was killed during the reported
transparent sync phase, reopened as interrupted, resumed to **3,482,322** in
**50.853 s**, and again matched independent reduction. The first profile stayed
unchanged. [Sanitized evidence and reproducible harness](../evidence/productionize-m3-private-wallet-2026-09-13/README.md).

This exercised native code and Dart bindings, not the release application's
screens. The kill was phase-observed, not directly proven to occur during an
HTTP request, and the interrupted profile had no committed transparent anchor.
The oracle obtains watched scripts from wallet discovery and trusts complete
transparent compact-block data; it is not an independent proof of discovery or
index completeness. No mnemonic, private ledger or raw wallet log is published.

## Implementation and prior deployment


| Capability | Observed state |
|---|---|
| Shard schema | Source is `transparent-shard-v9` (4,096-byte rows, 21 directory slots, 46 events per page, 14-byte salted tags, 87-byte events). Implemented and unpublished. The live fleet remains the v7 publication described in the rollout above. A version-2 event journal is built only into a new directory; opening the version-1 production journal with this binary returns an error and leaves the files in place |
| Registry | `recent-8k` 8192/8192; `recent-4k` 4096/4096; `archive-32k` 32768/32768; `archive-wide` 32768/65536 |
| Optional recent pairing | `recent-4k-8k` 4096/8192 registered; not published. Real shards and a [placement study](../evidence/directory-placement-4k-2026-09-28/README.md) fit it in one segment with no overflow up to 96% load |
| Single-lookup directory | **Live for new tail and sealed revisions since 2026-09-27 22:43 UTC** (see the release rollout above). Optional manifest `directory_choice`; publisher `--directory-choice off\|sealed\|all` (controller config `directory_choice`, default `off`); builder and server verify routing; wallet sends one directory query per matched script when present. [Measured locally](../evidence/single-lookup-measure-2026-09-27/README.md): 939/939 exact syncs, directory queries halved, restore-6m payload −24%; a [temporary bench fleet](../evidence/single-lookup-fleet-2026-09-27/README.md) reproduced this at 8 and 32 wallets. wallet-libraries not updated |
| Census ranges | `--start-height`, `--end-height`, `--first-shard-id`, geometry overrides, `--placement`, `--single-lookup`, `--per-shard`, exact script matches exist in `shard-census.rs` |
| Two-tier publisher | `--recent-geometry`, `--archive-geometry`, `--recent-from` exist in `shard-publish.rs` |
| Workflow exposure | `publish-transparent-shards.yml` exposes commit, journal, output directory, anchor, `recent_from` (re-derived and checked) and both geometries; the backfill workflow's `inventory` action records journal identity, cutoff and an independent event spot-check |
| Loading/cache | `ShardSet::open_with` loads the whole set or an assignment's subset: every shard's manifest and filter, tables only for assigned ids, global ids and manifest chain intact; bounded runtime cache and file-backed plaintext sources |
| Continuous publication | Controller, incremental suffix publisher, worker prepare/activate/invalidate control, and shadow/activate deployment workflow implemented and deployed; see the dated rollout below. RPC supplies non-finalized tip blocks; the RocksDB secondary remains for historical backfill. |
| Revision handling | Revision-addressed setup/query, 409 refresh, retryable cache pressure, bounded wallet refresh exist |
| Retention | Three superseded revisions per shard beyond current, with optional byte bound (`--retain-bytes`); live collection preserves the newest three retired snapshots and individually held readers, collects other idle snapshots, and trims unpinned retired runtimes before preparation. Current-only prewarm preserves build headroom |
| Observability | Every metric series labelled with map and assignment digests, worker id and role; cold-build histogram; process RSS and cgroup memory gauges; queue depth and body bytes in flight; unassigned refusals; prewarm progress |
| Query validation | The query route refuses a body whose declared length is not the selected table's exact length before buffering, queueing or building; a missing length is 411; the runtime re-checks length and binding as defence in depth; the global HTTP cap remains the ceiling for the widest geometry |
| Admission | `admission.rs` bounds waiting requests (`--query-waiters`), buffered body bytes (`--body-bytes`), upload time (`--upload-deadline-secs`) and total wait (`--query-deadline-secs`); a full queue, exhausted body budget or expired deadline is 503 with `retry-after`, a stalled upload 408; a dropped connection releases its place and is counted |
| Deployment acceleration | Persistent public runtime snapshots, stable per-worker assignment identity, binary identity in readiness, selective staging/restarts, healthy replica pairs and transaction-scoped rollback implemented; cache/managed-preparation progression is recorded by M1. Broader timing acceptance remains in [remaining work](remaining-work.md). |
| Readiness | `/v1/ready` reports mode, map and assignment digests, warm and target runtime counts; loaded-only mode (`--pilot-cold`, whole-set default) is ready once loaded, warm mode (assignment default) only once every assigned runtime is prewarmed |
| Fleet assignment/router | `transparent-assignment-v1` planned by `shard-assign` from a roster; router is Caddy rendered from the assignment, routing on the shard id in the path only; fleet deploy modes prepare, verify with the staged binary, activate owners then replicas, switch the router, verify publicly and over the VPC, prune, and roll back on failure; filter service has its own deploy workflow. Exercised on the fleet; failure and capacity rehearsals remain separately tracked |
| Wallet manifest binding | `GET /v1/shards/:id/revisions/:digest/manifest` serves canonical manifest bytes; before any private request on a matched shard the wallet recomputes the digest, checks every field against the map entry, the previous entry's digest and the registry, and takes the geometry from the verified manifest |
| Accepted-anchor recovery | `sync_into` and the facade require a wallet-accepted height/hash. Partial-shard coverage retains a distinct source endpoint, events above the target are discarded after validation, and rollback accepts an exact ancestor. SQLite schema 2 migrates legacy progress conservatively. See [tests](testing.md); deployed regression remains a separate gate. |
| Persisted continuation | `sync_into` over a `WalletStore` (reference `MemoryStore`; SQLite in `transparent/crates/transparent-wallet-store`): per-script per-shard coverage with the block hash it rests on, settled or provisional; atomic idempotent shard commits; reorg rollback to the accepted ancestor via the wallet's `ChainView`; provisional tail truncation and promotion; durable pending page work; scripts added by the wallet's rules discovered over their required range; a budget or outage ends a call incomplete with no anchor. `sync()` is a wrapper over a fresh memory store. `TransparentSync` facade takes plain data for a host binding. The store contract suite is exported under the `testing` feature and `parse_init` is available without `reqwest` (`5758ffd`); the Zakura wallet (`valargroup/wallet-libraries`) implements the store over its own database and passes it |

Relevant sources: [store](../../transparent/crates/transparent-wallet/src/store.rs), [publisher](../../transparent/services/transparent-filter-server/src/bin/shard-publish.rs), [census](../../transparent/services/transparent-filter-server/src/bin/shard-census.rs), [service](../../transparent/services/transparent-shard-server/src/service.rs), [runtime](../../transparent/services/transparent-shard-server/src/runtime.rs), [loader](../../transparent/services/transparent-shard-server/src/shardset.rs), [wallet](../../transparent/crates/transparent-wallet/src/sync.rs).


The [September 8 continuous-publication rollout](../evidence/continuous-publication-2026-09-08/README.md)
established mainnet publication from a journal starting at height zero, mixed
archive/recent geometry, both public origins and warm fleet routing. Its exact
174-shard inventory, cutoff, IPs, memory measurements and component versions are
historical snapshots; M1 above is the later accepted operational record.
The [inventory](../evidence/inventory-2026-09-08/README.md),
[census](../evidence/census-2026-09-08/README.md) and
[publication verification](../evidence/publication-2026-09-08/README.md) retain the
dataset and independent spot-check provenance.

The native adapter at `bca43b343`, client `22e6bec`, established accepted-anchor
persistence, exact rollback, atomic balance/coverage and safe old-layout refusal;
its [hardening evidence](https://github.com/valargroup/wallet-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/transparent-pir/evidence/hardening-2026-09-08/README.md) precedes M2/M3.
M2 closed the measured append amplification, misleading near-tip completion and
demo-fallback/custody findings. Its application artifact was ad-hoc signed and
not distributed through an accepted release channel.

Persistent runtime caches, managed preparation and revision-aware collection
progressed from the [cache canaries](https://github.com/valargroup/wallet-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/transparent-pir/evidence/deployment-runtime-live-2026-09-08/README.md)
through the [matching M1 rollout](../evidence/productionize-m1-deferred-collection-2026-09-11/README.md).
Those old canary-only configuration notes are not today's rollout checklist.
A sub-ten-minute compatible fleet update and failed-batch rollback still need
separate timing/recovery evidence. The optional
[parent-filter artifact rollout](../evidence/parent-filters-production-2026-09-08/README.md)
passed a bounded canary; its heavy-wallet performance comparison did not finish.
