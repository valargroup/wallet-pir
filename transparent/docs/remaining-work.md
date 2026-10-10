# Remaining work for the transparent PIR recovery beta

Attempt 12 activated a fresh v11 publication after the archive roster correction,
then canonical verification refused a worker warm-publication attestation.
The failed native status body was not retained, so future preparation is a
possible cause, not a confirmed historical diagnosis. Local SSH transport failed
separately; the remote owner completed automatic cold rollback in 389.565 seconds.
Independent private/both canonical native proofs, five reopened SQLite stores
and owner exit checks passed. V11 serving verification now checks the exact
warm, non-invalidated active map while a future candidate prepares, as the native
status producer distinguishes these states. HTTP readiness, assignment, release,
all workers and every independently canonical advertised anchor remain required.
Control and HTTP observations are retained privately before refusal. Initial
candidate prewarm, all phase bounds and all final qualification gates remain
unchanged and open. [Evidence](../evidence/activity-metadata-2026-09-30/attempt12-warm-refusal-cold-rollback-6f37db70.json).

Attempt 11 passed candidate preparation and activation, then the closed startup
guard refused a real publication error: the cloned predecessor roster pinned
archive shards 0–76 while the v11 map requires 0–81. Native `shard-assign`
confirmed the mismatch. Automatic cold rollback passed all five phases in
390.229 seconds; independent private/both canonical native proofs and five
reopened SQLite stores passed, and all owners exited. Service preparation now
requires pinned archive ranges to cover the qualified map exactly, rejecting
stale ranges, gaps, overlap, foreign shards, invalid endpoints and changed roles.
The private generator derives this fleet's single owner's range from the
checksum-verified qualified v11 map. Corrected cutover and all final gates remain
open. [Evidence](../evidence/activity-metadata-2026-09-30/attempt11-archive-roster-refusal-cold-rollback-7bdbcbb9.json).

Attempt 10 passed candidate preparation and independent worker activation, but
canonical verification failed with HTTP 503. Automatic cold rollback passed in
379.941 seconds; private and each canonical native/query/SQLite proof passed,
and all owners exited. The historical response endpoint/body were not retained;
controller startup reconciliation is an inference. A closed startup observation
now permits only the native controller's exact local reconciliation refusal,
with pinned unit/PID/binary, no restart/OOM, 20% resource floors and both public
origins still withdrawn. It is bounded to 300 seconds inside the unchanged
canonical phase; all full canonical gates still follow. Actual corrected cutover
and all final qualification remain open.
[Evidence](../evidence/activity-metadata-2026-09-30/attempt10-canonical-refusal-cold-rollback-9164c2c7.json).

Attempt 9 completed the remote v11 cutover and exact private/both canonical
query proofs, but continuous publication failed: operations inputs used `v2`
instead of the native profile name `zcash-transparent-range-v2`. Load remained
paused with zero completed queries. Explicit ordinary cold rollback passed in
377.309 seconds; private and both canonical queries and reopened SQLite stores
passed, and all remote owners exited. The forward SSH reset and subsequent local
rollback transport timeout remain failures, separately reconciled against the
remote journal. The service-input guard now binds the canonical profile from the
reviewed publication geometry and rejects shorthand/foreign names. Corrected
redeployment and every final sustained/freshness/capacity/lifecycle gate remain
open. [Evidence](../evidence/activity-metadata-2026-09-30/attempt9-cutover-freshness-failure-cold-rollback-57e71be1.json).


Updated 2026-09-28. This owns milestone definitions, execution order and the
authoritative outstanding checklist for the opt-in, recovery-only macOS beta. [Status](status.md) summarizes accepted
work; [deployment](deployment.md) owns all operating thresholds. Historical gate
numbers are reconciled below rather than retained as a second release checklist.

## Transparent operations after the 2026-10-09 redeploy

The [2026-10-09 redeploy](../evidence/fleet-redeploy-2026-10-09/README.md) moved the
history workers and the controller to `a317455e`. Operations source now pins those
bytes, and the adapter the reconciler runs, for v11 schema operations
([deployment](deployment.md#portable-activity-worker-executables)). These items remain;
the last two were found during the redeploy.

- [ ] Stage the operations source that carries the new pins on the coordinator with
  `schema-source-plan`, `-preflight` and `-stage`. Every schema operation runs from a
  staged source, and `4c85b6c2`'s copy still pins the old bytes. Run the read-only
  preflight first. Attribution files retained on 2026-10-05 carry the old script pin
  and are now walked as owner records; the reconciler processes they name have exited.
- [ ] Stage the deployed portable executables on the coordinator before any version-1
  worker-input, service-input or product preflight. These are `transparent-shard-server`
  `34ba7ebb…`, `shard-control` `9208555a…` and `transparent-publish-controller`
  `13af048e…` from full CI 37921342549, whose verified extract is on the coordinator
  under `/opt/transparent-publisher/releases/a317455e…/binaries`. They go into
  `/srv/transparent-activity/portable-workers/releases/<sha256>/<name>`, mode 0755,
  through wrapper plan/preflight and `preflight --stage`. Until then those preflights
  refuse with "portable worker artifact identity/mode differs".
- [ ] Make the `4c85b6c2` operations source verify again, or record that the 2026-10-03
  cutover's rollback is no longer available.
  - **The change.** On 2026-10-06 the txid display router hook rewrote
    `transparent-live-fleet.py` inside that tree (`f3df5c53…` → `35b6b436…`). The live
    controller's `fleet_command` and the replica reconciler both run that file.
  - **Why it refuses.** The receipt still records the original bytes, so every phase of
    the committed 2026-10-03 schema transaction refuses at receipt verification, its
    rollback included.
  - **Why source cannot fix it.** Those phases run `4c85b6c2`'s own code. Only the
    bootstrap survey, which runs from the staging client, accepts the one replacement.
  - **One fix.** Stage a new source and point the controller's `fleet_command` and the
    reconciler and control-sessions units at its adapter, then restart them. Next,
    restore the original bytes kept by the router-hook transaction. After that, the
    attribution pins move to the new source.
- [ ] Re-pin the version-1 portable executables to full CI 38011976059 at `9d2cbda0`,
  which production runs since the
  [`9d2cbda0` redeploy](../evidence/fleet-redeploy-9d2cbda0-2026-10-10/README.md):
  `transparent-shard-server` `6ab86d6a…`, `shard-control` `ff3c0264…` and
  `transparent-publish-controller` `6041ea47…`, with `controller.json` `source_sha`
  `9d2cbda0`. The pinned `a317455e` bytes would install older executables.
- [ ] Re-pin or retire the changed-native candidate `c3c66b9b`. It predates `a317455e`,
  so version-2 inputs would install older workers and an older controller than
  production runs, and its 13 supplemental tools have no `a317455e` build.
- [ ] Port the standard deploy paths to v11; the v11 fleet lives under
  `/opt/transparent-publisher/v11/`.
  - `deploy-transparent-publisher.py` `shadow` and `activate` write
    `/opt/transparent-publisher/controller.json` and `fleet.json`. They default to the
    v7 publication, the version-2 journal and `/srv/zakura/transparent-publications`.
    `deploy-transparent-publisher.yml` runs it with those defaults.
  - `upgrade-transparent-fleet.py` defaults to `/opt/transparent-publisher/fleet.json`.
  - `deploy-transparent-shard.yml` reads `/opt/transparent-publisher/state/inventory.json`
    and `roster.json`.

  Run now, they would point the controller back at pre-v11 configuration. The
  2026-10-09 redeploy used a manual runbook instead (its `raw/steps/`), reusing only
  `install_worker`.
- [ ] Give `roll-recent-replicas.py` a reviewed floor for a two-replica tier. It
  restarts a replica only while two other recent replicas serve
  (`MIN_OTHER_SERVING = 2`). The tier has two replicas, and the actuator that used to add
  a temporary third is disabled. The 2026-10-09 roll therefore set the floor to 1 for
  that run only, through a logged wrapper (`raw/steps/roll-two-replica.py`, decision D2).
  Add an explicit, logged override, or bring up a temporary third replica for each roll.

## Dithered 44-bit queries (deployed 2026-10-09)

Servers in source accept a 49-bit or a 44-bit dithered selection, by exact length, and
`init` advertises the dithered schemes (`*_scheme_dq44`, display `scheme_dq44`). Wallets
and txid clients built from this source send 44 bits whenever a service advertises a
scheme they reproduce, so deploying these servers moves current clients to 44 bits with
no further switch; older clients keep sending 49
([architecture](architecture.md#pir-scheme)). History and txid display serve them since
2026-10-09 ([deploy](../evidence/fleet-redeploy-06a972db-2026-10-09/README.md); now on
`9d2cbda0`, [redeploy](../evidence/fleet-redeploy-9d2cbda0-2026-10-10/README.md)).

- [x] Before deploying them, certify every served segment at both widths: 49-bit nearest,
  as today, and 44-bit dithered (`native_certificate --query-rounding dithered`), at the
  unchanged floors `{archive-wide-pages: 83, otherwise: 128}`. The `native-certificates`
  gate and its certifier pin must move to ipir-sp `d76e61a`'s `certify_native.py`, which
  reads dithered reports; the [screen](../evidence/dithered-query-2026-10-09/README.md)
  has the shape-level margins.
  All 608 segments served at 3,512,212 passed at both widths
  ([certificates](../evidence/served-segment-certificates-2026-10-09/README.md)).
- [ ] Certify unsealed tip revisions as they are published; only one snapshot of each
  tip is certified.
- [ ] Move the `native-certificates` gate's checker pin to `d76e61a` and add the dithered
  width before the next schema candidate.

## Activity metadata v3/v11 delivery (2026-09-30)

Attempt 7 failed the candidate cache reader, then the locked wrapper completed
actual automatic cold rollback in 376.654 seconds. Correct native readiness
persistence counters before the next guarded preparation attempt. The transaction is
`rolled-back`; private and each canonical encrypted-query/reopened SQLite proofs
passed. Earlier cold failures remain failed. Complete bounded locked candidate
runtime-cache preparation before another independent activation attempt; a
successful v11 redeployment and every sustained/lifecycle acceptance gate remain
open. [Evidence](../evidence/activity-metadata-2026-09-30/attempt7-cache-reader-failure-cold-rollback-762e81f3.json).

Actual transaction `transparent-schema-20261001T112410Z-15d5de8189d2-c99edc`
started after locked preflight on 2026-10-01 at 11:24 UTC. All five complete v10
baselines were captured, but the coordinator timed out waiting for the archive
capture's repeated native verification. Remote owners subsequently completed and
exited. Recovery restored v10 bytes and services, then failed warm verification:
authority had restarted publication before workers were proved. Public origins
remain guarded; the transaction is `rollback-failed`. The 15-minute recovery
acceptance was missed and must be rerun. The [reviewed recovery repair](../evidence/activity-metadata-2026-09-30/interrupted-cutover-recovery-repair-focused.json)
preserves original recipe/baseline bytes and all failures. Its focused check
passed; source staging and actual repaired recovery are still pending. This is
not a completed v11 deployment.

Owner: this implementation chat. Scope is wallet-pir and zakura-core/wallet-libraries;
Vizor, sending and distribution remain excluded. [Starting observations](../evidence/activity-metadata-2026-09-30/README.md).

- [x] Isolate both repositories at the recorded source bases.
- [x] Provision and mount the selected 250 GiB coordinator volume, preserving v10 state.
- [x] Finish focused extraction, v11 codec, reference SQLite, contradiction and independent-oracle checks.
- [x] Freeze prototype source/artifacts; publish bounded real-chain recent and archive geometry data.
- [x] Establish nonempty HTTP SQLite recovery and exact 5 QPS / 20 QPS prototype gates.
- [x] Backfill a separate genesis-to-anchor v3 journal through 3500738.
  The frozen ingester and independent guard completed successfully; checkpoint
  and terminal evidence are [retained](../evidence/activity-metadata-2026-09-30/full-ingestion-publication-start.json).
- [x] Complete full v11 publication and artifact verification.
  The original wrapper owner PID 1876447 terminated successfully at 05:14 UTC:
  all artifacts and four journal rebuilds passed, with 90 shards and measured
  allocation retained in [terminal evidence](../evidence/activity-metadata-2026-09-30/full-publication-terminal.json).
  Preserve `/srv/transparent-activity/full-v11/preparation`; do not rebuild it.
- [x] Complete actual full-publication native correctness certificates at the approved security floors.
  All 180 distinct table segments passed; [terminal evidence](../evidence/activity-metadata-2026-09-30/full-native-certificates-801d7a62.json)
  retains the exact result and source/table pins. Installed setup/public hash agreement remains a cutover gate.
- [x] Complete independent sampled raw-chain metadata comparisons for the full publication.
  Seventeen blocks, including the three densest and the fixed anchor, matched
  all 52,550 journal events. [Evidence](../evidence/activity-metadata-2026-09-30/full-chain-oracle-801d7a62.json)
  retains raw calls and four adaptive batch-size refusals. Sampled real-chain
  coverage includes Sprout, Sapling and Ironwood; Orchard fixture coverage and
  canonical wallet recovery remain separate gates.
- [ ] Render reviewed live host plans, stage immutable operations/native/unit/publication inputs through the wrapper, and qualify actual SSH descendant locking.
  The new input wrapper has local streaming, interruption and reconciliation
  tests. The first worker copy failed native verification with SIGILL and was
  reconciled preserving abandoned bytes. Portable CI artifacts are selected for
  the replacement. All three candidates now passed transfer, native verification
  and final receipt revalidation. Actual worker host templates bind refreshed
  live rollback state and candidate inputs. Coordinator/router plans, complete
  recovery samples and the full reviewed recipe remain open; no live service transition occurred.
  Subsequent bounded SSH qualification passed on both recent workers, archive
  and router at `ccbfd35f`, after reproducing and correcting OpenSSH's closing
  of inherited descriptors. The keeper retained coordinator locks across parent
  exit; every remote lock and interruption fence passed before reconciliation.
  [Evidence](../evidence/activity-metadata-2026-09-30/ssh-lock-qualified-ccbfd35f.json)
  is scoped to this boundary. Complete nonempty samples and coordinator inputs
  are ready. The user authorized proceeding with pending operations CI while retaining
  passing native CI and exact provenance; finish fresh source staging and actual
  recipe/preflight after the reproduced bytecode integrity defect, then cut over.
  Source preflight refused worker `shard-control` clients issued by the running
  replica reconciler. Reconciler control attribution
  ([deployment](deployment.md#immutable-operation-source-staging-over-ssh)) is fixture-tested only. Before
  relying on it, check read-only on the coordinator that the reconciler snapshot is
  `verified` with the root-observed fragment/script pins, then rerun source
  preflight and keep the private evidence. A refusal whose controls are carried
  by the control-session master, or outlive their client, needs a separate
  reviewed design; candidate execution and deployed qualification surveys do not
  use this attribution.
  Ingest owner: `transparent-activity-full-ingest-release-a1c4b809` on the coordinator;
  journal `/srv/transparent-activity/full-v3/journal`, fixed anchor 3500738.
  Candidate 5/20 QPS gates passed. All 61 retained recoveries at concurrency
  1/4/8 matched fixtures and independently recomputed metadata and summaries
  after SQLite reopen. These brief runs do not establish sustained capacity.
- [x] Implement metadata, source evidence, honest summaries and bounded real HTTP adapter in wallet-libraries.
  Merged [library PR 77](https://github.com/zakura-core/wallet-libraries/pull/77)
  includes the bounded adapter and real HTTP shadow SQLite harness. Five adapter
  tests, 28 history tests and the candidate metadata/reopen/authority test passed
  at `53af85202`. Live HTTP recovered 9 receives and 7 spends; a separate raw
  block/prevout checker matched every event and its persisted metadata. All 17
  wallet HTTP requests used filter/PIR routes. The account remained inactive.
  CI found one obsolete provenance-fence test constant, corrected at `deac2aed4`;
  its focused check and full CI passed, including every feature/facade mode.
  Follow-up review reproduced and fixed mixed local-send fee suppression and
  crash-before-ack withdrawal loss. All 29 history and six adapter tests passed.
  Full CI passed at `cc6656f23`, and PR 77 merged to main as `9dabaa68`.
  The merged tree matches the verified head. This fixture does not prove
  financial ownership, shielded scanning or final lifecycle qualification.
- [ ] Verify fat-LTO release artifacts and perform guarded SSH canonical cutover with v10 rollback retained.
  The 16-artifact `3cfbc484` fat-LTO build passed. Full-publication verification,
  native certificates and worker transfer/cutover remain open. The repaired
  `a65c618e` fat-LTO owner passed with all 16 artifact hashes verified.
  Its full CI found two further shard-cache sort lints. Equivalent corrections,
  plus filter and test-only lints exposed by the complete transparent group,
  passed strict all-target/all-feature Clippy and the 39.55-second affected check.
  The final `12ce1291` fat-LTO build passed with all 18 retained artifact
  checksums verified. Comprehensive check jobs passed at `12ce1291`; CUDA
  artifact preparation failed because the checkout selected a different compiler
  from the recorded CUDA ABI. Its explicit toolchain selection regression now
  passes; the repaired aggregate CI run remains required. The publisher
  now accepts the new publication root and renders sandbox mounts; 31 focused
  tests, ops contracts and a coordinator hard-link probe passed. Repeat the
  probe under the installed unit. Release bundles now retain all 18 artifacts,
  including the two deployment helpers missing from earlier retention lists.
  Read-only worker plan/preflight through `ops/scripts/wallet-pir-deploy.py`
  passed against the current three-worker fleet; releases remain unstaged.
  Complete schema coordination and coherent rollback through that wrapper,
  with plan/preflight and its production lock, before any production change.
  No manual production unit changes or binary transfers are permitted.
  The wrapper now has a journaled schema recipe runner: 17 failure/recovery tests
  and 45 existing deployment tests passed. Actual trusted phase programs and the
  reviewed production recipe remain open. Wrapper-mediated immutable source
  staging is implemented, including exact archive and Git identities, private
  receipts and a lock held by the process performing the transfer/extraction.
  All 30 schema/staging tests passed. The reviewed operations-only source
  export at `56b67ba0` is now staged on the coordinator; all 269 files were
  reverified, and the root wrapper ran successfully with a pinned inventory
  supplied through stdin. Client archive-size admission and future receipt
  PID capture then passed 31 combined tests. Complete the actual recipe and
  phase programs before the schema transaction.
  The bounded private baseline dependency and deployment-descendant lock
  propagation now have focused recovery/process coverage. Complete reviewed
  per-host target/retention plans, service quiescence, origin withdrawal and
  canonical restore/reopen orchestration remain open; no live baseline or
  schema transaction has been performed by these fixture checks.
  The wrapper now owns a fixed full-publication preparation job, with retained
  release identities, completed-ingest/health-guard gates, six-month cutoff,
  all-artifact verification, resource limits and measured allocation. Focused
  tests passed; it started after ingestion and its guard completed successfully.
  Publisher configuration/state and worker active/cache/publication paths now
  support explicit separate v11 namespaces. Complete trusted cutover/recovery
  orchestration and installed sandbox/SSH descendant checks remain required.
  Concrete host transitions now capture quiesced complete mutable state,
  install bounded checksum-bound product files into v11 namespaces and restore
  prior authority/worker state while deferring routing/load/scaler. Twenty
  focused host tests passed, including surviving-child refusal, namespace/active
  assignment fences and corruption/startup failure recovery. Complete reviewed
  host plans, remote owners/locks, withdrawal/alignment/reopen orchestration,
  independent retrieval and updated load configuration still gate live use.
  Concrete routing/recovery dependencies now guard both origins, retain a
  loopback-only private verification relay, require every reviewed worker and
  accepted retained anchor, bind raw manifest bytes and prevent sealed-history
  rewrites. Private and canonical HTTPS reference recoveries require nonempty
  independently reopened SQLite, all reviewed classes and no unresolved effects.
  Failed public verification withdraws both origins; restored authority starts
  only after its maintenance fence is enabled. This is fixture evidence.
  Coordinated product service phases now bind reviewed transaction templates,
  capture original service states before stopping writers, dispatch pinned
  remote root owners under host locks, seed restart-safe v11 activation/fleet
  records and order all-worker warm proof before authority startup. They align
  filter/load/scaler units, require observe policy and 20% resource floors,
  probe the running publisher mount namespace and restore original router bytes
  before rollback reopening. Exit 75/lost replies interrupt instead of racing
  remote descendants. Generic deployment/source staging now refuse unfinished
  schema ownership. Complete actual SSH descendant qualification, immutable
  candidate input staging, reviewed live plans/recipe and full publication
  certificate/oracle gates remain open. No production baseline or cutover has
  occurred. Partial capture/restore and stale candidate namespace reconciliation
  must be exercised before the actual rollback/redeploy acceptance gate.
  Coordinator preparation now renders native assignment and worker units in an
  immutable inputs directory through plan/preflight/stage. A reproduced raw-map
  versus served-map mismatch is fixed across native assignment, activation and
  routing proofs; worker plans bind both digests. All three portable worker
  subsets and the twelve coordinator service inputs are now staged and
  native/checksum verified. Nonempty predecessor and candidate recovery samples
  are retained; prove complete effects through HTTP and reopened SQLite. Finish
  the actual coordinator/router plans, checksum-bound proof/specification bundles,
  full recipe/preflight and remote surviving-descendant qualification before
  maintenance. Capture the coherent predecessor baseline after quiescing every
  writer, including the filter, and retain whole old publication namespaces.
  Candidate load
  owner: `transparent-activity-observe-5qps-3cfbc484-2`; frozen-table observation
  does not count toward the canonical publication/300-block qualification gate.
  The `a1c4b809` cache oracle passed all 100 dense early-chain blocks and 83,730
  events. Its 16-artifact fat-LTO release build passed and retained hashes.
  Cached/uncached journals are byte-identical. The backfill moved at checkpoint
  324000 to that immutable release binary; its renewed health guard enforces
  the unchanged 20% memory/disk floors. One oracle startup race is retained as
  a failure, alongside the successful retry and immutable raw RPC inputs.
  Earlier full CI found stale operator fixtures and three reference-wallet lint
  findings; the fixture refresh and equivalent lint corrections passed focused
  operator, deployed-jq contract and Clippy checks. Repaired full CI remains open.
- [ ] Qualify and prepare the changed-native candidate `c3c66b9b` through the [candidate path](deployment.md#changed-native-activity-candidate-c3c66b9b).
  Source guards and focused fixture checks are [retained](../evidence/activity-metadata-2026-10-04/README.md).
  The following remain open:
  - Stage the reviewed operations source.
  - Transfer the three retained archives with `schema-candidate-upload-*`. The
    path exists in source; no archive has been transferred.
  - Run candidate bundle plan, preflight and stage from the retained CI 37173250956
    bundles and supplemental archive.
  - Take an immutable journal snapshot with `schema-snapshot-*` for the gates
    that read the journal. The path exists in source; no snapshot has been
    taken. Before it, root must:
    - bind the live controller identity and accept the controller downtime;
    - choose all six bounds, the sidecar policy and the coverage heights;
    - check the live sidecar count and bytes against the disk floor.
  - Produce the four candidate-bound artifact-verification, native-certificate,
    chain-oracle and CI reports from actual runs and retained raw results.
  - Stage and natively verify the candidate worker pair on every worker.
  - Run version-2 product preflight, including installed setups on both recent
    replicas, warm serving proof and the unchanged rollback to captured
    predecessor executables.
  The historical 12ce publication, assignment, samples and receipts stay
  unchanged; no floor or deadline changes.
- [ ] Complete broad verification, six hours and 300 new blocks, nine hours of 8/20/40-wallet capacity runs, and controlled lifecycle/recovery exercises.
  The closed [deployed qualification](deployment.md#deployed-candidate-qualification)
  interface exists in source with focused fixture tests; nothing has run. The
  following remain open:
  - Review the eight-wallet composition, the owner memory budgets and the
    fixed timing bounds.
  - After the candidate transaction commits, run staged load, then freshness,
    then 3+ trials per level, then the six faults, one request at a time.
  - Close the missing-assurance gaps the interface records: quality-alert
    shadow state, cache-corruption injection, and an owner-run probe of the
    rolled-back v10 service. Heavy and interrupted-store continuation use the
    existing `--scenario-worker` protocol, but stay unqualified until real runs
    are reviewed.
  - An owner killed during a fault effect leaves the unit stopped and fenced
    until `schema-qualify-reconcile` restores it.
- [ ] Retain an acceptance decision and capacity recommendation at no more than 50% of measured sustainable completed-sync throughput.

## Server txid display stage (2026-10-01)

Scope and reproducible acceptance command: [txid display](txid-display.md).

- [x] Isolate the server change on `codex/tpir-txid-display`.
- [x] Implement independent display codec, packed directory/pages and worker routing.
- [x] Link canonical extraction and display publication to the existing checkpoint.
- [x] Pass frozen confirmed-vector native HTTP retrieval and internal negative controls.
- [x] Pass codec/fragment bounds, multi-segment native retrieval and sidecar restart/reorg tests.
- [x] Repin to the landed metadata source and pass the final integrated native gates.
- [x] Retain the [stable-source native report](../evidence/txid-display-2026-10-01/README.md).
- [x] Pass the [focused repository check](../evidence/txid-display-2026-10-01/fast-check.json).
- [x] Rebase onto verified main and push only this completed server change.
- [ ] Finish post-push main CI follow-up; report inherited lint failures separately.
- [ ] Qualify and implement the later wallet-libraries/Vizor integration at pinned revisions.
- [ ] Qualify production capacity and release artifacts; obtain deployment approval.

## Txid display v2: senders in fixed-size entries (2026-10-08)

Design: [txid display](txid-display.md). Roman's decisions:
- cover regular cases only, with exotic ones flagged as omissions and an
  explicit public enhancement;
- the first address-shaped source;
- up to two outputs;
- one table per bucket;
- build the code across wallet-pir, wallet-libraries and Vizor before ingest;
- commit to `main`.

- [x] Code on `main`:
  - the entry codec and derivation;
  - extraction from resolved spent outputs;
  - v2x source sidecars that entries derive from, so an entry change is a
    republish;
  - one table per bucket with cuckoo placement;
  - the worker, controller and wallet client;
  - the history-attached path removed.
- [x] Independent oracle: `verify_fixture.py` derives every expected entry
  from raw blocks and parents; `make transparent-txid-demo` checks extraction
  and lookups against it.
- [x] wallet-libraries and Vizor draft PRs: facts, validation, view, and a
  user-initiated public enhancement
  ([zakura-core/wallet-libraries#127](https://github.com/zakura-core/wallet-libraries/pull/127),
  [chainapsis/vizor-wallet#885](https://github.com/chainapsis/vizor-wallet/pull/885),
  stacked on #879). Adversarial review of all three diffs found no blockers;
  its fixes are in both PRs and in `b88bf8a7` (history ingest no longer builds
  display sources).
- [ ] Oracle coverage: the frozen fixture has no entry with mixed funding
  (bit 16) and one with several source scripts. Add a transaction with
  transparent inputs and a positive shielded value balance, so Rust and the
  Python oracle agree on it by bytes, not only by reading.
- [ ] Worker `verify_rows` accepts trailing all-empty segments that the
  builder never emits. Refuse them, since each adds a reply frame to every
  query of that bucket.
- [x] v2 cutover, 2026-10-08: production serves v2 from height 1 on a dedicated
  archive host; v1 is retired and its data deleted
  ([status](status.md#txid-display-v2-in-production-from-genesis-2026-10-08)).
- [ ] Ingest phase (after the code is accepted), measured on a development host
  first:
  - a v2x genesis journal ingested by another session on 2026-10-08 is
    publishable as v2 without a new node ingest. Confirm its sidecar count,
    spot-check it, and derive entries from it with this code;
  - the share of entries with each omission, against Roman's six-month usage
    figures (gate: at least 90% complete);
  - cuckoo load and segments at the chosen `archive_target`, either 40,000
    or about 60,000 now that one table per bucket halves memory;
  - source-sidecar bytes from genesis against the 150 GB volume, with 20%
    headroom.
- [x] G, Roman: approved a direct v2 deploy (no clients), the 64 GB host and
  v1's retirement on 2026-10-08.
- [ ] Re-enable shipped runtimes for v2 once the ship copy no longer worsens
  block-to-serving p95 under the coordinator's I/O load.

## Tiered txid display proof of concept (2026-10-05)

Design and leakage: [tiered display publication](txid-display.md#tiered-display-publication-proof-of-concept).
Roman's decisions: production side by side with history, which stays untouched;
N=1 buckets; no paid infrastructure; commit to `main`.

- [x] Format, seal rule, controller, display worker, reference client, load and
  bandwidth tools, and the `txid-display-*` deploy commands, with package and ops tests.
- [x] [Local evidence](../evidence/txid-display-tiered-2026-10-05/README.md):
  seals and window drops reproduced by `verify`, 0 audit violations, all lookups
  exact, metered bytes equal computed, recent bench and bucket ablation.
- [x] Main CI green at `d191f86b` (CI full run 37388223909);
  `transparent-txid-display` release artifact.
- [x] Production access for the deploying account through the coordinator to
  archive-03, recent-01 and the router; a `wallet-pir-deploy` inventory with pinned `known_hosts`.
- [x] Production change applied with `archive_target` 40,000 and N=1 buckets.
- [x] Deployed in order on 2026-10-06: W0 baseline, `stage`, `ingest-start`, `firewall`,
  `router-hook`, `bootstrap`, `workers`, `route`, `measure-start`, `controller`
  ([status](status.md#tiered-txid-display-in-production-2026-10-07)).
- [x] Synchronous wallet client crate `transparent-txid-client` with
  in-process transcript, error and tamper tests
  ([wallet client](txid-display.md#wallet-client)).
- [ ] Measure each criterion live and write the production results
  ([evidence](../evidence/txid-display-tiered-2026-10-07/README.md),
  [status](status.md#tiered-txid-display-in-production-2026-10-07)):
  - [ ] anonymity: at least 10,000 real txids per queried (shard, bucket); report page-count classes
    (v1 only; v2 entries have none);
  - [x] recent rebuild: at most 20 s from block to serving over at least 300 live blocks, p50 and max.
    **Max failed:** p50 16.0 s, max 97.2 s, 22 of 1,054 live cycles over 20 s;
    Roman accepted the miss for the proof of concept;
  - [ ] archives: sealed digests unchanged across rebuilds and seals;
  - [x] latency: p99 at most 500 ms at the 20 QPS reference, with history p99 against W0.
    20 lookups/s: p99 187 ms, 12,004/12,004 exact; history p99 at most 75 ms (W0 33 ms);
  - [x] bandwidth: under 300 KB per lookup over HTTPS, inline and overflow.
    Holds for inline to 3 pages, cold and warm. 4 pages only warm; 5–7 pages
    exceed it (to 496 KB), 94 of 567,323 txids have 4 or more pages;
  - [ ] growth: seals and drops with no operator action across several boundaries
    (no live seal yet).
- [ ] Roman decides whether to leave it running, `stop` or `retire`.

## Txid display backfill below 3,407,001 (proposed 2026-10-07)

Plan and change sheet:
[deployment](deployment.md#txid-display-backfill-below-3407001-proposed).
Sizing:
[evidence](../evidence/txid-display-backfill-sizing-2026-10-07/README.md).
Roman chose a genesis floor. Without a cutover, the live 24-archive window
starts dropping archives around mid-December 2026.

- [x] Count display-eligible transactions per height range and size each start
  from genesis to 3,000,000.
- [x] Measure runtime memory: `txid-2k` and `txid-4k` hold four-byte matrices,
  44% and 40% below the reservation (synthetic rows).
- [ ] Genesis display ingest: started 16:47 UTC on 2026-10-07
  (`transparent-txid-display-genesis-ingest`), stopped at about 20:00 UTC
  after it latched history's load on coordinator disk headroom. The partial
  journal moved to a dedicated 150 GB volume at `/srv/txid-display-genesis`
  ([status](status.md#tiered-txid-display-in-production-2026-10-07)) and
  completed at 22:56:45 UTC: 3,508,674 blocks. The
  [census](../evidence/txid-display-genesis-census-2026-10-07/README.md) found a
  sidecar for every committed block. Remaining: a spot check.
- [x] Layout analysis on real data
  ([census](../evidence/txid-display-genesis-census-2026-10-07/README.md)).
  Today's `txid-2k` needs 1,325 runtimes and 51.8 GiB, not 850 and 33.2 GiB.
  Shared and smaller page tables do not reach 64 GB; only 80,000-record
  archives before NU6 do.
- [ ] G1, Roman: choose the display-archive host. Recommended:
  `m-16vcpu-128gb` (about $672 a month) with today's layout. The alternative is
  `m-8vcpu-64gb` (about $336), which needs about two weeks of layout work
  (variant C in [deployment](deployment.md#host-and-memory-options-for-genesis))
  and a client release.
- [ ] Ops: raise the 64 GiB `cache_bytes` cap, and count page segments per
  archive in the window check (1,325 runtimes at cutover).
- [x] Code: reservation true-up and `shard-residency` display geometries
  (source only; no release carries them yet).
- [x] Code: split display map, on `main` beside the unchanged full map (source
  only; no release carries it yet); measured in
  [status](status.md#split-txid-display-map-2026-10-07). After a 409 it costs
  about 1.1 KB gzipped at 426 entries, not 0.5 KB.
- [ ] Code: ops caps, `workers --replace-active` and the new archive host;
  Terraform resource; full CI and a release (G2).
- [ ] Retire `/v1/txid/shards` once deployed wallets use the split map; a
  later approved deploy.
- [ ] P0, read-only: real disk-cache entry lengths on archive-03; coordinator
  free disk, inodes and node state.
- [ ] G3, Roman: approve the change sheet. Then run P1–P5: provision, stage,
  ingest from 0, bootstrap and verify, and the cutover.
- [ ] Live acceptance: the existing criteria, plus exact lookups in every era,
  split-map bytes, the host at or above 20% available memory, and history p99
  within W0.
- [ ] G5, Roman: retire the archive-03 display worker and delete the old
  journal and root.

## Txid display sizing and independent routing

Findings and evidence: [sizing and anonymity](../../docs/transparent-txid-sizing-and-anonymity-findings.md).
The boundary is strictly more than 80% of eligible confirmed records inline.
The probability study supports a 128-byte sizing recommendation above 80%;
full-population census and anonymity minima remain **UNQUALIFIED**.

The completed one-working-day study supersedes the exhaustive UTXO/fee/native-capacity
qualification in task `t-01dd8f7354154bb8`. Its 0–30,811 biased-prefix checkpoint,
raw inputs and branches remain preserved; its historical 72-hour stop/resume
procedure is not a prerequisite for this study. The live node executable source
pin remains unavailable through the sanctioned read-only RPC interface.

- [x] Discover documented node/export sources and retain sanitized access/selection outcomes; preserve PR #124's vectors and controls with exact provenance.
- [x] Build a parent-free canonical export path with complete eligibility, shared shielded flags, raw outputs and current display bytes; bound unknown exact-fee encoded length using canonical monetary limits.
- [x] Complete the fixed-anchor, era-stratified whole-range probability sample within the one-day budget; publish clustered uncertainty, all requested cutoffs, 85/90/95/99 frontiers and conservative fee-size membership.
- [x] Select a useful cutoff strictly above 80% from that bounded study and assess independent coarse/hash lookup with global/broad overflow, joint routes, excess counts, thin revisions and timing. Sample estimates do not qualify population anonymity minima.
- [x] Add and locally validate the [read-only coordinator journal census command](../tools/txid-sizing/JOURNAL-CENSUS.md), including safe fee-size page bounds, unique identities, shared k-archive tables and smaller page geometries.
- [ ] Receive Roman's completed genesis-journal JSON at fixed height 3,508,673 with its anchor hash and ingest source/executable and total/eligible/shielded-only exclusion receipt; compare conservative coverage and geometry demand against the existing recommendation. The orchestrator owns the ingest; do not run a gateway scan or resume the old prefix.
- [ ] Remaining full-data gate: obtain sanctioned bulk/archive-local extraction and reconcile canonical genesis-to-anchor eligibility, exact identity and membership. Resolve exact fees only when their encoded-size uncertainty can change a decision. No full UTXO reconstruction is required for display sizing.
- [ ] Before changing production geometry, replay all observable joint routes across retained revisions, segment/request counts, refresh boundaries and relevant timing/history conditioning. Require distinct-real-candidate policy decisions for K=1000/10000; padding, fragments and a global overflow route cannot widen a narrow lookup class.
- [ ] Measure a small representative native workload only where it can change the geometry choice; report reservations and uploaded/returned bytes separately from measured RSS/latency. Exhaustive concurrency/hardware qualification and wallet recovery belong to separate release work.
- [ ] Implement any selected threshold, independent sharding, compact codec/envelope or manifest change in a subsequent authorized production-code task. Keep one logical coordinator and independent display/history geometry; require explicit residual-correlation and policy decisions.

## Release boundary

Use the existing native adapter and Flutter example in Roman's
`/Users/roman/projects/wallet-libraries` checkout of `zakura-core/wallet-libraries`.
The beta supports import/restore, synchronization, balances and history. Sending,
existing-database migration, mobile releases and general production availability
are separate gates. Existing wallet databases and unrelated working changes must
be preserved. Use the existing Amsterdam generator and six-worker fleet; this
plan does not require additional infrastructure.

Use explicit development-demo, validation/shadow and opt-in PIR modes. Native-load
failure must be an actionable error, not a simulated balance. Keep independent
shadow state authoritative until correctness acceptance. Do not persist mnemonic
material or expose secrets in diagnostics; sending stays disabled in UI and API.

## macOS recovery beta milestones

| Milestone | Current disposition | Next dependency |
|---|---|---|
| M0 — Baseline | Accepted; [evidence](../evidence/productionize-m0-2026-09-09/README.md) | None |
| M1 — Fleet | Accepted under the revised six-hour requirement; [decision and raw samples](../evidence/productionize-m1-six-hour-acceptance-2026-09-13/README.md) | Later freshness incident belongs to M5 |
| M2 — Recovery app | Accepted; [evidence](../evidence/productionize-m2-2026-09-09/README.md) | Distribution is M6 |
| M3 — Correctness | Public regression, fixture and native real-wallet comparisons passed; application/lifecycle acceptance incomplete | Finish the concrete work below before authoritative opt-in PIR |
| M4 — Incremental benefit | Open | M3 |
| M5 — Capacity and recovery | Open | M1 + M3 |
| M6 — Release and limited beta | Open | M2–M5 |

Assign named owners in each run: integration/operator for M0, operator for M1,
wallet engineer for M2, wallet engineer and correctness reviewer for M3,
performance/wallet engineers for M4, operator/performance engineer for M5, and
reviewer/release engineer/beta operator for M6. M4 and M5 may proceed independently
after M3; this ordering does not itself authorize parallel agents.

## M3 — Finish the release-candidate validation

The [frozen-executable public regression](../evidence/productionize-m3-suite-fix-2026-09-13/README.md)
passed all eleven cases. Its original five failures and every repaired-run HTTP
attempt are preserved; this public-fixture subgate no longer blocks M3.

- [ ] Adopt the qualified client correction in the wallet candidate and explicitly
  configure its bounded retry policy on all public/filter/shard paths. Update the
  coupled PIR crate revisions together in `zakura/wallet-transparent/Cargo.toml`
  and both HTTP adapters in `zakura/wallet-transparent/src/source.rs`. Merely
  changing the dependency pin does not enable retries: the shared adapter default
  remains one attempt. The current wallet call budget is 120 seconds; preserve
  an explicit total budget across attempts and verify the chosen policy on the
  candidate. Keep accepted-anchor validation and exact encrypted-body replay;
  verify overload, deadline exhaustion, cancellation and incomplete
  progress through native bindings. Record a new wallet/library/app identity.
- [ ] Drive that macOS release application through import, recovery, displayed
  balance/history/coverage, stop, reopen, resume and catch-up in an isolated
  validation profile. Compare exact events, UTXOs and history with independent
  reconstruction at the same accepted anchor. Verify a partial/unavailable
  result never appears synchronized and sending remains disabled. The
  [existing real-wallet run](../evidence/productionize-m3-private-wallet-2026-09-13/README.md)
  exercised the library and bindings, not application screens.
- [ ] Start from a profile with a committed nonempty transparent ledger; observe
  direct request activity, kill during that activity, reopen and resume, then
  compare independently at the final anchor and verify preserved committed state.
  Existing live evidence proves phase-based interruption before the first anchor,
  not this stronger lifecycle condition. Keep fault injection against controlled
  infrastructure; do not manipulate mainnet.
- [ ] Reconcile the final release candidate with the
  [fixture correctness/review record](../evidence/productionize-m3-2026-09-10/README.md)
  and [adapter contract](wallet-adapter.md). Confirm discovery, imported and
  unsupported scripts, fork/reorg and persistence coverage still apply after the
  client change. Publish a requirement-by-requirement M3 acceptance decision.

**Recommended immediate sequence:** pin and configure the qualified client in the
wallet worktree, build the recovery-only candidate, run its binding fault tests,
then use that exact artifact for the application and stronger interruption checks.
The supplied wallet is sufficient for this recent-history trial; broader history
and discovery coverage comes from the mandatory fixtures. M3 closes only when
all required comparisons and lifecycle checks pass on identified artifacts.

**Definition of done:** every mandatory fixture passes without unexplained event,
UTXO or history differences. Restart/resume is idempotent, reorgs remove orphaned
state, and incomplete results preserve progress without committing completion.
Discovery follows actual wallet derivation rules without silently raising birthday.
Unsupported scripts are explicit; no plaintext address/outpoint fallback occurs.
Only passing shadow validation enables opt-in PIR as the synchronized transparent
balance source. Review against the [adapter contract](wallet-adapter.md).

## M4 — Measure whole-wallet benefit

- [ ] Freeze identical wallets, discovery rules and accepted chain inputs for
  shielded-only, combined compact-block and shielded-only-plus-PIR workflows.
  Measure clean restore and retained-store catch-up through enhancement and
  durable complete coverage; keep stores independent.
- [ ] Verify the compact baseline uses the same discovery rescans and actually
  omits `vin`/`vout` when requested; report residual bytes otherwise.
- [ ] Run five alternating-order repetitions per fixture on macOS and repeat the
  concurrent Amsterdam reference workload. Capture actual bytes in both directions, time,
  CPU, sampled memory, writes, cache conditions, errors and incomplete work.
- [ ] Publish exact ledger equality and the disposition of each regression.
  The [80-recovery comparison](../evidence/block-comparison-2026-09-09/README.md)
  supports derived additional-payload savings only; it does not close this gate.

**Definition of done:** combined and PIR workflows recover identical transparent
and shielded results. Reports separate actual transport from representation-size
arithmetic. Use `1 − PIR traffic / (combined bytes − shielded-only bytes)` only
where shared work cancels; otherwise show measured whole-workflow totals. Never
present standalone latency ratios as incremental wallet speedup. Record the cause
and disposition of every regression before beta; do not require a predetermined
performance win to make the measurement valid.

## M5 — Qualify the existing fleet and failure recovery

- [ ] Diagnose the September 12 recent-01 freshness failure after the accepted
  M1 window and the transient 503/reset/closed-body failures retained in M3.
  Correlate worker, router, publication and client timestamps; qualify corrections
  and verify them under sustained publication. Successful retries remain failed
  attempts in availability accounting.
- [ ] Establish the [beta capacity operating point](deployment.md#macos-recovery-beta-acceptance-targets)
  on actual hosts with publication active: the accepted stepped concurrency,
  three sustained repetitions and ordinary-profile trace counts. Include unused,
  recent and old-birthday, multi-script and heavy reused histories with explicit
  heavy-workload budgets. Keep failures in denominators and admitted demand
  within measured headroom; record skew between archive owners.
- [ ] Measure HTTP/TLS warm/cold service, cache residency, retained revision overlap,
  CPU/RSS/cgroup/disk headroom and queue/body budgets on both host tiers. Measure
  one/two/four recent replicas without assuming linear aggregate bandwidth.
  Instrument scrape health and verify freshness/readiness/overload/resource alerts.
- [ ] Rehearse recent-replica loss, archive-owner loss/rebuild, router restart,
  interrupted preparation and failed-deployment rollback, one fault at a time.
  Restore canonical discovery/routing and exact wallet completion within the
  deployment recovery budget. Preserve coherent filter/map/binary rollback and
  artifacts held by existing clients. Record commands and escalation owners.
- [ ] Verify persistent cache/revision collection across the fleet during churn.
  Measure compatible-binary deployment below ten minutes excluding build/CI;
  prove unchanged rollouts cause zero restarts and router-only changes preserve
  service. Historical cache-canary startup measurements do not establish this.
- [ ] Reconcile deployed inventory with infrastructure state, assignments, mounted
  volumes and resource budgets. Inspect any infrastructure plan before applying;
  preserve shared Enhance/node resources. Record actual host identity and cost
  assumptions in the capacity report; no new fleet is required by this beta plan.
- [ ] Publish the supported workload envelope, latency thresholds, outage limits,
  alert evidence and all unresolved findings. Reapply affected M1 gates if a
  release-affecting fleet correction changes its accepted source/configuration.

**Definition of done:** the selected operating point meets the ordinary-profile
thresholds, exact recovery requirements and demand headroom. Queues, retained
request bodies, caches and disk remain bounded. Temporary overload is explicit
and resumable; failed work stays in the denominator. Every outage rehearsal
restores canonical service and exact completion within the beta recovery budget.
Archive-owner loss remains explicit temporary unavailability in this topology.
Failure blocks beta until corrected; it does not authorize new infrastructure.

## M6 — Review, distribute and observe the beta

- [ ] Obtain independent review of the final query/revision binding, completeness
  assumptions, malformed-response handling, retry privacy, resource bounds and
  secret-free diagnostics; resolve every release-blocking finding.
- [ ] Produce a versioned macOS artifact with reproducible sources and signing and
  distribution appropriate to the chosen internal channel. Confirm restore-only
  product choices and default lightwalletd configuration for that artifact;
  document supported workloads and the trusted-indexer boundary.
- [ ] Name release/support/incident owners and exercise rollback that disables PIR
  recovery or reports unavailable without silently changing the privacy model.
- [ ] Complete the [cohort and observation protocol](deployment.md#macos-recovery-beta-acceptance-targets):
  designated validation profiles, restore/restart/resume/catch-up, explained and
  reproducibly resolved failures, and a new observation window after every
  release-affecting fix. Keep sending, existing-profile migration and mobile out
  of this release's scope.

**Definition of done:** no unresolved release-blocking correctness/security finding;
no known mismatch, false completion, data loss or secret-bearing diagnostics.
Each tester completes restore, restart/resume and subsequent catch-up. Every
recovery failure is explained and reproducibly resolved. Release-affecting fixes
restart observation. Final acceptance records source, supported workloads,
capacity, outage limitations, trust assumptions and excluded features.

## Schema v10 qualification and republication

The compact codec, byte-weighted directory placement, greedy page packing and
restart-safe fragment validation are implemented and deployed. The version-2
journal remains unchanged. [Layout evidence](../evidence/compact-layout-2026-09-28/README.md)
records the storage work; [cutover evidence](../evidence/v10-cutover-2026-09-28/README.md)
records production source, failed attempts, public checks and bounded load.

- [x] Build a separate v10 full-chain publication, verify every emitted table and
  filter, check exact selected ledger histories, and retain v9 for rollback.
  All 86 emitted shards match the census; eight selected shards rebuilt exactly.
  This is not an independent all-script audit of event extraction.
- [x] Recompute native correctness certificates on the actual tables. All 172
  pass the configured floor; the 170 sealed public setups match the reports.
- [x] Verify native compact outpoints and the SQLite restart boundary. The
  reference store persists the logical preceding event and fragment byte count.
- [x] Deploy directly over SSH under explicit user authorization and run the full
  public regression and staged bounded load. See the dated evidence for results
  and the metadata maintenance interval; full CI was deliberately bypassed.
- [ ] Run a controlled paired native query/build and whole-wallet timing study;
  the full publication, private prewarm and public load observations are not a
  controlled CPU or latency comparison. Keep all incomplete work in denominators.
- [ ] Qualify downstream wallet consumers and existing-database migration for v10
  and the new publication lineage. The reference client/store is covered by this
  rollout; no application upgrade or release lifecycle acceptance is implied.
  Existing v9 clients fail closed on v10; old pending pages must not be resumed
  against changed shard identifiers.
- [ ] Complete sustained capacity and recovery exercises, including a live
  post-cutover rollback rehearsal. Preserved rollback material alone is not proof
  of fleet recovery. The bounded load run does not close the M5 envelope.

## Schema v9 republication and cluster rollout

The 2026-09-27 six-month architecture review is closed: its client, native-scheme
and operations tracks are implemented, and [architecture](architecture.md)
describes the result. The v9 rollout is historical; v10 now supersedes its publication.
Schema v9, the `zcash-transparent-range-v2` filter profile and the `recent-4k-8k`
geometry all change every manifest, and runtime cache keys include the revision,
so they ride one republication and one cold rebuild of every runtime, archive
included. A v9 publication also breaks any client built before it, including the
wallet-libraries transparent branches. The
[cluster qualification baseline](../evidence/cluster-qualification-2026-09-28/README.md)
is the acceptance gate for each step below.

- [x] Build a candidate full publication into a new directory on the coordinator:
  v9 records and tags, the v2 filter profile, `recent-4k-8k` for the recent tier,
  `--directory-choice all` on every shard, both tiers. Verify with `shard-verify`,
  real placement and exact replay against the journal, and verify every choice
  route at load. Decide whether provisional tails carry tables.
- [ ] (Skipped on 2026-09-28 at the operator's request; the production rollout measured 866–1,187 s cold per archive owner, see the [cutover evidence](../evidence/v9-cutover-2026-09-28/README.md).) Rehearse the cold rebuild on bench copies of one archive owner and one
  recent replica. Measure rebuild time and peak memory per role; that is what
  sizes the maintenance window for the rollout.
- [x] Deploy the candidate: continuous publisher on the v2 profile, fixed-publication
  rollout to all workers, rollback set retained. Port the choice-table lookup and
  the v9 record codec to wallet-libraries before the fleet serves it.
- [ ] Measure the deployed format against the current state in one paired cluster
  run: bytes, request counts and p50/p95 for each workload class.
- [ ] Publish the tail's filter as immutable segments, one per few hundred blocks,
  so a returning wallet downloads only segments it has not cached instead of the
  whole tail filter every revision. Private tables are unchanged. Frequent-sync
  classes should show filter bytes proportional to new blocks, not to tail size.
- [ ] Keep public metadata available during a shadow deploy, or accept and document
  the window. Shadow mode withdrew public metadata for about fourteen minutes on
  2026-09-27; this should be settled before the rollout above.

**Definition of done:** the cluster serves the candidate publication, the
regression and load clients report exact results against it, the paired
measurement is published with its workload mix, and the previous set remains
restorable. Projected savings do not substitute for the measurement.

## Deferred measurement and research

None of these block the recovery beta, and none has an implementation decision.

- [ ] Measure the qualification harness from a remote region and from a
  constrained mobile-class host: p50 and p95 against the emulated projections,
  plus client CPU and memory for choice-table evaluation. Emulated round trips and
  a bench copy are not a WAN measurement.
- [ ] Run a capacity series from a dedicated load generator against the cluster,
  before and again after the republication above, for a sustained operating
  envelope. This is the M5 capacity gate's cluster-side input.
- [ ] Measure cold archive service on one bench host: restore latency and admission
  for `archive-wide` tables, concurrent-restore behaviour, and a draft eviction
  policy. Archive restore latency, admission, eviction and churn are unmeasured,
  which is what a rolling warm window would depend on.
- [ ] Decide bulk-history isolation. The
  [census](../evidence/bulk-isolation-census-2026-09-28/README.md) shows that
  separating long histories would turn fourteen recent shards into five, but the
  gain mostly disappears under `recent-4k-8k`, and a public small/bulk table
  choice reveals a history-size class. It needs that privacy review, bulk geometry
  sizing and a view on six-week tails before it is built or dropped.

## Declared re-cut of sealed recent shards

The goal is to merge a run of sealed recent shards into fewer archive shards and switch
routing to the new map with no downtime, while wallets keep everything they hold. The
map's re-cut declaration, its shape checks and the reference wallet's handling are in
place ([architecture](architecture.md#declared-re-cuts)); the wallet-libraries adapter and
Vizor follow separately. What remains is the server side, and no production map is re-cut
until all of it passes and every client in use reads declarations:

- [ ] A publisher mode that builds the re-cut map from the current one: entries below the
  first changed height hard-linked byte for byte, the span rebuilt in the archive geometry
  with its end on an old sealed boundary, every later sealed shard and the tail renumbered
  at a revision above the one each replaces at its geometry and start height, and a
  declaration of every replaced entry exactly as published.
- [ ] The continuous publisher carries every earlier declaration forward on each
  republication, in rising epochs, and keeps seal parameters for every geometry any
  declaration names. Today it writes an empty `recuts`
  (`transparent/services/transparent-filter-server/src/publication.rs`); a republication
  that dropped a declaration would strand wallets holding the revisions it named.
- [ ] `shard-verify` checks a declaration against the previous map: identical prefix,
  unchanged boundary terminal blocks, exact tiling, declared entries equal to what was
  published, and the revision rule.
- [ ] Prepare and verify the new archive shards on the archive owner and switch the router
  at a publication boundary. A wallet that has synced the re-cut map refuses the earlier
  one, so routing back after the switch stalls such wallets until the re-cut map is served
  again.
- [ ] Re-pin the regression fixtures, whose tool treats renumbering as drift.
- [ ] Acceptance: a bench fleet re-cut under wallet load, with the reference wallet and the
  adapter keeping their history (no private query for held heights, no rollback below the
  replaced tail) and the router switch without failed requests.

## Earlier technical gates and deferred work

Inventory/spot-check/cutoff (Gate 0), mixed census and publication (Gates 1–2),
identity/admission (Gate 3), assignment/publication (Gate 4), and persisted wallet
continuation (Gate 5) have implementation and dated evidence linked from
[status](status.md) and the [evidence index](../evidence/README.md). The remaining
live ledger/application checks map to M3; measured residency, publication
coexistence, host scaling and sustained load map to M4/M5; infrastructure and
rollback (former Gates 6–7) map to M5. This removes stale instructions to provision
or stage a fleet that has already completed M1. It does not turn historical
microbenchmarks or code-only tests into operational acceptance.

The following remain separately scoped; they do not block this recovery beta:

- [ ] Complete Vizor-specific integration if requested; the accepted first native
  integration is Zakura. Account for mixed-pool enhancement and pending state
  in any new wallet's adapter rather than inferring support from this beta.
- [ ] Finish the optional parent-filter paired benchmark on the heavy history:
  establish a baseline completion budget, repeat frozen finalists/seeds under
  equal limits and resolve recent-latency/total-byte gates before wider promotion.
- [ ] Track aged recent-shaped shard growth and re-census before expansion.
  Moving aged recent shards into archive shards is the
  [declared re-cut](#declared-re-cut-of-sealed-recent-shards) below.
- [ ] Scope archive/router redundancy for general availability, mobile, sending
  and seed custody, existing-database migration, key reuse and stronger
  completeness/traffic-pattern guarantees as separate decisions.

Close each item with source/configuration identity, commands, preserved failures
and an acceptance artifact. Update target values only in deployment; delete
superseded prose and validate links. Keep private wallet material outside Git.

## Interfaces, checks and evidence

Extend existing facade/bindings only where needed for recovery mode, explicit
unavailable/incomplete state, cancellation and measurements. Reuse
`TransparentProgress` and store contracts; do not duplicate synchronization state
in Flutter. Keep compact scanning/oracle adapters benchmark-only. No PIR wire
format, geometry change or existing-database migration is required.

Each milestone produces an immutable bundle under `transparent/evidence/` containing exact
source/configuration identities, commands, expected/actual outcomes, raw failures,
metrics coverage and a pass/fail result. For private wallet trials publish only
sanitized evidence; never commit wallet secrets or private histories. Code changes
run repository-required checks. Wallet changes additionally run native tests,
generated-binding checks, Flutter tests and a macOS release build. Documentation
changes run `make check-docs`.


## Service-quality rollout follow-up (2026-09-29)

- [x] Superseded 2026-09-29 at the owner's request: the six-hour / 300-block telemetry canary and gated five-worker rollout were stopped, and recent replicas were rolled to `dbeb3960` without the soak (see [replica membership](../evidence/replica-membership-2026-09-29/README.md)). Archive owners still run the predecessor binary and need a maintenance-window upgrade.
- [ ] Complete full-fleet observation; retain failures and exact-query/configuration provenance.
- [ ] Review 24 hours of complete shadow coverage and exercise firing/recovery through the existing notification outbox.
- [ ] Activate only the new APM quality and independent service-probe families, then observe 24 active hours.

The live dashboard and initial passing probes do not close these elapsed-time gates.

## Track A: elastic recent replicas (approved 2026-09-29)

The recent tier grows and shrinks automatically; the archive stays two static,
manually operated owners with R=1 per range, and its single-owner outage risk is
accepted. Tier-boundary movement (ownership aging, cutoff advance, geometry
epoch) is a separate later design. The steady-state recent tier is two full
copies; four hosts was a load target, not a requirement. Each phase is started
only on the owner's explicit go-ahead.

- [x] **0. Membership correctness.** Every member managed, probes off the routing
  lock, failure hysteresis, write-once plans, prepare grace, `membership.json`,
  live `ready_replicas`, slot metrics, rolling recent upgrades. Production gate:
  4 of 4 recent replicas routed in at least 99% of blocks over 24 hours (open;
  see [status](status.md)).
- [x] **1. Dynamic inventory and pinned placement.** Live since 2026-09-29 13:13 UTC; the first inventory plans reproduced the live worker rows. Durable intent inventory with
  compare-and-swap writes, the archive partition pinned so adding or removing
  recent members never re-cuts archive ranges, v1 assignments only, deploy and
  publisher scripts reading the inventory. Gate: the planner reproduces the live
  assignment; enroll, drain and retire a recent replica on a bench fleet under
  load.
- [x] **1b. Recent tier from four to two.** Done 2026-09-29: two replicas passed at 20.9 QPS (p99 53 ms); recent-03/04 destroyed ([evidence](../evidence/recent-floor-2026-09-29/README.md)). Drain recent-03 and recent-04, run
  20 QPS on the pair (p99 < 2 s, p50 < 700 ms), then retire them and reduce
  `transparent_recent_count`; cancel the drains if the gate fails. Standing gate:
  2 of 2 routed in at least 99% of blocks over 24 hours.
- [x] **2. Elastic recent provisioning.** Validated 2026-09-29: recent-05 created, bootstrapped and routed, then drained and destroyed by the actuator. A recent-only isolated Terraform root
  following the Enhance `enhance-v4` pattern, a journaled actuator and a saved-plan
  validator that refuses archive and production-root addresses.
- [x] **3. Automatic recent failure replacement** Validated 2026-09-29: a stopped elastic replica was replaced make-before-break by the scaler and actuator ([evidence](../evidence/elastic-recent-validation-2026-09-29/README.md)). (make-before-break) and
  capacity-forecast alerts in APM, shadow before active.
- [x] **4. Recent load autoscaler** Validated 2026-09-29 under a low-capacity validation policy (scale-out under load, scale-in after it); production policy in `act` since 14:35 UTC. from a capacity model measured on a bench fleet:
  observe, recommend and act-dry before act.

Invariants for every phase: serving recent replicas never drop below two outside
withdrawal or maintenance; the scaler and actuator never change archive members,
ranges, `recent_from` or archive droplets; stale signals mean hold.

Open observation gates for Track A:

- [ ] 24 hours with both enrolled recent replicas routed in at least 99% of blocks.
- [ ] 24 hours of the APM `scaling` family in shadow, including a real
  fire-and-recover, then `PIR_APM_SCALING_ALERT_MODE=active`.
- [ ] Refine `capacity_qps_per_replica` from a measurement above 8.3 QPS per
  replica; the production value (10) is conservative, not a measured limit.
- [x] Upgrade archive owners (still `0ece0ae1`) in a maintenance window.
  Superseded 2026-09-29: the archive moved to one owner on `a704616c` without
  maintenance ([evidence](../evidence/archive-consolidation-2026-09-29/README.md)).

## Single archive owner (2026-09-29)

The archive runs on one static owner, `transparent-pir-archive-03`, holding
shards 0–76; the single-copy availability risk is accepted in
[deployment](deployment.md#sizing-and-availability).

- [x] Named archive owners in Terraform, `repartition`/`restore`, standby tool,
  cutover, and the combined 20 QPS measurement on both topologies
  ([evidence](../evidence/archive-consolidation-2026-09-29/README.md)).
- [x] Remove `transparent-pir-archive-01` and `-02` after the owner confirms:
  stopped, names dropped from `transparent_archive_names`, destroyed through a
  saved plan of exactly two destroys and one project change (2026-09-30).
  `restore` is no longer possible.
- [x] Set `transparent_worker_deploy_public_key` in the production tfvars
  (same apply; `user_data` changes are ignored, so no droplet changed).
- [ ] Rehearse archive-owner loss and rebuild on the single owner (restart from
  the disk runtime cache and a cold rebuild), with the public effect recorded.

## Txid display shipped runtimes

From the [2026-10-08 deploy](../evidence/txid-display-shipped-runtimes-2026-10-08/README.md)
and the review of `6b8c5c91..d993bc75`. The change is on `main` (`229b9eb9`) and off
by default; v2 does not enable it.

- [ ] Carry the 80 MiB copy within the latency targets before enabling it again:
  measure the runtime files' rsync in isolation on the private network; try
  `--whole-file` for `.runtime` files (no basis to delta against), shipping each
  file as its build finishes instead of after both, or compressing nothing (the
  files are incompressible). Gate: ship p95 under 2 s over 300 cycles.
- [ ] Take the prebuild off the critical path where possible: cache
  `SharedParams` per table kind across controller cycles and reuse
  `publish_parts`' verification instead of `verify_tables` again (about 0.8 s of
  the 2.8 s prebuild).
- [ ] Lower the replica's load-time interference (lookup p99 1.4× the quiet tail
  while loading or receiving): cap the self-check's build-pool threads or run it
  at lower priority, and keep rsync's receiver work off the query threads.
- [ ] Pin the self-check's any-row claim with a test where rows B differ from
  rows A in one row only; use `rand::rng` for the sampled row and say why it must
  be unpredictable to the publisher.
- [ ] Count a refused shipped file once per slot on an `Overloaded` retry.
- [ ] Document in `txid-display.md` that the prebuild runs before
  `deliver_invalidations` (a reorg's stale window grows by `prebuild_ms`) and that
  archive owners receive the recent runtimes at seal boundaries; correct the bench
  README's 48 loads to run 2's 50.
