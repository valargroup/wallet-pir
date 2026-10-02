# Remaining work for the transparent PIR recovery beta

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
- [ ] Complete broad verification, six hours and 300 new blocks, nine hours of 8/20/40-wallet capacity runs, and controlled lifecycle/recovery exercises.
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
  Epoch re-cutting needs explicit lineage/cache/replay/migration/rollback design.
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
