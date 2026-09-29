# Remaining work for the transparent PIR recovery beta

Updated 2026-09-28. This owns milestone definitions, execution order and the
authoritative outstanding checklist for the opt-in, recovery-only macOS beta. [Status](status.md) summarizes accepted
work; [deployment](deployment.md) owns all operating thresholds. Historical gate
numbers are reconciled below rather than retained as a second release checklist.

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

- [ ] Accept the fresh six-hour / 300-block telemetry canary and gated five-worker rollout.
- [ ] Complete full-fleet observation; retain failures and exact-query/configuration provenance.
- [ ] Review 24 hours of complete shadow coverage and exercise firing/recovery through the existing notification outbox.
- [ ] Activate only the new APM quality and independent service-probe families, then observe 24 active hours.

The live dashboard and initial passing probes do not close these elapsed-time gates.
