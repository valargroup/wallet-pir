# Remaining work for the transparent PIR recovery beta

Updated 2026-09-13. This is the authoritative outstanding checklist for the
[execution plan](productionize-plan.md). [Status](status.md) summarizes accepted
work; [deployment](deployment.md) owns all operating thresholds. Historical gate
numbers are reconciled below rather than retained as a second release checklist.

## macOS recovery beta milestones

| Milestone | Current disposition | Next dependency |
|---|---|---|
| M0 — Baseline | Accepted; [evidence](evidence/productionize-m0-2026-09-09/README.md) | None |
| M1 — Fleet | Accepted under the revised six-hour requirement; [decision and raw samples](evidence/productionize-m1-six-hour-acceptance-2026-09-13/README.md) | Later freshness incident belongs to M5 |
| M2 — Recovery app | Accepted; [evidence](evidence/productionize-m2-2026-09-09/README.md) | Distribution is M6 |
| M3 — Correctness | Public regression, fixture and native real-wallet comparisons passed; application/lifecycle acceptance incomplete | Finish the concrete work below before authoritative opt-in PIR |
| M4 — Incremental benefit | Open | M3 |
| M5 — Capacity and recovery | Open | M1 + M3 |
| M6 — Release and limited beta | Open | M2–M5 |

## M3 — Finish the release-candidate validation

The [frozen-executable public regression](evidence/productionize-m3-suite-fix-2026-09-13/README.md)
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
  [existing real-wallet run](evidence/productionize-m3-private-wallet-2026-09-13/README.md)
  exercised the library and bindings, not application screens.
- [ ] Start from a profile with a committed nonempty transparent ledger; observe
  direct request activity, kill during that activity, reopen and resume, then
  compare independently at the final anchor and verify preserved committed state.
  Existing live evidence proves phase-based interruption before the first anchor,
  not this stronger lifecycle condition. Keep fault injection against controlled
  infrastructure; do not manipulate mainnet.
- [ ] Reconcile the final release candidate with the
  [fixture correctness/review record](evidence/productionize-m3-2026-09-10/README.md)
  and [adapter contract](wallet-adapter.md). Confirm discovery, imported and
  unsupported scripts, fork/reorg and persistence coverage still apply after the
  client change. Publish a requirement-by-requirement M3 acceptance decision.

**Recommended immediate sequence:** pin and configure the qualified client in the
wallet worktree, build the recovery-only candidate, run its binding fault tests,
then use that exact artifact for the application and stronger interruption checks.
The supplied wallet is sufficient for this recent-history trial; broader history
and discovery coverage comes from the mandatory fixtures. M3 closes only when
all required comparisons and lifecycle checks pass on identified artifacts.

## M4 — Measure whole-wallet benefit

- [ ] Freeze identical wallets, discovery rules and accepted chain inputs for
  shielded-only, combined compact-block and shielded-only-plus-PIR workflows.
  Measure clean restore and retained-store catch-up through enhancement and
  durable complete coverage; keep stores independent.
- [ ] Run the plan's alternating-order repetitions on macOS and the concurrent
  Amsterdam reference workload. Capture actual bytes in both directions, time,
  CPU, sampled memory, writes, cache conditions, errors and incomplete work.
- [ ] Publish exact ledger equality and the disposition of each regression.
  The [80-recovery comparison](evidence/block-comparison-2026-09-09/README.md)
  supports derived additional-payload savings only; it does not close this gate.

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

## Earlier technical gates and deferred work

Inventory/spot-check/cutoff (Gate 0), mixed census and publication (Gates 1–2),
identity/admission (Gate 3), assignment/publication (Gate 4), and persisted wallet
continuation (Gate 5) have implementation and dated evidence linked from
[status](status.md) and the [evidence index](evidence/README.md). The remaining
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
- [ ] Evaluate a new `recent-4k-8k` profile without changing existing registry
  meanings; require measured byte and tail-latency benefit before promotion.
- [ ] Track aged recent-shaped shard growth and re-census before expansion.
  Epoch re-cutting needs explicit lineage/cache/replay/migration/rollback design.
- [ ] Scope archive/router redundancy for general availability, mobile, sending
  and seed custody, existing-database migration, key reuse and stronger
  completeness/traffic-pattern guarantees as separate decisions.

Close each item with source/configuration identity, commands, preserved failures
and an acceptance artifact. Update target values only in deployment; delete
superseded prose and validate links. Keep private wallet material outside Git.
