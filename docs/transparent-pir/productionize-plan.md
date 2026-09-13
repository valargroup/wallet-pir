# Transparent PIR productionization execution plan

Agreed 2026-09-09. Target: an **opt-in, recovery-only macOS beta**, beginning with
shadow validation and isolated new profiles. This is an execution specification,
not a deployment or acceptance record. Completion is tracked in
[remaining work](remaining-work.md#macos-recovery-beta-milestones).

## Outcome and boundaries

Use the existing native adapter and Flutter example in Roman's
`/Users/roman/projects/wallet-libraries` checkout of `zakura-core/wallet-libraries`.
The beta supports import/restore, synchronization, balances and history. Sending,
existing-database migration, mobile releases and general production availability
are separate gates. Existing wallet databases and unrelated working changes must
be preserved. Use the existing Amsterdam generator and six-worker fleet; this
plan does not require additional infrastructure.

The [paired benchmark](evidence/block-comparison-2026-09-09/README.md) passed 80
exact recoveries. Its raw fresh-suite representation sizes and measured PIR
upload plus download imply **99.19% less additional transparent payload** when
compared with combined compact bytes minus shielded-only compact bytes. This is
a derived bandwidth comparison, not a measurement of incremental whole-wallet
latency, sustainable capacity or production availability.

This document owns execution order and milestone definitions. [Deployment](deployment.md)
owns operating targets; [status](status.md) owns observed state;
[evidence](evidence/README.md) owns measurements. Existing technical gates remain
applicable: these milestones organize their closure rather than replace them.

## Execution order

| Milestone | Dependency | Responsible role | Required output |
|---|---|---|---|
| M0: Baseline | None | Integration engineer + operator | Capability matrix and source/deployment inventory |
| M1: Fleet acceptance | M0 | Operator | Matching canary, fleet rollout and sustained observation results |
| M2: Recovery application | M0 | Wallet engineer | Native macOS recovery-only release candidate |
| M3: Wallet correctness | M1 + M2 | Wallet engineer + correctness reviewer | Independent ledger/history comparison and lifecycle results |
| M4: Incremental benefit | M3 | Performance + wallet engineers | Matched whole-wallet workflow comparison |
| M5: Capacity and recovery | M1 + M3 | Operator + performance engineer | Accepted operating envelope and failure rehearsals |
| M6: Limited beta | M2–M5 | Reviewer + release engineer + beta operator | Release review and completed beta observation |

M1 and M2 can proceed independently after M0; M4 and M5 can proceed independently
after M3. Assign a named person to each role in the milestone evidence before
execution. This ordering does not itself launch parallel agents.

## M0 — Establish the exact baseline

**Execute**

- Trace the existing demo from native bindings through recovery, persistence,
  providers and displayed balances. Classify each capability as implemented,
  locally tested, live verified or missing.
- Inventory uncommitted wallet changes and establish a reproducible integration
  baseline without resetting, discarding or automatically committing unrelated work.
- Record the client pin. Determine whether the reference SQLite append correction
  applies to the wallet's own store; measure its checkpoint write behavior rather
  than assuming the fix transferred.
- Retrieve the existing supervised rollout state and raw result before considering
  a restart. Record source and binary hashes, configuration, readiness, public
  authority and an independently accepted chain anchor.

**Definition of done:** every later run can identify its source, configuration and
output directory; the native path, endpoint configuration and persistence behavior
are documented; existing work is preserved. Current rollout acceptance is established
from matching evidence or explicitly unverified. Documentation alone cannot pass M0.

## M1 — Accept sustained publication and fleet operation

**Execute**

- M1 is accepted under the user-confirmed six-hour post-rollout requirement;
  see [current status](status.md#m1-accepted-observation)
  and the [dated acceptance record](evidence/productionize-m1-six-hour-acceptance-2026-09-13/README.md). The matching loaded canary,
  corrected six-worker rollout and first six hours/300 blocks per worker passed.
  The original 24-hour supervisor's later failure is preserved as an M5 follow-up.
  Apply the [hardening rollout gate](deployment.md#hardening-rollout-gate)
  to future release-affecting changes; do not restart the completed run.
- On failure, preserve the closed raw samples, query logs, terminal results and
  component identities. Diagnose and qualify the correction before restarting
  the affected acceptance stage. Failed, interrupted and diagnostic runs earn
  no acceptance credit outside the explicitly recorded six-hour transition; bounded HTTP retries do not extend freshness deadlines.
- On success, audit all six terminal results and both sustained query-client logs.
  Verify freshness, exactness, canonical readiness, routing withdrawals, memory,
  restarts/OOMs, managed preparation, cache reclamation and revision retention.
  Reconcile worker source/binary, operations, router, monitor and configuration
  provenance across the canary, rollout and observation; do not infer matching
  acceptance from a successful exit code alone.
- Preserve the final raw evidence, record conclusions in status and close the
  applicable [remaining-work gates](remaining-work.md). Keep capacity and failure
  rehearsals assigned to their original milestones; this observation does not
  establish the M5 operating envelope or authorize general production availability.

**Definition of done:** the complete canary duration and block-count gates pass,
followed by the complete all-worker observation. Public and replica freshness,
canonical readiness, memory headroom and no-unexpected-restart/OOM requirements
meet deployment targets. Every advertised worker is warm on the current authority
before reopening. Preserve failed attempts and scrape gaps. Relevant source or
configuration changes invalidate the affected acceptance result.

## M2 — Deliver a bounded native recovery application

**Execute**

- Reuse the current adapter and Flutter example. Provide explicit development-demo,
  validation/shadow and opt-in PIR recovery modes.
- Beta native-load failure must show an actionable error, never simulated balances.
  Validate endpoints and network identity; missing configuration is not empty history.
- Use a separate beta database location and reject unsupported layouts without
  modifying existing wallets. Disable sending in both UI and native API; remove
  the placeholder-signing path from the beta build.
- Do not persist mnemonic material. Retain only the existing viewing/derivation
  material needed for recovery and exclude secrets from diagnostics.
- Expose accepted target, covered height, completion reason, pending work and
  unresolved spends coherently with balance/history. Support cancellation and
  restart/resume without deleting validated progress.

**Definition of done:** a macOS release build uses real native bindings and identifies
its recovery mode. Restore/import, sync, balances, history and resume work end to end.
Partial or unsupported coverage never appears as a synchronized zero balance.
Direct API calls cannot send. Existing databases are unchanged. Ordinary telemetry
contains no seeds, keys, addresses or scripts.

## M3 — Establish wallet correctness before authoritative PIR use

**Execute**

- Complete the deployed accepted-anchor regression suite; the 20-wallet benchmark
  does not replace it. Compare application results with independent block/journal
  reconstruction at the same independently accepted anchor.
- Compare exact events, UTXOs and transaction history, including amounts and spend
  linkage. Include unused/zero-balance histories, coinbase, self-transfer, multiple
  scripts, old receives spent recently, offline receive/spend, imported scripts
  and gap-limit discovery.
- Inject interruption around persistence boundaries, network loss, overload,
  revision replacement and corrupt responses. Exercise same-height forks and
  cross-tier reorgs on controlled infrastructure, not by manipulating mainnet.
- During shadow validation keep independent state authoritative and separate from
  PIR state: neither path may repair or contaminate the other.

**Definition of done:** every mandatory fixture passes without unexplained event,
UTXO or history differences. Restart/resume is idempotent, reorgs remove orphaned
state, and incomplete results preserve progress without committing completion.
Discovery follows actual wallet derivation rules without silently raising birthday.
Unsupported scripts are explicit; no plaintext address/outpoint fallback occurs.
Only passing shadow validation enables opt-in PIR as the synchronized transparent
balance source. Review against the [adapter contract](wallet-adapter.md).

## M4 — Measure the incremental wallet benefit

**Execute**

- Extend the benchmark around the existing wallet engine, using separate stores
  for shielded-only sync, combined shielded/transparent compact sync, and
  shielded-only sync plus PIR. Freeze identical wallet and chain inputs.
- The compact baseline must implement the same discovery rules, including required
  historical rescans when scripts are discovered later. Verify the actual service
  omits `vin`/`vout` where requested; report residual bytes otherwise.
- Time complete workflows through accepted coverage, required transaction
  enhancement and durable state. Run five alternating-order repetitions per
  fixture on macOS: clean stores for restores, retained stores for catch-up.
  Repeat the concurrent reference workload from Amsterdam.
- Capture actual upload/download, wall time, CPU, sampled memory and process writes.
  Preserve all repetitions, failures, cache conditions and machine details.

**Definition of done:** combined and PIR workflows recover identical transparent
and shielded results. Reports separate actual transport from representation-size
arithmetic. Use `1 − PIR traffic / (combined bytes − shielded-only bytes)` only
where shared work cancels; otherwise show measured whole-workflow totals. Never
present standalone latency ratios as incremental wallet speedup. Record the cause
and disposition of every regression before beta; do not require a predetermined
performance win to make the measurement valid.

## M5 — Establish capacity and failure-recovery limits

**Execute**

- Use the [beta capacity protocol](deployment.md#macos-recovery-beta-acceptance-targets)
  on Amsterdam with publication active. Freeze thresholds before each run; do not
  relax them in response to failures. Keep unusually heavy reused histories in a
  separately budgeted, resumable class.
- Rehearse recent-replica loss, archive-owner interruption, router restart,
  interrupted preparation and failed-deployment rollback, one fault at a time.
- Add missing service CPU/RSS and scrape-health instrumentation; exercise alerts
  for freshness, readiness, overload and memory/disk pressure. Record recovery
  commands, timings and escalation owners in the operator runbook.

**Definition of done:** the selected operating point meets the ordinary-profile
thresholds, exact recovery requirements and demand headroom. Queues, retained
request bodies, caches and disk remain bounded. Temporary overload is explicit
and resumable; failed work stays in the denominator. Every outage rehearsal
restores canonical service and exact completion within the beta recovery budget.
Archive-owner loss remains explicit temporary unavailability in this topology.
Failure blocks beta until corrected; it does not authorize new infrastructure.

## M6 — Release review and limited beta

**Execute**

- Obtain independent review of query/revision binding, completeness assumptions,
  malformed responses, resource exhaustion, logs and retry privacy. State the
  [trust and privacy boundary](contract.md): PIR does not prove index completeness
  or eliminate all traffic-pattern leakage.
- Build a versioned macOS release artifact with reproducible inputs and the
  signing/distribution requirements of the selected internal channel.
- Follow the [beta cohort and observation targets](deployment.md#macos-recovery-beta-acceptance-targets).
  Use isolated profiles. Run shadow comparisons only on designated validation
  profiles; opt-in PIR profiles must not silently run another transparent path.
- Rollback disables new PIR recovery or returns explicit unavailable status; it
  must not silently change the privacy model. Name support and incident owners.

**Definition of done:** no unresolved release-blocking correctness/security finding;
no known mismatch, false completion, data loss or secret-bearing diagnostics.
Each tester completes restore, restart/resume and subsequent catch-up. Every
recovery failure is explained and reproducibly resolved. Release-affecting fixes
restart observation. Final acceptance records source, supported workloads,
capacity, outage limitations, trust assumptions and excluded features.

## Interfaces, checks and evidence

Extend existing facade/bindings only where needed for recovery mode, explicit
unavailable/incomplete state, cancellation and measurements. Reuse
`TransparentProgress` and store contracts; do not duplicate synchronization state
in Flutter. Keep compact scanning/oracle adapters benchmark-only. No PIR wire
format, geometry change or existing-database migration is required.

Each milestone produces an immutable bundle under `evidence/` containing exact
source/configuration identities, commands, expected/actual outcomes, raw failures,
metrics coverage and a pass/fail result. For private wallet trials publish only
sanitized evidence; never commit wallet secrets or private histories. Code changes
run repository-required checks. Wallet changes additionally run native tests,
generated-binding checks, Flutter tests and a macOS release build. Documentation
changes run `make check-docs`.

M6 acceptance completes this plan. Mobile, sending/seed custody, database migration,
archive/router redundancy for general availability and stronger completeness
guarantees remain separate follow-on work, not implicit beta capabilities.
