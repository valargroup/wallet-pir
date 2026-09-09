<!-- Copied verbatim from docs/transparent-pir/m2-agent-handoff.md as prepared 2026-09-09, with relative links adjusted to this directory. -->

# M2 wallet agent handoff

Prepared 2026-09-09. Assign the receiving agent as M2 wallet engineer and record
its working branch, baseline and final source revisions in the milestone evidence.

## Objective and ownership

Implement M2: deliver a bounded, native macOS recovery-only application using the
existing Zakura wallet adapter and Flutter example. Drive implementation, focused
validation and a real macOS release build to completion. Read the repository's
applicable instructions before editing.

M0 is complete. M1 and M2 both depend on M0 and may proceed independently. M3
requires both M1 and M2. Completing M2 does not authorize production release or
authoritative PIR balances before the independent correctness gate in M3.

Own wallet/application changes. The M1 operator owns the live fleet, coordinator,
Amsterdam generator, canary and acceptance evidence. Do not change their binaries,
configuration, services or load. Use local fixtures/mocks for disruptive and load
tests. Coordinate any live end-to-end validation with the M1 operator so it does
not alter the acceptance workload. Report server/protocol blockers to that operator
rather than deploying a server fix yourself.

## Read first

- [Execution plan: M2 and M3](../../productionize-plan.md).
- [M0 findings and reproduction](../productionize-m0-2026-09-09/README.md).
- [Wallet adapter contract](../../wallet-adapter.md).
- [Remaining work](../../remaining-work.md) and [current status](../../status.md).
- M0's `commands.md`, `wallet-source-baseline.json`, `coverage-probe.rs` and
  `status-probe.dart` in the linked evidence directory.

## Repositories and preservation

Wallet repository: `/Users/roman/projects/wallet-libraries`.
PIR repository and documentation: `/Users/roman/projects/spendability-pir`.

At handoff inspection, wallet HEAD was
`b6aa1f97fd48744f31f129c59c7d287a7c96d5aa`, with no tracked changes and an unrelated
untracked `.vscode/` directory. Recheck before starting. Create a dedicated M2
branch and worktree from the current wallet source. Preserve any subsequent user
changes; do not reset, clean or switch the user's primary checkout.

M0 captured older wallet source, including then-uncommitted changes. Its snapshot
is reproducible historical evidence, not an instruction to replace current work.
Reproduce each finding against the current source; a newer change may already
address it. Record what remains and what is verified as fixed.

Do not open or migrate existing personal wallet databases. Use synthetic test
wallets and a separate beta profile. Never persist mnemonic material or include
seeds, keys, addresses or scripts in ordinary diagnostics. Leave
`docs/roman_notes.md` and unrelated editor configuration untouched.

## Execution sequence

1. **Reconcile the baseline.** Trace current native startup, configuration, modes,
   wallet persistence, coverage display and send entrypoints. Produce a short
   implementation/checklist mapped to M2's definition of done. Make routine
   implementation decisions autonomously; ask about product or custody choices
   that the plan and existing code do not resolve.
2. **Fix wallet persistence.** M0 measured `2N + 2` SQL row changes for one new
   coverage checkpoint after N existing checkpoints. The wallet implements
   `PirStore` through its own `WalletDb::commit_transparent_shard`; changing the
   PIR dependency alone does not apply the reference store's fix. Remove the
   historical rewrite on ordinary append. Preserve atomicity, replay,
   replacement and rollback/reorg semantics. Validate with row-change evidence
   and contract tests, not only elapsed time.
3. **Establish recovery-only boundaries.** Provide explicit development-demo,
   validation/shadow and opt-in recovery modes. Native-load failure in beta must
   fail visibly, never produce simulated balances. Validate endpoint pairing and
   network identity. Isolate the beta database; reject unsupported layouts
   without modifying them. Disable sending in both UI and native API and remove
   placeholder signing from the beta build. PIR opt-in authority remains gated
   on M3; do not silently make it authoritative. Keep shadow state independent.
4. **Present coverage honestly.** Expose accepted target, covered height,
   completion reason, pending work and unresolved spends with balances/history.
   M0 found incomplete reasons suppressed within ten blocks of tip. Recheck the
   current implementation and preserve explicit incomplete reasons. Missing or
   unsupported coverage must never appear as a synchronized zero balance.
5. **Validate native recovery and lifecycle.** Exercise import/restore, sync,
   balance/history, cancellation, restart and resume with disposable fixtures.
   Preserve validated progress on interruption. Test direct native send refusal,
   native-load/configuration errors, database isolation and unsupported layouts.
6. **Build and hand off.** Produce a macOS release build using real native
   bindings and verify the recovery mode and principal application flows.
   Run relevant native, database, Dart and Flutter checks plus required repository
   checks. Record exact commands, results, source revisions and artifact hashes.
   Update M2 status/evidence in coordination with the M1 operator. Do not publish,
   distribute or claim M3 correctness from a successful application build.

## Definition of done and final handoff

Use the M2 definition of done in the execution plan without weakening it:
real native macOS release build; explicit mode; working restore/import, sync,
balances, history and resume; honest partial coverage; native send refusal;
existing databases unchanged; no secrets or wallet identifiers in ordinary
telemetry.

Deliver reviewable commits, the release artifact location/hash, test evidence,
the checkpoint-write comparison, and a requirement-by-requirement completion
record. Identify anything that could not be validated and why. List remaining
M3 fixtures separately; do not mark M2 complete if its own required native flow
or release-build verification is missing.
