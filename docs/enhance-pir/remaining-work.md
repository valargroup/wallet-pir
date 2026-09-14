# Enhance remaining work

This checklist tracks qualification of the implemented c-4 expansion target.
The [deployment runbook](deployment.md) owns exact limits and procedures;
[status](status.md) records the latest committed observations.

- [ ] Identify the release artifact, deployed schema and intended wallet client;
  verify schema-7 conformance and preparation receipts before any cutover.
- [ ] Qualify the exact candidate on isolated c-4 workers for six hours and 300
  publications, including memory/swap, exact answers, retained sessions,
  replica failure, online range-boundary append, and publication lag.
- [ ] Assemble the combined qualification receipt from the complete evidence.
  A short preflight or fixture summary alone does not grant acceptance.
- [ ] Perform the coordinated initial rollout with old inventory and binaries
  available for rollback; complete the 24-hour production observation with
  auto-provisioning disabled.
- [ ] After acceptance, inspect the narrowly scoped legacy-worker retirement
  plan, retire the old pair, and enable expansion only after its live prerequisites pass.

If an operator explicitly waives qualification or initial observation, use the
[documented operator-acceptance receipt](deployment.md#explicit-operator-acceptance).
Record waived gates as waived, never as passing. This cleanup does not grant a waiver.
