# V4 provisioning adapter validation — September 23, 2026

The [provisioning adapter](../../ops/scripts/v4-provision.py) connects a pending
infrastructure journal operation to an isolated, preinitialized Terraform root.
It pins module content, backend identity and locking, state lineage, account,
project ownership, VPC region/coordinator address, and established worker IDs and
private origins. It validates an additive saved plan, durably records apply
intent, checks the saved-plan digest again, and requires a final no-change plan
plus matching provider identities before marking resources provisioned.

## Validation

[Fourteen adapter tests](provision-tests.log) passed, covering:

- Successful apply and final reconciliation, with an evidence digest stored in
  the journal; a second invocation does not reapply.
- Restart after the whole fleet was created, without a second apply.
- Partial interrupted apply remaining fenced while newly recorded identities
  survive; no blind retry or replacement.
- Wrong account/project/VPC, lineage, tainted state, provider profiles, changed
  inventory origins, duplicate names, and provider orphans being rejected.
- Unsafe plans, modified saved plans, extra/symlinked module inputs, wrong
  backends, disabled backend locking, and ambient argument overrides.
- Provider pagination origin checks and duplicate resource detection.

These tests use fake provider/command responses and the Terraform-generated mock
plan fixture. They do not invoke DigitalOcean or apply Terraform. The production
API interaction and remote state behavior remain unverified until a live isolated
campaign is run. Automatic import or resumption of ambiguous partial applies is
not implemented; those conditions require explicit recovery.

[All checks](checks.log) passed: 62 Enhance operations tests, 3 filter tests,
4 parent-filter tests, 17 release/tooling tests, and documentation link checks.
Python compilation and `git diff --check` passed as well. The source-only adapter
change did not require rebuilding Rust binaries or repeating PIR load tests.

[Source hashes](source-sha256.json) bind the adapter, guard, journal, tests, and
operator documentation. No credentials were fetched and no remote infrastructure
or service was changed. Bootstrap, qualification receipt verification, inventory
registration, initial fleet deployment, and hardware/load campaigns remain
outstanding in the [implementation status](../../docs/architecture_2-implementation.md).
