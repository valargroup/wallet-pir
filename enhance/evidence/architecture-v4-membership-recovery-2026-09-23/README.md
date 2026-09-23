# Interrupted membership recovery — September 23, 2026

When all requested droplets have verified state/provider/journal identities, an
interrupted apply with only project-membership creates remaining can be resolved
as retryable. Reconciliation persists evidence and performs no apply. A later
invocation obtains a fresh plan; recovery cannot widen to droplet, tag or firewall
creation. Missing workers, orphans and conflicting identities remain fenced.

Validation uses the mocked provider and Terraform adapter, not live resources:

- Membership-only interruption records a durable retryable resolution and every
  worker ID without applying; the next invocation can complete reconciliation.
- An unfinished firewall preserves the original apply fence.
- Firewall drift on the fresh retry plan is rejected before recording an apply.
- Existing partial-worker, orphan, duplicate, profile, lineage and replacement
  rejection tests continue to pass.

[Regression results](tests.log): 108 enhance operations tests ran, 106 passed and
two Linux-only tests skipped; all 17 release/tooling tests and both filter suites
(3 and 4 tests) passed. Diff checks passed.

No credentials were accessed, provider requests sent, Terraform state mutated or
infrastructure provisioned. Partial/orphan worker recovery and live interruption
campaigns remain outstanding. This is not deployment or qualification evidence.
