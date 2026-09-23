# Memory demand to operations journal — September 23, 2026

The HTTP placement/recovery test now invokes the real Python operations CLI
against live coordinator health after an injected reservation memory refusal.
The CLI receives the actual two-group inventory and a synthetic local policy,
adopts the ordinal-zero request for pair three, and persists phase `requested`.
Repeated subprocess invocations leave the journal byte-identical, both before
and after publication recovers. The recovered publication still serves exact
encrypted answers through the selected replica pair.

[HTTP test](http-tests.log): passed in 7.81 seconds.
[Clippy](clippy.log): integration target passed with warnings denied.
Formatting and diff checks passed. The integration test requires Python 3.

The test does not call Terraform, the provider, SSH, bootstrap or qualification.
It verifies the Rust-to-Python demand handoff and durable deduplication, not a
complete provisioning campaign. No infrastructure or credentials were touched.
