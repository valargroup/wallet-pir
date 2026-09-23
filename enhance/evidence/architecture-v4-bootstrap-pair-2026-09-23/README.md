# V4 pair bootstrap and Linux validation — September 23, 2026

The [pair-bootstrap driver](../../ops/scripts/v4-bootstrap-pair.py) connects
provisioned resources to verified artifact transfer and host bootstrap. It reuses
the provisioning adapter's live account/project/VPC/state checks, selects the
new pair by recorded IDs and private origins, requires pinned SSH host keys,
verifies transferred checksums before executing bundle code, and records each
validated replica receipt durably. Bootstrap remains explicitly unqualified.

## Validation

[Seven pair-driver tests](pair-tests.log) passed. They cover partial-pair failure
and restart, frozen configuration/target bindings, receipt identity and limits,
duplicate worker-process rejection, target selection, strict host-key checks and
argument quoting, and refusing execution when transfer verification fails. Provider
and SSH operations in these tests are synthetic. They do not prove live bootstrap.

[Local checks](local-checks.log) ran 81 Enhance operations tests: **80 passed,
1 Linux-only test skipped**. All 17 release/tooling tests, 3 filter tests, and
4 parent-filter tests passed. Documentation links and diff whitespace checks passed.

Docker Desktop was started for Linux validation. The first readiness probe timed
out during startup; a subsequent probe confirmed the same daemon became ready.
A disposable Ubuntu 24.04 container then ran **all 47 v4 operations tests with
no skips**, including `systemd-analyze verify` on the generated service.
[The Linux test log](linux-tests.log) and [image identity](linux-environment.json)
record the result. The container was removed on exit and mounted the repository
read-only. This was an arm64 Linux environment for Python and unit-parser testing;
it did not run the x86-64 worker binary, a systemd-managed worker, or hardware
qualification. Host installation effects in the tests remain mocked.

Live SSH transfer, actual service startup, qualification campaigns and receipt
verification, coordinator inventory registration, and production deployment
remain outstanding. No cloud resource, remote host, credential store, production
service, or production inventory was changed. [Source hashes](source-sha256.json)
identify the driver, shared provider checks, tests, and operator instructions.
See the [implementation status](../../docs/architecture_2-implementation.md)
for the complete remaining release gates.
