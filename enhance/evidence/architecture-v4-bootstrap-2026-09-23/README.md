# V4 worker bootstrap validation — September 23, 2026

The candidate bundle now contains a [host-local bootstrap installer](../../ops/scripts/bootstrap-worker.py).
It verifies a trusted checksum-manifest digest, the full artifact inventory,
clean-source candidate metadata and Linux x86-64 ELF format. It checks measured
host RAM/available RAM, CPU count, disk, swap and cgroup support against explicit
candidate limits before installing a dedicated non-root service and immutable
binary. The receipt remains `qualification: unqualified`.

## Checks

[Bootstrap tests](bootstrap-tests.log): **11 passed, 1 skipped**. Coverage includes
bundle integrity and release-packaging compatibility, rejection of dirty/foreign
artifacts, measured memory and host-overhead limits, private-address service
configuration, repeated installation, changed-input/assigned-state rejection,
cleanup after partial startup, and effective cgroup limit verification. A
readiness failure after completed bootstrap does not stop the existing service.

Filesystem installation tests use a temporary root and mocked account, systemd,
health, and runtime inspection calls. They establish orchestration behavior, not
successful Linux service execution. The Linux-only `systemd-analyze verify` test
was skipped on macOS; it is included in operations test discovery for Linux CI.
The Docker CLI is present locally, but its daemon was not running, so no Linux
container test was performed.

[The complete checks](checks.log) ran 74 Enhance operations tests: **73 passed,
1 skipped**. The 17 release/tooling tests, 3 filter tests, 4 parent-filter tests,
and documentation checks passed. Python compilation and `git diff --check`
also passed. No Rust source changed and no new load result is claimed.

A retry after successful bootstrap verifies the current service without a
restart/stop. An interrupted initial installation can resume only with identical
identity and limits. Changes to an established installation require a separate
upgrade procedure. No production service template or inventory was modified.

Actual Linux installation, remote transfer/orchestration, hardware qualification,
receipt verification, and automatic registration remain outstanding. No cloud
resource, host account, or running service was changed by these tests. Refer to
[bootstrap instructions](../../docs/qualification.md) and the
[implementation status](../../docs/qualification.md) for the
remaining gates. [Source hashes](source-sha256.json) bind this change's inputs.
