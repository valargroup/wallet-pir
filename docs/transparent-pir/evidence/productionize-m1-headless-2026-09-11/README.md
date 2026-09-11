# Persistent headless worker configuration — 2026-09-11

The [runtime console comparison](../productionize-m1-control-path-failure-2026-09-11/README.md)
motivates the optional `headless_console` fleet setting. The installer stages a
small helper and preflights it without changing console state. On activation,
a privileged worker ExecStartPre runs the helper before the worker starts,
including on reboot. An enabled serial console is required; unknown framebuffer
drivers are refused before writes. Existing helper and worker unit are backed
up for binary rollback. Runtime graphics are not automatically rebound by a
rollback; see the deployment contract.

Read-only observer checks require the expected helper digest, the loaded
pre-start command, boot enablement, serial access and unbound framebuffer state.
The helper digest is included in canary provenance and the full-fleet admission
check. No timing, memory, duration, block or exact-query gate is relaxed.

The [operations suite](m1-headless-tests-final.log) passed 97 tests. New coverage
includes read-only preflight, missing serial/unknown driver refusal before
writes, idempotent unbinding, malformed bindings, boot/load configuration,
changed helper and console rejection, staged-only installation, installed
verification and matching helper provenance for fleet promotion.
[Full `make check`](m1-headless-make-check.log) passed: 590 Rust tests, zero
failures, two ignored, with formatting, lint, documentation and report checks.

[Linux preflight](m1-headless-linux-preflight.log) passed on recent-01. The
subsequent persistence check intentionally failed because the runtime-only
experiment lacked the loaded pre-start hook. This confirms the gate rejects
that incomplete state. [All six workers](fleet-preflight.json) passed read-only
preflight with an enabled serial console and virtio framebuffer. Only recent-01
was unbound from the earlier experiment; the other five were not changed.

The helper SHA-256 is
`1daa5418f1dc9a510e839f032c2aed6b062395bd855d5b6a729db563f4cf44df`.
This is source/preflight evidence. Persistent installation and a fresh full
acceptance run are still required; source checks alone do not close M1.
