# Local deployment-runtime validation — 2026-09-08

**Offline checks passed. Live canary and fleet timing acceptance remain open.**

The [manifest](manifest.json) records the tested revision, commands, host and
limitations. `make check` passed, followed by strict Clippy, the cache tests and
six offline rollout test groups. Raw logs: [full checks](make-check.txt),
[cache tests](runtime-tests.txt), [rollout tests](rollout-tests.txt),
[Clippy](clippy.txt). Captured logs normalize trailing blank lines.

| Geometry/table | Cold build | Disk restore | Cache bytes |
|---|---:|---:|---:|
| recent-8k directory | 1.122 s | 0.394 s | 134185024 |
| recent-8k pages | 0.745 s | 0.339 s | 134185024 |
| archive-wide directory | 1.600 s | 0.661 s | 234848320 |
| archive-wide pages | 2.029 s | 1.093 s | 369066048 |

These are one-shot Apple M4 Max measurements with immediate post-write reads.
The tests verify identical public setup bytes, response bytes and decrypted rows
at both deployed geometries. They also exercise revision/segment/source mismatch,
truncation, checksum failure, atomic replacement, exhausted cache budgets,
retention, coalesced restoration after restart and corrupted source tables.

Restore timing covers `DiskCache::load`, not the additional source-table
verification performed by `RuntimeCache::get`. This is correctness and local
feasibility evidence, not a fleet deploy-time or capacity result. The test machine
was shared with other work and the two cache tests ran concurrently.

The rollout harness runs the real remote activation and rollback scripts against
a temporary filesystem and simulated systemd. It covers unchanged decisions,
configuration drift, helper-only updates, group health limits, pair ordering,
failed-batch joining, interrupted activation, restoration of previous files and
release markers, and preservation of the last rollback target on a no-op.

Read-only live preflight observed archive-01 with 176252690432 bytes available on
a 206900281344-byte root filesystem, no systemd drop-ins, and 160/160 runtimes
warm. Recent-01 reported 28/28 warm. The owner's release marker was `8802cd0`; both readiness responses reported the
same map at inspection. A fleet load test was active; no activation was attempted.
These observations do not close the live rollout gates in
[remaining work](../../remaining-work.md).
