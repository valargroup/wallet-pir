# M1 collection lock regression — 2026-09-10

The worker collection path waited for the runtime-cache `.lock` while holding
its retired-snapshot write lock. Status needs that snapshot lock. Collection
also performed synchronous filesystem work on a Tokio executor thread.

`blocked_collection_keeps_status_and_readiness_responsive` holds the real disk
cache writer lock, starts collection, then requires concurrent status/readiness
to respond within 500 ms. It failed against parent `4223200` with
`disk collection blocked status/readiness` (0 passed, 1 failed, 0.93 s).
The first correction passed (1 passed, 0 failed, 0.44 s). The final test also
cancels collection's caller and verifies that later mutation remains serialized
until the writer releases its lock.

Collection now owns its mutation guard in a blocking job, snapshots retention
under the serving locks, then releases those locks before dropping removed
states, pruning runtime files or deleting unused publication directories.
Retention rules and acceptance budgets are unchanged. Cancellation cannot let a
later publication race the remaining disk work.

This establishes a reproducible availability defect, not proof that it explains
every production pause. The separate live diagnostic run remains on worker
`f2f351c`; no corrected binary has been deployed from this evidence record.
M1 still requires matching qualification, six hours AND 300 new blocks, fleet
rollout and 24-hour fleet observation.

Final focused release validation: 16 passed, zero failed, two ignored manual
benchmarks; see `regression-tests.log`. Workspace `make check` passed: 588 Rust tests, zero failures and two ignored
manual benchmarks, plus formatting, Clippy, operations, docs and report checks.
See `make-check.log`.

`source.tar.gz` and `source-manifest.json` preserve the exact Linux build inputs;
`build.py` builds compatible worker and qualification binaries. The bounded
`transparent-m1-collection-build.service` is running on the existing coordinator
with CPU quota 200%, nice 15 and a 6 GiB memory ceiling. Linux qualification and
production deployment remain pending at this capture.
