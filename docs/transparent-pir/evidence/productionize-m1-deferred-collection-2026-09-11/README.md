# M1 deferred live cache collection

The [background-advice qualification](../productionize-m1-cache-advice-2026-09-11/README.md)
failed public freshness after live collection waited 10.405 seconds for a
snapshot writer's disk-cache lock. This candidate makes live disk collection use
`try_lock`: a busy writer returns an explicit `disk_collection_deferred: true`
with zero freed bytes. The next collection retries. Startup pruning still waits
for the lock, and the snapshot writer still enforces the same disk-byte ceiling
under that lock. Busy collection never deletes a writer's partial file or a
retained revision. If cache space is exhausted before reclamation, optional
snapshot writes are refused rather than exceeding the budget.

The [build bundle](build.tar.gz) includes hashes of 321 source files, the Linux
build commands, library and integration results, and artifact hashes. Worker
SHA256 is `200ca85065c8096d344749d5e51a2db569ec369c71bd2ffff8cf0e9fd014ff62`.
All 39 Linux worker library tests and 26 Linux integration tests passed; two
manual burst benchmarks were ignored. The [full local check](make-check.log)
exited successfully: 594 Rust tests passed, zero failed, two ignored, together
with the other required checks.

The regression holds the disk writer lock and requires collection, status,
readiness and a following mutation to complete before releasing it. It then
requires nondeferred collection. A separate test checks no files are removed
while deferred and only unretained files are removed after release.

The [initial local validation](initial-make-check-failure.log) and
[initial Linux validation](initial-validation.tar.gz) failed the existing churn
test's assumption of immediate final collection: it saw 12 cache files instead
of eight while optional writes were still finishing. The corrected test checks
the disk-byte ceiling each churn cycle, waits for outstanding writes, and then
requires exactly the same eight retained files after collection. It passed both
local and Linux validation. This adjusts the test to explicit deferral without
loosening eventual cleanup or capacity assertions.

The candidate has not yet been deployed. Actual-worker qualification and the
complete matching M1 canary/fleet observation remain required. Prior failed or
interrupted observations earn no acceptance credit.
