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

## Deployment and fresh acceptance

Source `a5f79ed760e458b873b5382a1e4243dda632d1f7` matches all 321 build-source
hashes. The [deployment bundle](deployment.tar.gz) records the source identity,
guarded recent-01 upgrade, query/warm-state verification and acceptance launch.
The canary upgrade passed and public endpoints reopened.

A fresh supervised acceptance run started at approximately 14:54:32 UTC under
`transparent-m1-defer-collection-rollout.service`, PID 3869401, with output at
`/opt/transparent-publisher-build/defer-collection-v2-20260911/rollout`.
The [status helper](status.py) targets that run. It reuses the just-verified
installed binary, but no prior samples: the supervisor requires both six hours
and 300 new blocks with two exact-query clients before permitting the fleet
upgrade, then a separate 24-hour fleet observation. RuntimeMaxSec is 172,800;
Restart is no. The first live poll confirmed the canary-observation phase and
both query streams producing exact answers.

The existing bounded maintenance deferral was not extended. Its scheduled
restoration may occur during full-fleet observation; do not describe the entire
run as protected from maintenance. All normal restart, freshness and identity
gates remain in force. M1 remains open; prior failed or interrupted observations
earn no acceptance credit.

## Early acceptance checkpoint

At 15:10:14 UTC, the [checkpoint](fifteen-minute-checkpoint.json) confirmed the
same supervisor PID 3869401 live in canary observation after 942.646 seconds.
It recorded 12 new blocks, 14,298 exact responses, zero mismatches and nine
retries. Maximum public/replica visibility was 15.215/13.463 seconds; minimum
available host RAM was 25.605%, with no new routing withdrawals. This is an
intermediate checkpoint, not a completed canary result.

## Completed canary and fleet upgrade attempt

The [final canary result](passed-canary-result.json) passed at 21:10:44 UTC:
22,572.362 seconds, 300 public and replica blocks, and 336,603 exact responses.
The [raw canary bundle](passed-canary.tar.gz) contains both query streams,
samples and the monitor log. Query streams recorded zero mismatches. Maximum
public/replica visibility was 22.046/22.049 seconds; the observed minimum
available-memory fraction was 0.2455177. This completes the canary gate only.

The supervisor then entered fleet upgrade. That attempt entered guarded
rollback at approximately 21:27 UTC while archive-02 was still prewarming.
Its installed unit lacked a persistent runtime cache and used one build slot;
it had reached 117/160 warm runtimes at 21:26:55 without prewarm failures.
At 21:33:47 the supervisor was still live in rollback verification, and
archive-02 was rebuilding on its predecessor binary at 47/160 runtimes.
The final upgrade/rollback result is pending. No 24-hour fleet observation
has started, and this canary pass does not close M1.
