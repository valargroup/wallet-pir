# M1 publication-stage timing investigation

The [headless canary failure](../productionize-m1-headless-2026-09-11/README.md)
showed public visibility at 30.001 seconds while local readiness stayed fast.
Existing fleet command timings cannot attribute slow SSH/rsync operations to a
host or staging phase. Structured `worker_stage` events now identify worker,
host, publication digest, phase, start and outcome. They cover status, orphan
revocation, collection, inventory, hardlinks, transfer, assignment and prepare.
Exceptions and cancellations propagate unchanged; their type is recorded without
logging command arguments, credentials or payloads.

[Operations tests](operations-tests.log) pass all 99 tests, including concurrent
identity isolation and cancellation/error propagation. [Full make check](make-check.log)
passes all 590 Rust tests with two ignored; the new targeted test invocation was
also run after adding it to `check-ops`.

Investigation sequence: attribute staging delays by host and phase during live
publication; correlate those durations with controller queueing and public-read
timing; change the measured bottleneck; qualify the correction before starting
a fresh matching acceptance gate. Diagnostic observations never count as soak
acceptance, and no timing threshold is relaxed.

[Deployment](deployment.json) at 02:11:55 UTC installed source `58a5db4`
on the coordinator and restarted only the reconciler (PID 2777929). Worker
binaries and fleet configuration were unchanged. The previous fleet script is
backed up under `/opt/transparent-publisher-build/stage-timing-20260911/`.
The ten-minute `transparent-m1-stage-timing-diagnostic.service` uses the existing
observer and two query clients with the unchanged freshness limits; its output
is `stage-timing-20260911/diagnostic` under the same build root. It is diagnostic
only and cannot satisfy the six-hour gate.

## Candidate: reuse supervised SSH connections for staging

File staging previously used independently expiring 60-second automatic SSH
masters even when the fleet already supervised authenticated worker connections.
The candidate selects the supervised connection for worker shell operations and
rsync whenever `control_sessions` is enabled. A missing master fails without
silent fresh-login fallback. Explicit direct mode and non-worker destinations
retain their previous connection selection. Multiplexed staging output uses
private temporary files so cancellation does not wait on master-owned pipes.

[Candidate operations tests](transfer-reuse-operations-tests.log) pass all 100
tests, including worker selection, explicit direct mode, disabled sessions,
non-worker destinations and no fallback on failure. [Candidate make check](transfer-reuse-make-check.log)
passes all 590 Rust tests with two ignored. This is qualified source; a live
comparison and fresh acceptance run are still required.

The [complete baseline diagnostic](baseline-diagnostic.tar.gz) finished at
02:22:48 UTC: 600.299 seconds, nine blocks, 8,646 exact queries, maximum
public visibility 20.733 seconds. It is a ten-minute diagnostic only.

[Candidate deployment](transfer-reuse-deployment.json) at 02:24:43 UTC installed
source `9e674d3` and restarted only the reconciler. The live fleet script digest
is `d1001a2039e9e92dcd4268efa2b086868393f2f7a4d9a90562d3dec7bb9c17f6`.
The unchanged worker runs under a new ten-minute diagnostic,
`transparent-m1-transfer-reuse-diagnostic.service` (PID 2797229 confirmed active).
Output is `/opt/transparent-publisher-build/transfer-reuse-20260911/diagnostic`.
Compare stage durations and readiness under load before a new full gate; no
acceptance time is reused.
