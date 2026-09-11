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

## Candidate comparison failed: activation delay

The [complete candidate diagnostic](transfer-reuse-failed.tar.gz) failed at
02:32:15 UTC after 434.583 seconds, three accepted new blocks, 6,582 exact
queries and seven retries. Public visibility at height 3,479,181 was 32.882
seconds. Recent-01 hardlink staging was consistently about 0.45–0.52 seconds,
but preparation took 8.0–9.45 seconds and one activation-phase SSH operation
took 9.782 seconds after preparation. Controller freshness reached 31.568
seconds. The connection improvement alone does not satisfy the gate.

[Local probes](transfer-reuse-failure-probes.json) during 02:31:50–02:32:20
remained fast: 60 samples per path, no errors, maxima 4.76 ms Unix and 10.55 ms
HTTP. The local HTTP map changed only at 02:32:14.368, following preparation
completion around 02:32:03. The worker persists and synchronizes its active
record before switching the public map. Slow persistence is a hypothesis to
trace, not a proven cause.

Activation/attestation timings now identify each worker and router separately.
[Operations tests](activation-timing-operations-tests.log) pass all 100 tests;
[full make check](activation-timing-make-check.log) passes. A bounded
[worker fsync trace](worker-activation-fsync.bt) targets PID 695393 for fifteen
minutes and reports calls longer than ten milliseconds with their opened paths.
No full rollout started, and the failed diagnostic contributes no acceptance.

The [activation-timing diagnostic](activation-timing-diagnostic.tar.gz) completed
at 02:50:53 UTC on source `2932a60`: 600.026 seconds, five blocks and 9,362
exact queries. Maximum public visibility was 20.574 seconds. It does not establish a fix: final trace analysis below found a long
activation even though this short diagnostic stayed within its freshness limit. The first traced
activations were 23 and 84 ms on recent-01; the initial router update took
1.357 seconds.

The [completed fsync trace](worker-activation-fsync.log) and
[completed record-I/O trace](worker-record-io-v2.log) both terminated successfully
(MainPID 0, ExecMainStatus 0). The record-I/O probe's initial version failed
loading; the committed v2 attached nine probes and ran to its bounded end.

[Final correlation](slow-activation-correlation.json) identifies publication
`38050c2bb632ff417d13e92b61f087c309ce5deb32aca6c6d19d6ce485233b0e`:
recent-01 activation took 7.732 seconds at 02:48:48–02:48:55 UTC. Its active.tmp
fsync took 6.171892 seconds and the following parent-directory fsync took
1.544899 seconds, accounting for 7.716791 seconds. An overlapping cache-file
fsync took 3.277513 seconds; cache writes also slowed during the same interval.
This directly attributes the activation pause to durability barriers. Cache
write interference is a supported hypothesis, not yet an isolated causal result.
The earlier initial-trace maximum of 579 ms missed this later event.

The next source candidate paces optional background cache persistence with a
`sync_data` barrier after each 8 MiB before accepting more bytes. It keeps the
checksum, final file fsync, atomic rename and directory fsync, and does not
change active-record persistence. It adds no allocation or new dependency.
A byte/checksum regression crosses multiple writeback boundaries. Source tests
and Linux performance qualification must pass before deployment; no reduction
in latency or completed M1 gate is claimed from this implementation alone.

Linux qualification is running under
`transparent-m1-incremental-writeback-build.service` on the existing coordinator,
started 03:04:49 UTC (PID 2834345). Its immutable source manifest, test output
and artifacts are under
`/opt/transparent-publisher-build/incremental-writeback-20260911/`.
The existing Amsterdam fixture is
`/opt/transparent-full-fixture-20260909/fixture-canonical.json`; qualification
uses three repetitions, two build slots, separate query clients and the existing
14-second worker screening budget. This screening does not replace the public
30-second freshness gate or the sustained fleet observation.

[Full local make check](incremental-writeback-make-check.log) passed, including 591 Rust tests, zero failures and two ignored. [All 100 operations tests](incremental-writeback-operations-tests.log) passed. Linux qualification remains separate.
