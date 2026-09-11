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
