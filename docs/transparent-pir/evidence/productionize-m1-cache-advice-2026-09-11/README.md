# M1 background cache-advice candidate

The [instrumented loading experiment](../productionize-m1-loading-diagnostics-2026-09-11/README.md)
captured a 3.771-second cache-advice call waiting on write submission while
publication loading waited for it. This candidate moves optional Linux cache
advice to one background thread with at most four queued file descriptors and
one active call. Submission never waits for advice or queue space; saturation
skips the optional hint. The queue owns cloned descriptors, so later path removal
or replacement cannot redirect advice. Integrity verification, admission limits
and durability barriers are unchanged.

This bounds background work and descriptor retention, not kernel file-cache
residency. Actual-worker memory headroom and publication latency still need
qualification. A slow background call can delay reclamation, and skipped advice
leaves eviction to the kernel. This is not yet a deployed fix or M1 acceptance.
All 321 manifest hashes were verified against committed source
`be1e5759d89e62fc5801944cfd8d635626cb1199`; see [source identity](source-commit.json).

The [build bundle](build.tar.gz) records the hashes of all 321 source files,
Linux build/test commands and resulting artifact hashes. The worker binary is
`674b7c927c9cd0c3b31389389129cd23df857926575a8c273c5240eebd940aa5`;
the Linux library test executable is
`a8dd7c7f94d306394391619d37bb96661c75b0bb6a2cf802a1a4340ae5ffc72a`.
All 38 Linux worker library tests passed. The build completed at 14:16 UTC.
The [local full check](make-check.log) exited successfully: 593 Rust tests passed,
zero failed, two ignored, plus the remaining required checks.

The added tests hold the advice action blocked while checking caller completion
and queue saturation, check descriptor identity across removal/replacement, and
exercise actual Linux advice for unchanged contents and file position.
The [Linux integration suites](linux-integration.log) passed: `assignment` had
eight passes, and `revisions_and_cache` had 18 passes with two manual burst
benchmarks ignored. The integration unit exited successfully.

The longer diagnostic remains on the prior instrumented binary; this candidate
has not been deployed. Its Linux build and integration compilation ran on the
coordinator concurrently with that reproduction experiment, not on recent-01.
