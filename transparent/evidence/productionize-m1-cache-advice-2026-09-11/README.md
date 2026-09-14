# M1 background cache-advice candidate

The [instrumented loading experiment](https://github.com/valargroup/enhance-pir/blob/42b5f9c145cc3f2a400c428938c566d2cc9699db/docs/transparent-pir/evidence/productionize-m1-loading-diagnostics-2026-09-11/README.md)
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
leaves eviction to the kernel. The candidate is deployed to recent-01 for qualification; it is not M1 acceptance.
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

The longer diagnostic on the prior binary was intentionally interrupted after
capturing the blocking call, to begin candidate qualification. Its Linux build and integration compilation ran on the
coordinator concurrently with that reproduction experiment, not on recent-01.

## Guarded upgrade and loaded qualification

The [deployment script](deploy.py) checks source/artifact identities, completed
Linux validation and the prior observer's terminal state before upgrading only
recent-01. Unit `transparent-m1-cache-advice-deploy.service`, PID 3849428, is
now terminal with exit status zero. Output is under
`/opt/transparent-publisher-build/cache-advice-20260911/candidate-upgrade`.
The [deployment bundle](deployment.tar.gz) records the successful guarded upgrade
at 14:24:23 UTC, including private-query and warm-state verification. The public
endpoints were reopened. No other worker was upgraded.

A 30-minute loaded qualification began at approximately 14:26:00 UTC under
`transparent-m1-cache-advice-screen.service`, PID 3850092. It uses two exact-query
clients and the unchanged freshness and memory budgets, with output under the
candidate build root at `qualification`. The [status helper](qualification-status.py)
targets that exact process. Expected completion is approximately 14:56 UTC;
RuntimeMaxSec is 1,900 and Restart is no. The first live sample had 29.48% host
memory available and both query streams producing exact results. This short
screen has no M1 acceptance credit and no automatic fleet rollout. If it passes,
start a fresh matching six-hour/300-block canary.

## Terminal qualification failure

The screen failed at 2026-09-11 14:34:16 UTC after 496.804 seconds:
`public freshness exceeded at block 3479763: 30.342s exceeds 30s`.
The [full failed screen](failed-qualification.tar.gz) contains 7,294 exact
responses, zero mismatches, three retries and seven completed blocks. Minimum
sampled host memory headroom was 26.498%, with no new routing withdrawals.
The screen unit is terminal; its time earns no acceptance credit.

The [worker log](failure-worker.log) records a 10.405-second wait for the runtime
cache writer lock in collection, followed by loading of 1.256 seconds and warming
of 5.168 seconds for map
`7314cfbdb501364e58019752c9ebd692e6897f945ffae036965ab44a95b808ae`.
The [controller log](failure-controller.log) records publication at 14:34:17 UTC
with freshness 30.443 seconds. The background-advice change did not eliminate
this separate collection wait. Investigate deferring optional collection when
its writer lock is busy without weakening writer capacity bounds or revision
retention guarantees. No acceptance canary has been started.
