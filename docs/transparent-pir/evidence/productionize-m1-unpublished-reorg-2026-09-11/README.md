# Unpublished reorg correction

The [failed writeback canary](../productionize-m1-incremental-writeback-2026-09-11/README.md)
withdrew valid older coverage while preparing a block replaced by a one-block
fork. The [baseline regression](baseline-regression.log) reproduces unconditional
withdrawal on a journal-only fork before any new publication is advertised.

The controller now requests retention only when the fork begins above its
current served endpoint and that endpoint is not already withdrawn. Under the
publication gate it increments the preparation epoch and persists the revocation
intent. The fleet must explicitly acknowledge retention; errors or older adapters
without the acknowledgment fail closed. Restart with an unfinished revocation
intent still withdraws conservatively.

Under the routing lock, the fleet checks that its active map matches the
controller's requested map; the recorded endpoint is below the fork and matches
the node; the currently routed set has an owner/recent quorum; and every routed
worker attests that warm map. Otherwise it withdraws before revocation. A rejected
retention hint causes all revisions to be rechecked for a potentially deeper
fork. Failed revocation of a retained routed worker also withdraws. The existing
durable routing audit is unchanged.

Explicit invalidation now always reaches the worker, even when all served and
retired revisions remain canonical: that cancels an orphaned candidate's epoch
without revoking the canonical predecessor. Regular staging retains the existing
conditional orphan-revocation behavior.

Regressions cover journal-only forks, a fork while preparation is paused,
continued public HTTP availability, rejection of the obsolete candidate, deep
served forks with immediate withdrawal before slow ancestor lookup, failed owner
revocation, and a stale retention hint after fleet activation. The existing
worker regression verifies that a fork above the predecessor invalidates a
prepared candidate's epoch. [All 101 operations tests](operations-tests.log) pass.
The controller concurrency regression also passes in the full workspace run;
complete workspace and Linux qualification results are recorded when finished.

The [source manifest](source-manifest.json) binds the staged Linux build at
`/opt/transparent-publisher-build/unpublished-reorg-20260911/` under
`transparent-m1-unpublished-reorg-build.service`. No corrected controller or
fleet script is deployed yet; the failed canary remains stopped. A new matching
full gate is required after deployment. This correction does not establish
resolution of the separate close-block freshness margin.

[Full make check](make-check.log) passed: 591 Rust tests, zero failures, two ignored, with formatting, Clippy, operations and documentation checks. The separate final 101-test operations run covers the final fleet changes. Linux qualification remains outstanding.

## Linux qualification and deployment

[Linux controller tests](linux-controller-tests.log) passed, including the
concurrent unpublished-fork and deep-reorg regression. The build completed
successfully at 03:54:56 UTC. [Artifact digests](artifact-sha256.json) bind the
release controller to the staged source manifest.

The [deployment procedure](deploy-qualified.py) verified staged hashes and
predecessors, backed up the controller, fleet script and controller config,
restarted only the controller and reconciler, and required equal public maps,
a warm matching recent-01 and a served endpoint matching the node's current tip.
It retained rollback handling until verification succeeded. The
[deployment result](deployment-result.json) records both predecessor and installed
identities. Controller source/config source is `862239c`; the unchanged worker
remains the qualified `7621c34` writeback binary. The deployment unit completed
successfully and started a fresh supervisor.

[Start samples](start-samples.ndjson) establish the new canary at 03:56:41 UTC,
with routing withdrawal baseline 9. The [initial checkpoint](initial-checkpoint.json)
at 03:57:19 confirms `transparent-m1-unpublished-reorg-rollout.service` active
under PID 2877535 in canary observation, with 630 exact queries and no retries
or mismatches. No new block had yet been counted at that checkpoint; zero timing
maxima are absence of samples, not zero publication latency. Raw output is under
`/opt/transparent-publisher-build/unpublished-reorg-20260911/rollout/`.
No failed-run time is reused. Both six hours and 300 new blocks, the matching
fleet rollout and 24-hour fleet observation remain mandatory and incomplete.

The [quarter-hour checkpoint](quarter-hour-checkpoint.json), captured at
04:12:44 UTC after 962 seconds, confirms the same active supervisor: 13 new
blocks, 14,419 exact queries, 13 retries, no mismatches and unchanged routing
withdrawal baseline 9. Maximum public visibility is 22.106 seconds and canary
visibility 24.834 seconds; minimum sampled available memory is 26.664%. One cache
save was pending in the latest sample, with zero write failures. The observer
recorded one publication change between endpoint reads without a stable mismatch.
This remains partial observation, not M1 acceptance.

The [half-hour checkpoint](half-hour-checkpoint.json), captured at 04:27:20 UTC
after 1,838 seconds, confirms the same active supervisor and source identity:
26 new blocks, 27,386 exact queries, 25 retries and zero mismatches. Routing
withdrawals remain at baseline 9. Maximum public and canary visibility remain
22.106 and 24.834 seconds respectively. The latest cache sample has no pending
saves and zero write failures. This checkpoint does not satisfy the six-hour,
300-block canary gate or the subsequent all-worker observation.

The [first-hour checkpoint](hour-checkpoint.json), captured at 04:57:06 UTC
after 3,625 seconds, confirms the same active supervisor and source identity:
45 new blocks, 54,700 exact queries, 55 retries and zero mismatches. Routing
withdrawals remain at baseline 9. Maximum public and canary visibility remain
22.106 and 24.834 seconds respectively. Minimum sampled available memory is
26.038%; the latest cache sample has no pending saves and zero write failures.
This is partial evidence only: the six-hour/300-block canary and subsequent
all-worker rollout and 24-hour observation remain incomplete.

The [two-hour checkpoint](two-hour-checkpoint.json), captured at 05:56:55 UTC
after 7,214 seconds, confirms the same active supervisor and source identity:
84 new blocks, 109,091 exact queries, 116 retries and zero mismatches. Routing
withdrawals remain at baseline 9. Maximum public visibility is 24.908 seconds
and canary visibility is 25.967 seconds. Minimum sampled available memory is
25.646%; the latest cache sample has no pending saves and zero write failures.
Two publication changes between endpoint reads have been recorded. This remains
partial evidence: both six hours and 300 new blocks, followed by the matching
fleet rollout and 24-hour observation, are still required.
