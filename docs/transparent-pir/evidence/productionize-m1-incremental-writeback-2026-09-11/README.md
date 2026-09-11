# Incremental runtime-cache writeback qualification

Source `7621c34` paces optional background runtime snapshot writes with a
`sync_data` barrier after each 8 MiB before accepting more bytes. It preserves
checksums, final file synchronization, atomic rename and directory synchronization.
Active publication durability is unchanged. The [preceding trace evidence](../productionize-m1-stage-timing-2026-09-11/README.md)
attributes a 7.73-second activation to durable-record synchronization concurrent
with slow cache I/O. This candidate targets that interference; isolated success
does not prove the live tail-latency issue resolved.

## Qualification

The preceding evidence folder preserves all 591 passing Rust tests and 100
operations tests. [Linux tests](linux-tests.log) passed, including restored
runtime answer equality at deployed geometries. [Artifact digests](artifact-sha256.json)
and the [source-file manifest](source-manifest.json) bind these results.

The [complete Amsterdam archive](amsterdam-qualification.tar.gz) contains all
three runs and raw cgroup/client reports. The unit
`transparent-m1-incremental-writeback-qualification.service` completed successfully
at 03:12:03 UTC (MainPID 0, ExecMainStatus 0). Each run used the frozen 14-shard
fixture, two build slots, separate exact query clients, four worker CPUs,
MemoryHigh 5.5 GiB, MemoryMax 7 GiB and no swap. The worker screening budget was
14 seconds. All three passed with 182 total exact queries, complete cache saves,
no OOM and maximum publication time 8.876 seconds.

This invocation accidentally used the runner's 512 MiB host-overhead default.
The [accepted summary](qualification-summary.json) preserves that original
result and adds headroom recalculated from the same measured kernel peaks with
the preceding qualification's stricter 768 MiB allowance. Minimum corrected
modeled headroom is 25.963%, above the required 20%. This calculation does not
change cgroup enforcement or establish live-host memory acceptance.

## Fresh canary rollout

The [start record](rollout-start.json) records supervisor launch at 03:12:40 UTC.
`transparent-m1-incremental-writeback-rollout.service` was confirmed active with
PID 2836142 in the canary-upgrade phase. Its output directory is
`/opt/transparent-publisher-build/incremental-writeback-20260911/rollout`.
The worker candidate digest is
`b2da68f289e4eebd3eafbc5b763963fa87c3daf347b8b2d312e1ac64ea97a38c`.
The live operations script, fleet configuration and headless helper match the
recorded predecessor. Worker source changed, so no earlier time is reused.

The supervisor upgrades recent-01, checks warm canonical service and exact
queries, then requires both six hours and 300 new blocks. Only a passing matching
result permits the remaining fleet rollout and separate 24-hour observation.
M1 is not complete; launch is not acceptance evidence.

[Canary upgrade evidence](canary-upgrade.tar.gz) confirms successful installation
and exact canonical queries. [Start samples](start-samples.ndjson) establish
03:13:32 UTC at node height 3,479,215, with routing-availability baseline 8 after
the authorized maintenance window. The [initial checkpoint](initial-checkpoint.json)
at 03:14:19 confirms the supervisor active in canary observation, 730 exact
queries, no retries or mismatches, and no result yet. These are startup facts,
not a sustained pass. Initial restoration had 28 cache hits, no misses or write
failures and no pending saves.

At the [15-minute checkpoint](quarter-hour-checkpoint.json), the same supervisor
remained active after 928 seconds: 19 new blocks, 13,378 exact queries, five
retries, no mismatches and unchanged routing withdrawal baseline. Maximum public
visibility was 29.059 seconds, close to the 30-second ceiling. Minimum sampled
available-memory fraction was 27.171%; two pending cache saves were observed and
had drained by the latest sample, with no write failures. The [capture script](capture-checkpoint.py)
reads the live unit and raw monitor data; this partial checkpoint is not a pass.

## Terminal canary failure: unpublished candidate reorg

The [complete run](failed-canary.tar.gz) failed at 03:32:05 UTC after
1,112.910 seconds and 21 new blocks: public routing was unavailable. The unit
is terminal failed (MainPID 0, ExecMainStatus 1). It completed 16,101 exact
queries with six retries and no mismatches. Routing withdrawal count increased
from 8 to 9; service subsequently recovered. No fleet promotion occurred and
none of this run counts toward a replacement acceptance gate.

The [controller journal](failure-journal.log) shows preparation of height
3,479,237 followed by `candidate invalidated while preparing`; controller status
reported a reorg depth of one. The last observed served endpoint was 3,479,236.
A subsequent [canonical-hash check](served-endpoint-canonical.json) matches its
recorded hash exactly. This supports an unpublished-suffix reorg, not evidence
of the earlier slow-fsync incident. The later hash check alone cannot prove the
absence of transient intermediate forks; preserve that timing limitation.

Source inspection finds two unconditional withdrawal decisions:
`controller::invalidate` marks the authority withdrawn even for journal-only
forks, and fleet `_invalidate` routes to an empty set before selective revision
revocation. The existing worker invalidation supports retaining canonical
revisions and invalidating the preparation epoch. The next correction must
separate cancellation of orphaned candidates from withdrawal of invalid served
coverage, without creating an activation race.

Execution path:

1. Add deterministic regressions for a fork strictly above served coverage,
   including one during preparation. Assert continued canonical public coverage
   and refusal to activate the orphaned candidate.
2. Keep a separate regression for a fork touching the served endpoint: immediate
   withdrawal and durable orphan refusal remain mandatory. Cover failed remote
   revocation/retry and a reorg racing activation.
3. Coordinate controller invalidation, fleet routing and worker epochs. Preserve
   routing only after validating the publication being retained under the
   relevant mutation locks; uncertainty must still fail closed. Do not bypass
   the routing audit or relax acceptance thresholds.
4. Run controller, fleet and worker regressions plus full qualification, then
   deploy matching artifacts and begin a fresh canary. Keep this failed run as
   evidence; no automatic retry of the unchanged failed gate has been started.
