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
