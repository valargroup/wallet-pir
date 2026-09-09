# M1 fleet acceptance investigation — 2026-09-09

Operator: Codex acting for Roman. **M1 remains incomplete.** The existing managed
canary failed; the supervisor did not promote the other five workers. No new
acceptance run is active as a result of this investigation.

## Retrieved evidence

The complete existing run is preserved in `failed-run-3.tar.gz`, including both
sustained query logs, observer samples and supervisor output. `manifest.json`
records hashes, source and acceptance thresholds. The extracted terminal result
is `canary-result.json`; the supervisor outcome is `supervisor-status.json`.

The run failed at 2026-09-09 01:29:55.843 UTC after **2,090.910 seconds and 23
blocks**. The observer measured block 3,476,836 overdue at 30.719 seconds against
the unchanged 30-second public-visibility budget. Neither the six-hour nor the
300-block requirement passed.

## Confirmed latency failure

`failure-controller.log` independently confirms the missed deadline:

| Publication height | Activation UTC | Controller cycle | Freshness from node observation |
|---|---|---|---|
| 3,476,835 | 01:29:41.513 | 17.300 s | 17.330 s |
| 3,476,836 | 01:29:56.044 | 14.509 s | 30.842 s |
| 3,476,837 | 01:30:17.656 | 16.295 s | 16.350 s |

The first two blocks arrived about one second apart. `controller.rs` awaits each
complete build/prepare/activate cycle before capturing its next journal snapshot.
The second block therefore waited behind the first cycle, then incurred its own
cycle. This is actual publication delay, not merely observer sampling error.

`worker-prewarm-window.json` isolates the principal observed preparation cost:
recent-04 took 10.407 and 9.933 seconds; recent-02/03 took roughly 13–14 seconds;
the loaded recent-01 canary took 18.257 and 23.336 seconds. Unchanged archive
prewarming took less than a millisecond. The public quorum can activate before
the canary catches up, so canary preparation time must not be equated directly
with public cycle time. Old workers log cumulative warm counters; use readiness
rather than those counters to assess current residency.

The observer samples immediately before failure show no canary restart or OOM,
and about 2.21 GB available host memory. These samples do not establish long-term
memory acceptance. No evidence identifies archive preparation as the bottleneck.

## Current inventory and access

`fleet-snapshot.json`, captured at 07:18:13 UTC, reports all six workers warm:
28/28 runtimes on each recent replica and 160/160 on each archive owner.
Recent-01 reports binary `817b621cbffc5743750ed0d91280dc041ed4040f55764aadf1230ce6cc718eee`;
the other five report `a2ed225699fdee84a155dba28a94e85decc917f8382da0258faf03c02cc6682a`.
Only recent-01 has managed preparation configured. The snapshot includes hashes
of the deployed operations/configuration files that were present, not secrets.

SSH was restored by adding only the current operator address `206.223.234.6/32`
for TCP 22 to coordinator firewall `6318c192-08c5-472d-a376-8fdc6a785133`.
Worker inspection used the coordinator's existing private access. No worker,
router, build-slot, memory-limit or service configuration was changed.

## Next execution slice

1. Reproduce preparation under two sustained exact clients with recent geometry
   and consecutive candidate arrivals one second apart. Record snapshot/staging,
   warm preparation, activation and end-to-end freshness separately. Use isolated
   candidate state so a replay cannot replace live mainnet authority.
2. Evaluate bounded directory/page build concurrency against the current single
   build slot. Measure peak memory and loaded-query latency; retain the existing
   20% host-headroom requirement. Concurrency is a candidate optimization, not a
   demonstrated fix. If insufficient, address serial candidate scheduling while
   preserving epoch/canonical checks and managed preparation ownership.
3. Require the burst regression to meet public 30 s and replica 60 s budgets,
   with exact queries and no readiness/reorg regression. Record final source,
   binary, operations and configuration identity before deployment.
4. Deploy the qualified correction to the canary and start a fresh supervised
   six-hour/300-block run. Only a matching pass may trigger the authorized fleet
   maintenance rollout and subsequent 24-hour all-worker observation.

Do not relax the freshness budget or reuse samples from this failed run. The
runtime optimization, burst regression and renewed acceptance remain outstanding.
