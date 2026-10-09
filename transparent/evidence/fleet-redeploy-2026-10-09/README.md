# Transparent fleet redeploy to `a317455e`, 2026-10-09

The three history workers and the publisher were moved to `main` at
`a317455e` (declared re-cuts, txid client and display bucket bound), from
worker `80c94f32` and publisher `12ce1291` builds about 250 commits older.
Roman approved the deploy and decisions D1–D9 of the prepared runbook.
[Manifest](manifest.json) holds every hash, time and measurement; `raw/`
holds the command logs and samples.

Nothing changed what the fleet serves. The publisher still writes no re-cut
declarations, the maps stayed byte-identical in shape and every sealed entry
is unchanged. What was new on history is runtime code that had not served it
before: the batched recent hint, admission against memory in use, charging
runtimes their held bytes, restore-slot release and the faster tail sealer.

## What ran

Release: the `transparent-publisher` bundle of full-CI run 37921342549
(archive `23cfeb12…`), checked with `tools/ci/release.py extract` on the Mac
and on the coordinator, which agreed on every checksum. Operations ran from
the coordinator's staged `4c85b6c2` tree; the seven scripts and libraries
used are byte-identical to `a317455e`. Every install and restart ran under
`/run/lock/wallet-pir-production.lock`. Each command and its output is in
`raw/logs/` (numbered in order); the step scripts are in `raw/steps/`.

| UTC | Step | Result |
|---|---|---|
| 11:06–11:15 | Baseline and health gates (read-only) | membership fresh, 3 serving; controller serving at the tip; load running; scaler `observe`; actuator off |
| 11:50–11:51 | Stage the bundle; new `shard-assign` re-reads the live map | byte-identical re-serialization; file lists equal the old binary's for all three workers |
| 11:51–11:58 | Stage and `--verify-only` on each worker, no restart | recent 8–9 s; archive-03 220 s, verifier at most 195 MB RSS, host never under 30.4 GiB available |
| 11:59:09 | recent-01 rolled (`roll-recent-replicas.py`, other-serving floor 1, D2) | 24.3 s; 18/18 warm |
| 12:00–12:33 | Mixed versions: recent-01 new, recent-02 and archive old, 5 QPS exact load | 9,337 exact, 0 errors, 0 missed slots |
| 12:33:09 | recent-02 rolled | 24.2 s; 18/18 warm, 18 cache hits |
| 12:33–12:45 | Load with both recent replicas new | 3,415 exact, 0 errors |
| 12:46:03 | Controller binary swapped, `source_sha` set to `a317455e` (D5), restarted | serving again in about 2 s |
| 12:47–12:59 | Load after the controller swap | 3,389 exact, 0 errors; freshness p50 12.1 s, max 19.3 s |
| 12:59–13:08 | archive-03 re-verified (217 s), installed and restarted from its disk runtime cache | warm 164/164, 164 hits, 0 misses |
| 13:30–14:01 | Load with every worker new, archive pin updated (D8) | 9,088 exact, 0 errors, 0 missed slots |
| 14:01–14:11 | 20 QPS gate (D9) | passed, below |

Cross-replica setups for the last three shards (directory and pages) were
byte-identical between old and new recent-01 and recent-02, and again between
the two new replicas.

## Outages

- **Recent tier.** None. Each replica was unreachable for 7–8 s and then warming
  for 4–5 s, while the other served alone.
- **Public metadata.** The runbook expected 30–60 s of 503. One-second probes of
  both origins saw one 502/502 sample at 12:46:03.4 and one 503 (public map) /
  200 (filter origin) sample at 12:46:04.5, and 200 on both from 12:46:05.6:
  at most 3.3 s. The new controller activated its first publication,
  3,511,772, about 15 s after the restart.
- **Archive shards 0–81.** archive-03 answered `/v1/ready` with the old binary
  last at 13:03:03.5 and with the new one, warm, at 13:08:42.7: **339 s**. The
  service stopped at 13:03:04 and started at 13:03:06. It loaded its shard set
  in 224 s and restored all 164 runtimes from disk in 111.5 s, as in the v11
  measurement (223 s and 110 s). Publication stalled for the same window: the
  last activation before was 3,511,789 at 13:02:39, the next 3,511,793 at
  13:08:45 (freshness 289.8 s), and 3,511,794 at 13:09:56 was back to 11.5 s.
  Public metadata stayed 200 throughout. The router's 503 counter rose by one.
  APM fired shadow warnings for archive coverage, readiness and publication
  age, and each recovered by 13:09:07; nothing paged.

## Validation after the deploy

- **Identity.** On all three workers the file, `/proc/<pid>/exe` and
  `/v1/ready` all report `34ba7ebb…`, and `shard-control` is `9208555a…`. Unit
  flags equal the pre-deploy units apart from order, and `NRestarts` is 0. The
  controller's executable is `13af048e…`.
- **Warm.** Recent replicas 18/18 and archive-03 164/164, no prewarm failures,
  no cache write failures, all on the controller's map.
- **Maps.** Both public origins byte-identical (`e1ce89d1…`, equal to the
  active map digest). Every sealed entry equals the pre-deploy baseline (90
  sealed shards). No `recuts`. The new `shard-assign` re-serializes the active
  publication for every worker. New `publication.json` files record
  `tool_sha` `a317455e` (the runbook named this field `source_sha`; that is the
  field in `controller.json`).
- **Freshness.** For 30 minutes after the archive returned: 32 publications,
  freshness p50 12.8 s and max 23.4 s, `node_height − public_height` at most 2
  (pre-deploy 14.5 s).
- **Memory and disk.** recent-01 and recent-02 use 1.25 and 1.26 GB (peak 1.45
  and 1.47 GB) with 5.4 and 6.0 GiB available. archive-03 uses 33.6 GB (peak
  33.9 GB, `MemoryHigh` 48 GiB) with 30.5 GiB available. Coordinator free space:
  `/` 24%, `/srv/zakura` 22.8%, `/srv/transparent-activity` 59%.
- **Txid display** (not redeployed, D6). `txid_live_lookup` from `a317455e`
  passed before (11:11) and after (13:32): an archive lookup, 80,400 B up and
  50,383 B down. Both display workers stayed ready and warm, and the controller
  reported no failures with its journal at the tip.
- **APM.** No open incidents; scaler `observe`, holding at two replicas.
- **pir-monitor.** At 13:32 the transparent canary was still `oracle_invalid`,
  the known failure caused by its v10 fixture. A separate session, holding the
  production lock from 13:49:43, moved it to the v11 load fixture at 13:50
  ([quality monitoring moved to v11](../quality-monitoring-v11-2026-10-09/README.md)).
  At 14:19 it had passed every sample since, 30 successes and no failures, all
  against the fully redeployed fleet (`raw/logs/040-v4-pir-monitor-recheck.log`).
  The same change pointed APM's Transparent view at the v11 load status, which
  showed the load fresh and running.
- **Wallet regression.** `transparent-regression` at `a317455e` stopped at
  preflight with `publication drift` before and after, because its fixture
  pins the v10 map `3780f252…`. No wallet sync ran against production. Reports
  are in `raw/mac/`.

## 20 QPS gate

Three `rate-query` clients at 7 QPS each for 600 s from
`roman-ipir-bench-8vcpu` against `https://transparent-pir.valargroup.dev`, on
top of the 5 QPS load. They used the v11 canonical-load binary (`3d8d0cda…`)
and fixture (`705770a6…`): 80% recent, 20% archive, sealed revisions, fresh
keys and exact row hashes. Scripts and `analyze-gate.py` are in `raw/gate20/`.

| Run | Fleet | Queries (exact) | Errors | Missed slots | QPS | p50 | p95 | p99 | Max |
|---|---|---|---|---|---|---|---|---|---|
| This deploy | v11, 2 recent + 1 archive (0–81), all `a317455e` | 12,290 (all) | 0 | 423 | 20.51 | 17.0 ms | 38.5 ms | 59.2 ms | 0.99 s |
| [2026-09-30](../shared-native-rollout-2026-09-30/README.md) | v10, 3 recent `6360f0d8` + archive `a704616c` | 12,528 (all) | 2 retried | 42 | 20.88 | 15.7 ms | 34.7 ms | 47.6 ms | 2.16 s |
| [2026-09-29](../recent-floor-2026-09-29/README.md) | v10, 2 recent `a704616c` + two archive owners | 12,519 (all) | 2 retried | – | 20.9 | 19 ms | – | 53 ms | 0.24 s |

By class, p50 / p99: archive directory 15.7 / 47.7 ms, archive pages
24.2 / 63.9 ms, recent directory 14.8 / 52.3 ms, recent pages 17.2 / 63.5 ms.
Query slots were busy 4.1% (recent-01), 5.0% (recent-02) and 2.5%
(archive-03) of the window. The 5 QPS load stayed exact throughout, its
trailing-minute p99 at most 0.44 s.

The gate (p99 under 2 s, p50 under 700 ms, every query exact) passed. The
missed slots are the client's: the bench host was also running CI Rust builds
(load average about 4), and each missed slot reports "client admission
delayed beyond one interval". p99 is 6–12 ms above the v10 runs. The schema,
fixture and shard counts differ, so this is not a like-for-like comparison.

## Deviations from the runbook

- The mixed-version window ran 33 minutes, not 10, and the 5 QPS load was
  stopped from 12:59 to 13:30, about 21 minutes longer than needed after
  archive-03 was warm at 13:08. The operator's wake-up watches did not fire.
  Nothing else waited on them, and no service was affected.
- Stopping and starting the load and editing its pins ran outside the
  production lock, as the runbook writes them. Every binary install, unit
  change and restart ran under the lock.
- The archive unit's `MemoryMax=56G` line moved below `MemoryHigh`. The lines
  and values are unchanged; `install_worker` rewrites that block.

## Rollback material

On each worker, `/opt/transparent-publisher/rollback/roll-a317455e/<worker>/`
holds the previous binary, `shard-control` and unit. On the coordinator,
`/srv/transparent-activity/build/evidence/release-12ce1291…/artifacts/transparent-publish-controller`
(`a68dca01…`) and `/root/deploy-a317455e/controller.json.before` restore the
publisher; `raw/steps/p6-rollback.sh` and `raw/steps/p7-rollback.sh` are the
commands. Rolling back archive-03 costs another archive outage.

## Not established

This is not a soak. It does not cover wallet recovery against production, a
re-cut publication, cold archive rebuilds on the new binary, or txid display
on `a317455e`. Future v11 schema operations still pin the old worker
(`6db1fa05…`, `6dfe78fa…`) and adapter (`f3df5c53…`) and need new pins.
