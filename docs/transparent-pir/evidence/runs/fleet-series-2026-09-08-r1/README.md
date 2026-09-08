# Fleet series, repetition 1 (coordinator as client), 2026-09-08

The first of the three Gate 6 repetitions, run from the coordinator's runner
([run 34212408175](https://github.com/valargroup/enhance-pir/actions/runs/34212408175),
harness `dd76da2`). Its 8- and 32-wallet steps are valid; the run was
stopped during the 128-wallet step because the coordinator's eight shared
vCPUs saturated as the client (load above 120) and would have measured the
client rather than the fleet. `report.json` was taken from the runner's
workspace after those two steps; there is no console log for this run. The
remaining repetitions run from a dedicated-CPU load generator.

Fleet as in [run c](../fleet-warm-2026-09-08-c/README.md); workload: 10 min
per step, 100 completions per class, 6,000 queries per sync.

## 8 wallets, 924 s: 502 syncs, 497 covered, 497 exact, 0 failed

| Class | n | Exact | p50 | p95 | p99 | Stopped short |
|---|---:|---:|---:|---:|---:|---|
| catch-up-1d / 7d / 30d | 55 / 56 / 55 | all | 0.36 / 0.26 / 0.27 s | 0.63 / 0.54 / 0.64 s | 0.84 / 0.68 / 1.38 s | unresolved-spends (counted covered) |
| restore-6m | 56 | 56 | 0.76 s | 1.53 s | 2.47 s | |
| restore-old | 55 | 55 | 3.5 s | 21.2 s | 24.0 s | |
| multi-script | 57 | 57 | 5.9 s | 10.6 s | 11.9 s | unresolved-spends (counted covered) |
| small-active | 56 | 56 | 0.20 s | 0.51 s | 1.44 s | |
| unused | 56 | 56 | 2.6 s | 3.2 s | 3.7 s | |
| reused-tail | 56 | 51 | 14.0 s | 625 s | 652 s | 5 query-budget |

## 32 wallets, 1,420 s: 1,111 syncs, 1,100 covered, 1,100 exact, 1 failed

| Class | n | Exact | p50 | p95 | p99 |
|---|---:|---:|---:|---:|---:|
| catch-up-1d / 7d / 30d | 123 / 123 / 123 | all | 0.93 / 0.72 / 0.73 s | 1.63 / 1.20 / 1.90 s | 1.81 / 1.43 / 3.22 s |
| restore-6m | 123 | 123 | 1.9 s | 3.7 s | 4.8 s |
| restore-old | 124 | 124 | 9.9 s | 42.3 s | 74.9 s |
| multi-script | 124 | 123 (1 failed) | 15.8 s | 23.6 s | 36.5 s |
| small-active | 123 | 123 | 0.50 s | 1.56 s | 3.72 s |
| unused | 124 | 124 | 6.9 s | 9.2 s | 10.8 s |
| reused-tail | 124 | 114 | 40.5 s | 1,102 s | 1,258 s |

Every covered sync in both steps was exact. Completed-sync rates (0.54/s and
0.77/s) are bounded by each step's tail: the heaviest reused-tail scripts
run ten to twenty minutes and the step waits for them.

## What the fleet did

After the 32-wallet step, cumulative queries: archive-02 156,475; archive-01
21,506; each recent replica about 16,000. Archive-02 holds shards 80–159,
where the reused, spammed scripts live, and carried seven times archive-01's
demand: the archive split by resident bytes is even, the split by demand is
not. No worker refused a query in either step (queue rejections, body budget
rejections, deadline exceedances all zero); archive-02's queue depth peaked
at 5 at 32 wallets and its load at about 8 on 8 vCPUs. Resident memory did
not move: owners 49.7 GB, replicas 4.6–4.9 GB.

Latencies from 8 to 32 wallets roughly doubled or tripled across every
class. With the fleet's queues near empty, most of that is the client: the
coordinator was already at load 5–11 during the 32-wallet step. The
load-generator repetitions separate the two.
