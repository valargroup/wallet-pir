# Shipped txid display runtimes: production deploy and measurement, 2026-10-08

The recent replica no longer builds the two `txid-2k` runtimes of each new recent
revision: the controller builds them once (`--ship-runtimes`) and ships them with
the candidate; the replica loads, self-checks and serves them
([design](../../docs/txid-display.md#shipped-recent-runtimes),
[plan](../../docs/txid-display-shipped-runtimes-plan.md),
[pre-deploy bench](../txid-display-shipped-runtimes-bench-2026-10-07/README.md)).
This records the deploy to the tiered proof of concept, the live measurements, and
the end of the window when the proof of concept was replaced by txid display v2.

## Result

Measured over cycles 2233–2353 (121 cycles, 10:50–13:21 UTC) against the plan's
targets, which were estimates. Baselines are the 2026-10-07
[freshness](../txid-display-freshness-2026-10-07/README.md) and
[tiered](../txid-display-tiered-2026-10-07/README.md) evidence on the same hosts.

| | Target | Before | Measured | Verdict |
|---|---|---|---|---|
| Block to serving p50 | ≤ 6 s | 8.5 s | **6.74 s** (5.73 s in the quiet first 38 min) | close, not met |
| Block to serving p95 | ≤ 8 s | 12.4 s | **29.9 s** (16.5 s excluding the 11:28–11:35 fleet SSH timeouts) | **not met** |
| Over 20 s | 0 | — | 9 of 128 (all in the 11:28–11:35 minutes) | not met |
| Worker CPU per block | ≤ 1.5 CPU-s | 11.5–15 CPU-s | **1.51 CPU-s** (113 quiet blocks) | met |
| Recent lookup p99 during loads vs outside | within 20% | 337 vs 135 ms (ratio 2.5) | **159 vs 114 ms (ratio 1.37)** | not met, 4× smaller gap |
| Self-check failures, fallbacks | 0 | — | **0 of 242 shipped loads** | met |
| Shipped bytes per block | 80–140 MiB | — | 80.1 MiB (two files) | in band |
| rsync of one block | < 2 s | 0.47 s p50 / 2.37 s p95 (16 MiB) | **1.82 s p50 / 10.2 s p95** (96 MiB); 53 of 121 over 2 s | not met |

The stages the change owns behaved as the bench predicted: controller prebuild p50
2.81 s (p95 3.29 s) at two threads on the Xeon, against 4.36 s on roman-dev-2; worker
prepare fell from p50 7.0 s to 1.44 s; each shipped file loaded in 0.24–0.30 s and
self-checked in about 0.12 s. The replica's CPU per block fell by about a factor of
eight, and its lookup tail during loads moved from 2.5× the quiet tail to 1.4×.

What got worse is the copy. The candidate grew from 16 MiB to 96 MiB per block, and
on the private network between the coordinator and recent-01 that copy took p50
1.82 s but p95 10.2 s, so the block-to-serving tail is now the rsync tail. The copy
was already variable at 16 MiB (p95 2.37 s). Per period
([raw/controller/ship-by-period.txt](raw/controller/ship-by-period.txt)):

| Period (UTC) | Cycles | Ship p50 / p95 | Block to serving p50 / p95 |
|---|---|---|---|
| 10:50–11:28, quiet, includes window B1 | 28 | 1.39 / 4.43 s | 5.73 / 9.05 s |
| 11:28–11:35, fleet SSH timeouts | 7 | 5.00 / 18.7 s | 29.9 / 161 s |
| 11:35–11:56, before the coordinator bootstrap | 10 | 2.11 / 9.43 s | 6.78 / 14.3 s |
| 11:56–13:21, another session's genesis bootstrap on the coordinator (I/O pressure "some" 23–26%, `ionice` idle) | 76 | 1.82 / 13.3 s | 6.74 / 18.9 s |

So on its own hosts the design trades the worker's 12 CPU-seconds per block for an
80 MiB copy whose tail this network does not carry within the targets. The CPU
contention goal is met; the latency goal is met at the median and missed at the tail.

## Deploy

- Source: branch `claude/shipped-runtimes-lint-d993bc75` at `28c39d94` = main
  `98792569` (the reviewed change, `6b8c5c91..d993bc75`) plus the clippy fixes and
  releasing the restore slot once a shipped file is read. CI full passed on that
  head (run 37714350150); main carries the same change as `229b9eb9`.
- Binaries: built on roman-dev-2 with the CI release flags
  (`RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq" cargo build
  --locked --release`), hashes in
  [raw/deploy/binaries-28c39d94.sha256](raw/deploy/binaries-28c39d94.sha256); glibc
  2.34 (worker) and 2.39 (controller) against hosts at 2.39.
- Order, under `/run/lock/wallet-pir-production.lock` on the coordinator
  (held 10:45:04–10:55:08 UTC), each restart right after an activation with an empty
  seal queue:
  1. Worker, 10:44:49 UTC: [harness/deploy-recent.sh](harness/deploy-recent.sh)
     installed `releases/28c39d94-ship/` on recent-01 and swapped only the binary
     path inside the existing `zz-latency.conf`
     ([raw/deploy/zz-latency.conf](raw/deploy/zz-latency.conf): 2 build threads,
     `CPUQuota=200%`, `CPUWeight=50`). Two cycles with the old controller built
     locally as before ([raw/deploy/worker-verify.jsonl](raw/deploy/worker-verify.jsonl)).
  2. Controller, 10:49:21 UTC: [harness/deploy-controller.sh](harness/deploy-controller.sh)
     installed the binary on the coordinator and added
     [raw/deploy/zz-ship.conf](raw/deploy/zz-ship.conf): the same arguments plus
     `--ship-runtimes`, `TRANSPARENT_BUILD_THREADS=2` for the unit's `CPUQuota=200%`.
     The restart resumed live from `active.json` in about a second with no
     re-bootstrap, re-seal or replay of published blocks, as the restart analysis
     in the plan predicted.
- First five cycles ([raw/deploy/first-cycles.jsonl](raw/deploy/first-cycles.jsonl)):
  prebuild 2.6–2.8 s, ship 1.0–1.8 s, prepare 1.15–1.35 s, block to serving 5.3–6.3 s,
  2 shipped loads and 0 fallbacks each.
- Rollback was [harness/rollback-controller.sh](harness/rollback-controller.sh)
  (remove the drop-in, restart between cycles) and the previous worker binary path;
  it was not needed.

## Measurement

- Controller cycles: the publication `timeline.jsonl` (read-only tail,
  [raw/controller/](raw/controller/)), summarised by
  [harness/window300.py](harness/window300.py) into
  [raw/controller/window-2233-2353.json](raw/controller/window-2233-2353.json).
- Worker CPU: `CPUUsageNSec` of the worker and the history shard unit on recent-01
  every 30 s from the read-only supervisor
  ([harness/supervise.py](harness/supervise.py), the 2026-10-07 session's tool with
  two rule amendments recorded in [raw/supervisor/events.jsonl](raw/supervisor/events.jsonl)),
  divided by the blocks activated in the quiet segments, the load window excluded:
  113 blocks over 8,978 s, 1.51 CPU-s per block, worker 0.019 cores average.
- Controller CPU: `CPUUsageNSec` of the controller unit at cycles 2247 and 2331
  ([raw/controller/coordinator-cpu-samples.jsonl](raw/controller/coordinator-cpu-samples.jsonl)):
  5.3 CPU-s per cycle, the prebuild at two threads.
- Query interference, window B1 (10:55:56–11:06:00 UTC): 20 lookups/s for 600 s from
  the bench host with the 2026-10-07 session's `txid-rate`, fixture and arguments
  (`lookup 20 4 600 --cold-every 25`), against the public endpoint; the same window
  as that session's B ([raw/windows/B1-20lps-lookup-shipped/](raw/windows/B1-20lps-lookup-shipped/)).
  All 12,004 lookups exact, 0 errors, 0 missed slots. Total p50/p95/p99 54.8/85.9/117 ms
  against 54.6/126/187 ms the day before; per-minute p99 91–142 ms against 64–302 ms.
  [harness/prepare_pairing.py](harness/prepare_pairing.py) classes each lookup by the
  worker's ship, prepare or tail interval from the timeline
  ([prepare-pairing.json](raw/windows/B1-20lps-lookup-shipped/prepare-pairing.json),
  [baseline](raw/windows/baseline-2026-10-07-B-20lps-lookup-prepare-pairing.json)):
  recent lookups p99 159 ms in prepare (n=156), 157 ms in ship (n=176), 114.5 ms
  outside (n=9,076); the baseline had 337 ms in prepare (n=1,145) and 135 ms outside.
  Loads covered 9.8 s of prepare plus 10.9 s of ship in 600 s, against 78 s of
  prepare before. A second window could not run: the supervisor tripped on
  pre-existing history spikes (below) and the proof of concept was stopped at 13:21.

## Collateral

- History shard on recent-01: prewarm p50 by hour 10.06, 9.94, 10.68 s after the
  deploy against a 9.43–10.42 s hourly range the day before
  ([raw/history/](raw/history/)); the 12h hour sits just above the range, during the
  coordinator bootstrap; shard loading p50 0.81–0.91 s against 0.73–0.84 s.
- History query latency: the 5 QPS history load showed p99 spikes of 0.65–5.2 s in
  the trailing minute at 11:06, 11:22–11:38, 12:00, 12:09, 12:26 and 12:57, with p95
  near 0.03 s each time. These are the pre-existing pattern: the same log shows 5–27
  queries over 1 s in every hour of the previous day and 2–14 over 3 s in several of
  them. Over 98 cycles the slow queries sit at 0.07% inside display ship intervals and
  0.14% inside prepare intervals against 0.17% outside, and every query over 3 s fell
  outside both ([raw/history/history-clustering-10h50-12h51.txt](raw/history/history-clustering-10h50-12h51.txt)).
  The 11:22 cluster followed the history product staging a new map to all three of
  its workers at 11:21:18; the 11:30 cluster, with a history replica out of ready for
  two minutes and two display cycle failures (`ssh timed out after 60s` to recent-01),
  coincided with the history reconciler timing out against archive-03 as well, so the
  coordinator or its network, not recent-01.
- Archive owner: untouched; it receives a candidate only at a seal boundary, and none
  occurred (the recent shard sealed at 10:44, before the deploy).
- Memory on recent-01: the worker unit stayed under 0.5 GiB against `MemoryMax`
  1.5 GiB; the shipped preprocessing is page cache mapped from the candidate file.

## Supervisor trips

The read-only supervisor applied the 2026-10-07 brief's stop rules and tripped three
times, each recorded and cleared in [raw/supervisor/](raw/supervisor/):
1. 11:06:24, history p99 0.84 s, 24 s after window B1 ended; a 1.4 s stall of all four
   recent history shards with no display activity within 10 s.
2. 11:22:08, the history staging cluster above.
3. 12:57:42, history p99 0.65 s with p95 0.033 s.
Amendments: the `exact < 290` rule applies only while the load reports running
(it pauses itself when its publication lags), and from 13:31 the p99 rule required
p95 > 0.1 s or an error as well. The second window was then refused because the
proof of concept had already been stopped.

## End of the window

At 13:21:06 UTC another session stopped the proof-of-concept controller and began
the txid display v2 cutover (release `d730ed6b`, controller unit re-rendered 13:25).
That session removed both drop-ins, `zz-ship.conf` on the coordinator and
`zz-latency.conf` on recent-01, and reports that v2 runs without `--ship-runtimes`.
The shipped-runtimes window therefore covers 121 cycles rather than the planned 300.

## Not done

- The 300-cycle window and a second interference window.
- An rsync of the runtime files measured in isolation on the private network.
- The follow-ups in [remaining work](../../docs/remaining-work.md#txid-display-shipped-runtimes).

## Files

- `manifest.json`, `SHA256SUMS`.
- `harness/`: the deploy, rollback and watch scripts as run (hosts by ssh alias; the
  local `ssh_config` is not retained), the supervisor and its window driver and
  analyser from the 2026-10-07 session, `prepare_pairing.py`, `window300.py`.
- `raw/deploy/`: deploy logs, the drop-ins as written, binary hashes, the first cycles.
- `raw/controller/`: timeline tail, the 121-cycle summary, per-period ship times,
  controller CPU samples.
- `raw/windows/`: window B1 (txid-rate JSONL per process, window and summary, pairing)
  and the baseline pairing.
- `raw/supervisor/`: history and CPU samples, events, the three trip records, log.
- `raw/history/`: history prewarm timings and the clustering note.
