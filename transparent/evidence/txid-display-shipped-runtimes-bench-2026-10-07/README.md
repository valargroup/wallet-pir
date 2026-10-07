# Shipped txid display runtimes: pre-deploy bench, 2026-10-07

A local bench on roman-dev-2 of `txid-display-controller run --ship-runtimes`
([design](../../docs/txid-display.md#shipped-recent-runtimes)), run before any
deploy as the [handoff plan](../../docs/txid-display-shipped-runtimes-plan.md)
asks. Nothing here touched production. Synthetic chain only.

## Result

Run 2 is the source as pushed; run 1 is the same harness one source change
earlier (shipped files fsynced, worker reply fields not copied into the cycle
record) and is kept as recorded.

| | Plan estimate | Measured (run 2) |
|---|---|---|
| Bytes per shipped file (`txid-2k`, both tables) | 40–72 MiB | **40.05 MiB** (every file; compiled matrix at four-byte words) |
| Shipped per block (one directory, one pages table) | 80–140 MiB | **80.09 MiB** (below the 100–150 MiB band) |
| Controller prebuild per block, 2 threads on 2 CPUs | — | p50 **4.36 s**, p95 5.19 s (verify, 2 × build 1.35 s, 2 × write 0.44 s) |
| Worker load + self-check per file, 2 CPUs | — | load p50 0.24 s (max 0.41 s); self-check about 0.07 s |
| Worker CPU per block | under 1 CPU-s for load and check | **1.15 CPU-s** for the whole prepare, against 5.80 CPU-s building locally |
| One block's delta with the adapter's rsync flags, local disk | rsync under 2 s on the private network | 96.1 MiB in p50 **0.33 s** (max 0.42 s); 16.0 MiB / 0.12 s without the runtime files |
| Fallbacks | 0 | **0** in 24 shipped cycles (48 loads); 0 prebuild failures |

Block to serving on this host got **worse**, not better: controller cycle p50
4.22 → 6.38 s, p95 4.81 → 7.20 s. The worker's prepare fell from p50 3.62 s to
1.33 s, but the controller's 4.36 s prebuild now runs on the cycle's critical
path before the copy, at the same per-thread speed as the worker it replaces.
The gain in production rests on the coordinator building faster than recent-01
does under contention with history (recent-01 prepare p50 7.3 s on 2 CPUs in
the [freshness evidence](../txid-display-freshness-2026-10-07/README.md)); this
bench cannot show that. The worker's CPU saving does not depend on it.

## Setup

- Source: run 2 `5525b839`; run 1 `6b8c5c91` (see manifest).
  Binaries `transparent-txid-server` and `txid-display-controller`, profile
  `release-fast`, `RUSTFLAGS="-C target-cpu=x86-64-v3 -C target-feature=+pclmulqdq"`
  (the CI release CPU flags), hashes in `raw/run*/binaries.sha256`.
- Host: roman-dev-2, DO-Premium-Intel, 8 vCPU (AVX2, no AVX-512), 31 GiB,
  Linux 6.8.0-134-generic, shared with other agents' builds during the runs.
- [harness/bench.sh](harness/bench.sh): synthetic journal of 12,000 blocks
  (mean 8 records, seed 7, 96,023 records); bootstrap through 11,400 with
  production's seal parameters (`txid-2k`, one bucket, archive target 40,000,
  recent floor 10,000); recent shard 43,243–43,566 records, 5.2 MB directory
  bytes (production: 47,736 records, 4.6 MB). Then 40 blocks replayed one per
  4 s, twice from copies of that root: `baseline` and `shipped`
  (`--ship-runtimes`). Blocks coalesce when a cycle outlasts 4 s, so the
  shipped run has 24 cycles against 36.
- The worker runs with production's flags (`--cache-bytes 536870912
  --build-slots 1 --query-slots 2 --retain-revisions 1`, no disk cache) on
  `taskset -c 6,7` with `TRANSPARENT_BUILD_THREADS=2`, standing in for
  recent-01's `CPUQuota=200%` and two build threads; this CPU is faster per
  core than recent-01's DO-Regular 2.0 GHz. The controller runs on
  `taskset -c 2,3` with two build threads (the coordinator unit's
  `CPUQuota=200%`). The worker answers no queries.
- Worker CPU is utime + stime from `/proc` every 0.25 s over the steady
  cycles, divided by their count. The first replay cycle is excluded: the
  worker starts empty and also builds the bootstrap candidate.
- rsync: the newest two candidates of the shipped run, the adapter's flags
  (`-a --delete --numeric-ids --link-dest=<previous>`), local disk to local
  disk, five repeats each with and without `*.runtime`, `sync -f` included.

## Limits

- **rsync time is local, not the private network.** No SSH or network hop
  was measured. At 1 Gbit/s the 80 MiB of runtimes alone take about 0.67 s;
  production copied the 16 MiB delta in p50 0.47 s, p95 2.37 s
  ([freshness evidence](../txid-display-freshness-2026-10-07/README.md)), so
  a five-times larger delta could pass 2 s at p95. Measure `ship_ms` on the
  first live cycles.
- The worker figure is per block on a faster, idle CPU; recent-01 spent about
  twice the CPU per prepare of this host's one-CPU bench. Its 1.5 CPU-s
  acceptance target is not shown by this bench.
- Synthetic chain, two runs, a shared host.

## Files

- `summary.json`: run 2, from `python3 harness/analyze.py raw/run2`.
- `raw/run1/`, `raw/run2/`: each run's timelines, worker logs, CPU samples,
  rsync stats and timings, binary hashes; `raw/run1/summary.json` is run 1's.
  Run 1's `rsync.jsonl` has `.05`-style numbers from `bc`; the analyzer reads
  them and the harness now writes `0.05`.
