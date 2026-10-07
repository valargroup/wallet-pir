# Txid display block-to-serving on production, 2026-10-07

Block-to-serving for the tiered txid display recent shard, measured on production
before and after one change to the recent worker on recent-01. That change was:
- the batched exact hint for the `txid-2k` tables (`07f90675`);
- two build threads on up to two CPUs, instead of one thread on at most one CPU.

Block-to-serving is the time from the controller first seeing a block to the
recent worker serving a map that includes it (the controller's `freshness_ms`).

## Result

| | 24 h before (1,126 cycles) | After (40 cycles) |
|---|---|---|
| Block to serving p50 / p95 / max | 16.1 / 18.9 / 97.2 s | **8.5 / 12.4 / 12.9 s** |
| Blocks over 20 s | 30 of 1,152 | 0 of 40 |
| Recent worker prepare p50 / p95 | 14.9 / 16.8 s | 7.3 / 9.7 s |
| Copy to recent-01 p50 / p95 | 0.49 / 1.69 s | 0.47 / 2.37 s |
| History worker prewarm on recent-01 p50 / p95 | 10.06 / 11.67 s | 10.33 / 12.28 s |

Forty cycles over about 50 minutes is a short window, not the 300-block live
criterion in [remaining work](../../docs/remaining-work.md#tiered-txid-display-proof-of-concept-2026-10-05).
The before window contains a 97 s cycle, four blocks queued behind a slow cycle;
the after window had no queueing. History prewarm rose 3% at p50 and 5% at p95.
That is small, but the window is too short to tell it from noise.

Full numbers: [summary.json](summary.json), from `python3 harness/analyze.py`.

## What the measurements showed

- **The worker was capped, not starved.** Before the change, during each
  prepare the display worker ran at 0.91–0.94 cores, its `CPUQuota=100%` cap.
  History used 1.3–1.5 cores at the same time, leaving about 1.5 of
  recent-01's 4 cores idle. Its cgroup had been throttled for only 19.7 s in
  total since the 2026-10-06 deploy ([raw/cpu-before.txt](raw/cpu-before.txt)).
  A lower CPU weight was not the cause.
- **Each prepare cost about 15 CPU-seconds** on recent-01 (DO-Regular,
  2.0 GHz), against 6.6 s at one CPU on the dev host bench
  ([2026-10-05 bench](../txid-display-tiered-2026-10-05/README.md#benches-and-choosing-the-seal-parameters)).
- **Preprocessing is three quarters of each table build.** On roman-dev-2, for
  one `txid-2k` table at one thread: preprocessing 2.2 s, reference hint 0.83 s,
  encoding 0.02 s. The batched hint computes the same hint in 0.15 s.
  Preprocessing halves at two threads ([raw/probe-stages.txt](raw/probe-stages.txt)).
- **After the change:** 11.5–14.0 CPU-seconds per prepare on 1.5–1.7 cores,
  while history used 1.1–1.3 cores ([raw/cpu-after.txt](raw/cpu-after.txt)).

## The change

- Source: `07f90675` adds `TXID_2K` and `TXID_4K` to the batched-hint
  geometries. `the_batched_hint_runtime_is_the_reference_runtime` now also
  covers a full `txid-2k` directory and a partly filled pages table: identical
  published masks and epoch, byte-identical answers, decoded rows.
  `make check-fast BASE=aede3e26` passed on roman-dev-2.
- Binary: `transparent-txid-server` built on roman-dev-2 with `cargo build
  --locked --release` and the CI release flags (`-C target-cpu=x86-64-v3 -C
  target-feature=+pclmulqdq`), sha256 `c8089d9d…` ([manifest](manifest.json)).
  It was not the CI release artifact.
- Deploy: from the Mac over SSH with [harness/deploy-recent.sh](harness/deploy-recent.sh),
  holding the production lock on the coordinator. The script waits for an
  activation, writes the drop-in [raw/zz-latency.conf](raw/zz-latency.conf)
  (new binary, `CPUQuota=200%`, `CPUWeight=50`, `TRANSPARENT_BUILD_THREADS=2`),
  and restarts the worker. Display still loses CPU to history: weight 50 < 100
  and nice 10. The worker restored its publication and was warm again in 6.4 s.
- The drop-in sits beside the `txid-display-*` deploy state. The reviewed
  request still records `cpu_quota` 100%, `cpu_weight` 20 and `build_threads`
  1. Re-rendering the worker unit through `txid-display-deploy` would not remove
  the drop-in, which still sets the new binary and limits.
- Rollback: delete the drop-in, `systemctl daemon-reload`, restart the unit.

## Not done

- Per-block incremental update of the recent shard:
  [valargroup/wallet-pir#128](https://github.com/valargroup/wallet-pir/issues/128).
- The live acceptance criteria and the 300-block window.
