# Public Enhance baseline — September 14, 2026

Measured `https://enhance-pir.valargroup.dev` from 2026-09-14T22:18:09.644277+00:00 to
2026-09-14T22:24:42.157918+00:00. The release-built load client used source revision
`536d1af8a27f6abfd3bf8a26cbb20b8287247d42`. All four steps completed successfully, with 2,245
successful measured queries, zero errors and no failed health probes.

| Concurrent clients | Successful queries | Errors | Requests/s | p50 (ms) | p95 (ms) | p99 (ms) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 157 | 0 | 2.62 | 374.5 | 409.9 | 582.7 |
| 2 | 303 | 0 | 5.05 | 387.8 | 416.8 | 545.8 |
| 4 | 621 | 0 | 10.35 | 381.7 | 416.5 | 515.8 |
| 8 | 1,164 | 0 | 19.40 | 394.5 | 524.8 | 965.1 |

Each step used a 10-second warmup, 60 seconds measured, seed 42 and a pool of
1,024 random positions, followed by at least 30 seconds idle before the next
step. Each initialized one session and used fresh cryptographic randomness per
request. Preflight and warmup requests are additional to the table's counts.
Health was polled every five seconds, with a four-second request timeout and
termination after two consecutive failed probes. No stop condition was reached.

The advertised schema was 7, with 65,536 logical rows, 4,096 columns, two instances,
five shards and two configured workers. Measured generations were 3,483,566,
3,483,567 and 3,483,569, with 319,781–319,789 covered positions. Captured public
metadata identifies anchors and shard digests, not the deployed binary SHA or
machine inventory. No private host access, deployment or restart was performed.

The client was an Apple M4 Max, 16 logical CPUs, 128 GiB RAM, macOS 26.2. The
network route/access link and server SKU, region and release identity were not
independently established. Lightweight repository operations/link checks briefly
overlapped the four-client step; the run is not an isolated client-CPU benchmark.
Full compilation and final validation ran after load collection.

## Evidence

- [Manifest](manifest.json): exact commands, UTC times, build and binary identities,
  client hardware, per-step status, duration and limitations.
- [Supervisor](run.py): captured collection logic. Its paths target this directory;
  do not rerun it over retained evidence. Use a new run directory.
- [Concurrency 1](c1-summary.json), [2](c2-summary.json), [4](c4-summary.json),
  [8](c8-summary.json): raw load summaries, including every timing stage.
- `cN.log`: command output; `cN-health-probes.jsonl` and `cN-probe-NNN.json`:
  timestamped probe results and raw responses.
- `cN-init-before.json`, `cN-init-after.json`, `cN-health-before.json` and
  `cN-health-after.json`: raw public snapshots around each step. The load tool
  performs its own initialization; its summary identifies the queried generation.
- [Payload size check](payload-size-provenance.json): offline request construction
  and response-size derivation from the captured geometry.
- `SHA256SUMS`: run-directory checksums. Verify from this directory with
  `shasum -a 256 -c SHA256SUMS`.

## Interpretation

These are single, closed-loop runs, not an arrival-rate saturation test. Total
latency includes client preparation, network/server HTTP time and decoding.
Reported throughput divides all drained completions by the configured 60 seconds.
There were no errors in this run, so completed and successful rates coincide.
The supervisor's elapsed duration includes setup, warmup, drain and polling;
it is not a substitute measurement-window denominator.

The workload validates record decoding and encoding, not canonical equality or
whole-wallet recovery. It does not measure server resource use, full-capacity
memory, failover or online expansion. Passing this baseline does not satisfy
hardware qualification. See the [performance guide](../../docs/performance.md)
for stage definitions, historical comparisons and reproduction.
