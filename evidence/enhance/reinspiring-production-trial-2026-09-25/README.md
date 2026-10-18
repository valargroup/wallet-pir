# Native ReinspiRING production trial — 2026-09-25

This is an experimental trial for controlled clients, not approval for external
wallet use. Native q=2^54/p=2^16/two-limb Gaussian packing changes the protocol.
PR #17's correlated-error certificate and cryptographic release gates remain open.

## Saved production baseline

Raw load reports/logs and memory/CPU/stage samples are archived under `raw/`.
Run `python3 analyze.py` to reproduce `baseline-summary.json`. The archives were
captured around 06:22 UTC while the 20-QPS run was still active, so its totals
are a cutoff, not the final six-hour result. Per-report percentile ranges are
not aggregate percentiles. All HTTP errors and unsent arrivals are retained.

| Offered QPS | Completed | Correct | Incorrect | Errors |
|---|---:|---:|---:|---|
| 8, initial run | 14,296 | 14,291 | 0 | 5 HTTP 502 |
| 8, review run | 14,288 | 14,270 | 0 | 18 HTTP 502 |
| 15 | 31,500 | 31,487 | 0 | 13 HTTP 502 |
| 20, captured cutoff | 55,941 | 55,439 | 0 | 8 HTTP 502, 494 HTTP 503 |

The captured 20-QPS reports also contain 1,659 unstarted arrivals. The initial
load script rapidly retried failed startups during an interruption; batch
numbers are not elapsed minutes. Future comparisons must sort numerically and
include startup downtime, rather than considering only successful report files.

The router was manually stopped/started at 06:09:25 UTC; its incarnation and
cgroup counters changed. Do not interpret NRestarts=0 as proving no manual
restart. Correlate against `raw/router-timeline.txt` and split resource deltas by
incarnation. The actor/reason for that restart was not established here.
Before it, observed peak service memory was 4,771,106,816 bytes (4.44 GiB), with
zero recorded cgroup OOM events. The older isolated mapped qualification failed
on HTTP 503 at 703.15 seconds; it was not a passing 30-minute qualification.

At 20 QPS, observed total p90/p99 reached 409/492 ms, packing p90/p99 196/245 ms,
and worker p90/p99 52/95 ms. Router CPU consumption was approximately 3.2–3.6
cores of four. Artifact-load activity often overlapped slow periods, but not all
slow periods. This supports contention/queueing; it does not prove a unique cause.

## Quick real-data native check

The frozen source contains 604,328 records of 653 bytes, ending at block 3,495,424.
`snapshot.json` records the source hash and anchor. No real record payloads or
query secrets are committed. The snapshot is retained on the coordinator and
benchmark host under `/root/reinspiring-trial-20260925`.

Native implementation: PR17 head `96572f026da31ad4c4bc53b9a1ce08b9268a1f40`.
Profile: d=2048, q=2^54, p=65536, gadget bits=19, ell=2, Gaussian secrets; fresh
OS randomness per query. Record rows have 33 records, 21,549 bytes, padded to
12,288 u16 coefficients with no reduction of the plaintext width.

- First 4,096 rows: 135,168 records; 30/30 sampled queries correct; peak RSS
  796,004 KiB (777 MiB), retained packing coefficients 170,360,880 bytes.
- Full data padded to 32,768 rows: 100/100 sampled queries correct; peak RSS
  1,998,544 KiB (1.91 GiB). Loading every record is not exhaustive query coverage.
- A sequential same-host pair, 30 measured queries each, two Rayon threads:
  InspiRING median packing 37.9428 ms; native median packing 7.6021 ms (4.99x).
  In-process sequential server median 101.8128 vs 57.3832 ms. These exclude
  client/network time and do not compare the production GPU worker path.

The host is the existing eight-vCPU Xeon Gold 6548N benchmark host. Baseline
uses Rust1.91 `release` with native CPU flags; the final native harness uses
Skylake AVX-512 build flags with runtime SIMD dispatch. Production's packing
host is a four-vCPU Xeon Platinum 8168 and lacks some benchmark-host SIMD
capabilities; the benchmark alone does not establish production latency.
Earlier exploratory runs overlapped compilation; the final `*-paired-t2` runs
were sequential with no build running. Only that pair is used for the speedup.

`raw/native-sanity.tar.gz` contains timing/resource logs and build logs.
`harness/` contains the source; the final native harness additionally compares
recovered row bytes directly and enforces production row alignment.
Independent review approved the isolated harness with these coverage caveats.

## Implementation state

The dependency integration keeps the mapped-reader branch alongside PR17 and
adds a portable signed-word mapped native codec, bounded readers, and an explicit
power-of-two matrix interface (including CUDA) without fabricating NTT params.
The wallet integration is opt-in via `native-reinspiring`, uses a distinct
`ironwood-enhance-pir-v8-native-poc` protocol and control version 3, and keeps
coordinator-owned preparation, worker evaluation, router packing, live
publication/retention and current admission limits. The short integration checks and production cutover below are complete.


## Distributed integration checks

Dependency integration is pinned to `8168d158442ad6defac9ba0f5ff2d294b99e2ecd`.
Both native and legacy `packing_http` tests passed. A native run with
`QUALIFY_PUBLICATIONS=1 QUALIFY_QUERY_LANES=2 QUALIFY_QUERIES_PER_LANE=3`
also passed, verifying six queries during publication of 33 additional records,
plus request admission, slow-upload disconnects, watchdog and revocation checks.
These are short correctness checks, not sustained qualification.

Independent inspection found no blocking integration issue for this controlled
trial. Native control state must be rebuilt in a separate root; v7 control
manifests are incompatible. Prepared arithmetic caches trust the authenticated
coordinator. Build Enhance targets separately: Cargo feature unification means
an unrelated transparent binary built together with the native feature is not
an approved transparent-service artifact.

Linux build uses Rust 1.91.0, release-fast, `native-reinspiring,cuda` for server,
and `enhance-pir/native-reinspiring` for load client. Router/coordinator/CPU workers
use Skylake AVX-512. The GPU host's Xeon E5-2623 v4 requires a separate Haswell
build; the initial AVX-512 oracle trapped before deployment. CUDA verification
must use its `/usr/local/cuda-12.2/lib64` runtime library path.


## Production cutover and early results

Main `ac06df3` is deployed. Public Enhance was paused at 06:56:19 UTC; native
load through the reopened default endpoint began at 06:59:55 UTC. A private
smoke check passed 30/30 exact real-record answers before reopening. The actual
Quadro P4000 CUDA differential passed zero, Q-1 and mixed coefficient cases
against independent u128 arithmetic. `deployment.json` records binary hashes,
profiles and state roots. All roles use fresh native state; previous v7 roots,
binaries, unit overrides and Caddy config remain preserved. No rollback occurred.

Coordinator preparation, CPU/GPU worker evaluation, router packing and live chain
publication remain distributed. The GPU remains preferred. Publication advanced
from initial anchor 3,495,448 to 3,495,454 during the initial ramp; router packing
preparations stayed zero. Request/object admission settings were unchanged.

| One-minute load step | Correct/completed | HTTP errors | Unstarted | Client p99 |
|---|---:|---:|---:|---:|
| 8 QPS | 480/480 | 0 | 0 | 122.05 ms |
| 15 QPS | 900/900 | 0 | 0 | 141.18 ms |
| 20 QPS | 1,200/1,200 | 0 | 0 | 111.94 ms |

Router comparison uses the saved 20-QPS baseline at 06:15–06:22 UTC (414 seconds)
and samples inside the first native 20-QPS minute (50 seconds, 1,000 successful
queries). Stage p99 values interpolate Prometheus buckets, as APM does; these are
not exact raw-sample percentiles. Client p99 above includes client/HTTP costs and
is distinct from router total, which excludes body upload and response download.

| Router metric at 20 QPS | Previous profile | Native early sample |
|---|---:|---:|
| Packing mean | 84.05 ms | 17.86 ms |
| Packing p99, histogram estimate | 245.81 ms | 25.00 ms |
| Router total mean | 167.90 ms | 39.33 ms |
| Router total p99, histogram estimate | 492.62 ms | 62.50 ms |
| Mean CPU cores | 3.32 | 1.15 |
| Observed cgroup memory peak | 4,375 MiB | 802 MiB |
| Artifact load mean | 7.59 s (3 loads) | 1.25 s (1 load) |

Both compared windows recorded zero OOM events and zero artifact-load failures.
The memory peaks are since their respective process starts, not equal-length
steady-state bounds. The native run is short and has fewer publication overlaps;
this is promising POC evidence, not six-hour qualification or proof of the new
capacity limit. The native cryptographic correctness-bound caveat remains open.

`analyze_native.py` reproduces `native-summary.json` from the saved resource
samples and `raw/native-load-early.tar.gz`. Samples include process incarnation
and raw counters; do not average report percentiles. The archive is an early
cutoff while subsequent load remains active. APM's five-minute window initially
contains pre-cutover samples, so use the isolated counter deltas for comparison.

Sustained native load runs as `apm-native-load-ac06df3` on the coordinator at
20 QPS after the short ramp, plus one init request/second, retaining the original
10:56:32 UTC / 14:56:32 Dubai deadline. It stops on a failed batch, timeout, or
incorrect answer and never rolls services back. Live reports and timestamps:
`/root/reinspiring-trial-20260925/native-load/`. The router's existing five-second
resource sampler continues through the same deadline. No CI deployment or long
pre-cutover soak was added.

## Additional native load steps (unassessed)

`raw/load-30-40-summary.tar.gz` and `raw/ramp50-early.tar.gz` hold the load
client reports, batch logs and scripts for further one-minute steps against the
same deployment, protocol `ironwood-enhance-pir-v8-native-poc`. They were not
independently reviewed and are not six-hour qualification; they record what the
client observed and nothing about router internals.

| Step | Correct/completed | HTTP errors | Client p99 |
|---|---:|---:|---:|
| 30 QPS, run 1 | 1,800/1,800 | 0 | 114.6 ms |
| 30 QPS, run 2 | 1,800/1,800 | 0 | 134.9 ms |
| 30 QPS, run 3 | 1,800/1,800 | 0 | 141.7 ms |
| 40 QPS, run 1 | 2,400/2,400 | 0 | 149.8 ms |
| 40 QPS, run 2 | 2,400/2,400 | 0 | 149.5 ms |
| 42 QPS | 2,519/2,520 | 1 HTTP 502 | 180.6 ms |
| fast ramp, 42 QPS | all correct | 0 | 154.8 ms |
| fast ramp, 44 QPS | all correct | 0 | 155.6 ms |
| fast ramp, 46 QPS | all correct | 0 | 175.0 ms |
| fast ramp, 48 QPS | all correct | 0 | 188.4 ms |

The 42 QPS step's single 502 made the load script exit with status 1; the fast
ramp used ten-second steps rather than one-minute steps. The reports do not
identify the 502's origin.
