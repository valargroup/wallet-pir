# Full-shard batching measurements — September 24, 2026

This is an isolated localhost measurement of the new Enhance PIR batching
path. It is **not** a measurement on the production 8 GiB worker hosts or a
maximum-throughput qualification. The [manifest](metadata.json) records the
source commit, dirty-source patch, build profile, host, fixture, and binary
hashes. The [rate sweep](run.json) and each step's command, JSON report, and
Prometheus counter snapshots are retained here.

## Setup

- Native `release-fast` binaries on one Apple M4 Max Mac with 16 logical CPUs
  and 128 GiB RAM. Two worker processes and one coordinator used loopback HTTP.
- One synthetic shard with 1,081,343 records, 32,768 logical rows, and four
  8,192-row mutable units. Each worker reported **805,306,368 database bytes
  (768 MiB)**. The fixture did not publish new generations during measurement.
- The load driver scheduled random positions at a fixed open-loop rate,
  verified every returned record against the positional fixture oracle, and
  used zero warmup queries. The 0.1 QPS step ran for 120 seconds; the other
  steps ran for 90 seconds. One client drove 0.1 QPS and eight clients drove
  the other rates. All steps had zero wrong answers, HTTP/transport errors,
  and unstarted arrivals.

## Requested smooth-rate sweep

| Offered QPS | Correct | p50 ms | p95 ms | p99 ms | Queries / batch | Mean worker scan / query ms |
|---:|---:|---:|---:|---:|---:|---:|
| 0.1 (one per 10 s) | 12 | 60.6 | 66.4 | 66.4 | 1.00 | 8.32 |
| 1 | 90 | 61.3 | 71.8 | 80.3 | 1.00 | 8.81 |
| 2 | 180 | 57.7 | 66.9 | 123.8 | 1.00 | 8.61 |
| 4 | 360 | 57.6 | 66.0 | 71.0 | 1.00 | 8.28 |
| 8 | 720 | 54.7 | 57.8 | 65.6 | 1.00 | 7.66 |

These runs used server binary `b85a913d2a12d271f6ea712f17aa13f50c6d8c28b54498dd648663a2768cc024`.
Every smooth arrival waited about 21.5 ms in the coordinator's 20 ms
coalescing window, yet no two arrived close enough to share a scan. The
0.1 QPS p99 is based on only 12 observations.

## Controlled burst at the same average 8 QPS

To exercise batching, the supplemental driver released eight queries
together each second. The final SIMD server binary is
`74e66fd68cd35bfc1393a9ae5b602be11dd1ed7dde7f0f1d8ebbd9033d125eac`.
Both [smooth](smooth8-v3/run.json) and [bursty](burst8-v3/run.json) 8 QPS
comparisons used that exact binary, 720 correct answers, and zero errors.

| Pattern | Batches | Queries / batch | Mean scan / batch ms | Mean scan / query ms | End-to-end p50 / p99 ms |
|---|---:|---:|---:|---:|---:|
| Smooth: one every 125 ms | 720 | 1 | 8.07 | 8.07 | 54.5 / 81.5 |
| Burst: eight every second | 90 | 8 | 32.44 | 4.05 | 89.0 / 127.0 |

Eight separate single-query scans would consume about `8 × 8.07 = 64.6`
ms of worker scan time on this host. A batch of eight consumed 32.44 ms:
**about 2.0× less scan time per query**. This measures scan efficiency,
not sustainable fleet throughput. The burst pattern has higher wallet
latency because eight requests arrive together and share one longer operation;
the end-to-end percentiles should not be compared as if arrivals were equal.

The initial scalar batch implementation took 129.3 ms per eight-query scan
([raw run](burst8/run.json)). Packing query coefficients by row reduced that
to 58.5 ms ([raw run](burst8-v2/run.json)); the final aarch64 NEON lane path
reduced it to 32.44 ms. All three burst runs returned 720 correct answers.
These intermediate results are retained because they materially changed the
implementation choice. Their binary hashes are in each run file.

## Interpretation and limits

The first-dimension operation is `Y = A Q mod q`, with one query per column
of `Q`. Each database value contributes to eight independent accumulators
in a full batch. Database traffic is shared, while multiplication work grows
with batch size. Individual upload keys and response packing stay independent.

The requested rates were below this local host's capacity and evenly spaced,
so the batching path did not combine them. The 20 ms window increased their
latency without increasing measured throughput. Bursts did combine, but this
run does not find the saturation point. The two local workers share one M4 Max
and ample RAM; these results do not establish throughput, memory pressure, or
latency on the intended 8 GiB Linux worker hosts. The NEON optimization is
specific to aarch64; the portable packed-query path is used elsewhere.

The [rate script](measure.py), [burst scripts](measure-burst-v3.py) and
[final source patch](source.patch) capture the commands and code path.
Older burst scripts and outputs remain for the failed performance iterations.
