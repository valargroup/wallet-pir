# Performance

Enhance measurements answer different questions. A public load test includes
client cryptography and the network path. An isolated worker fixture measures
preparation, publication and query behavior under its own workload. Neither is
a whole-wallet sync measurement or proof of maximum production capacity.

## Schema-9 16-bit profile

Schema 9 keeps six instances and the 12,288-column database shape while moving
from 29 to 33 records per row. The response and 192-MiB encoded database
allocation are unchanged. Query precision rises from 41 to 46 bits, adding
`logical_rows × 5 / 8` bytes to the switched-query payload: 5 KiB at 8,192
logical rows, 10 KiB at 16,384, and 20 KiB at 32,768.

In the isolated full server benchmark, median p14/p16 times were 8.304/8.160 ms
on an M4 Max and 25.880/26.010 ms on an AVX-512 Xeon. Each p16 result used 33
records per row. These measurements cover server cryptography without HTTP,
coordinator aggregation or application work. See
[16-bit expansion](plaintext16-expansion.md) for the capacity arithmetic and
correctness scope.

## Public baseline: September 14, 2026

The [retained run](../evidence/public-baseline-2026-09-14/README.md) tests the public
HTTPS origin with one, two, four and eight concurrent clients. Each step has a
10-second warmup and a 60-second measured phase, with at least 30 seconds between
steps. The tool samples a repeatable pool of 1,024 positions using seed 42 and
creates fresh query randomness for every request. Each step connects separately
and holds that generation throughout warmup and measurement.

The client ran on an Apple M4 Max with 16 logical CPUs and 128 GiB RAM, macOS 26.2,
using a release build at the revision recorded in the manifest. The public
session advertised schema 7, 65,536 logical rows, 4,096 columns, two PIR instances
and five populated shards. Health reported two configured workers. Server SKU,
region, release SHA and host resource usage were not independently observed.

| Concurrent clients | Successful queries | Errors | Requests/s | p50 (ms) | p95 (ms) | p99 (ms) |
|---:|---:|---:|---:|---:|---:|---:|
| 1 | 157 | 0 | 2.62 | 374.5 | 409.9 | 582.7 |
| 2 | 303 | 0 | 5.05 | 387.8 | 416.8 | 545.8 |
| 4 | 621 | 0 | 10.35 | 381.7 | 416.5 | 515.8 |
| 8 | 1,164 | 0 | 19.40 | 394.5 | 524.8 | 965.1 |

All four steps passed the agreed stop thresholds, with 2,245 successful measured
queries and zero errors. Health probes remained successful. Lightweight repository
checks briefly overlapped the four-client step on the client machine; no isolated
client-CPU claim is made.

| Concurrent clients | Prepare p50 / p99 (ms) | HTTP p50 / p99 (ms) | Decode p50 / p99 (ms) |
|---:|---:|---:|---:|
| 1 | 28.831 / 38.815 | 343.551 / 553.983 | 0.854 / 1.632 |
| 2 | 28.879 / 35.935 | 358.143 / 513.023 | 0.853 / 1.432 |
| 4 | 28.831 / 33.311 | 351.743 / 486.911 | 0.859 / 1.112 |
| 8 | 28.463 / 40.639 | 364.799 / 935.423 | 0.857 / 1.217 |

These are single runs with a short tail sample, not repeated estimates with
confidence intervals. Current coverage and the client/network environment differ
from the historical figures below. Do not interpret their difference as a
performance regression or improvement without a matched comparison. The test
validates successful response decoding and record encoding, not equality against
independently retrieved canonical transactions.

## What the load tool measures

The [load tool](../tools/loadtest/src/load.rs) is closed-loop: each client starts
the next request after its previous request finishes. Parallelism is client
concurrency, not the number of server workers. It does not model a fixed external
arrival rate or account for the requests such a workload would have queued.

Total latency includes preparation, HTTP and decoding. Preparation constructs
fresh keys and an encrypted query. HTTP includes request transfer, server work
and response transfer. Decode includes PIR decoding, slot extraction and record
validation. Error attempts contribute to the total histogram; only successful
attempts contribute to stage histograms. Initialization, preflight and warmup
are excluded from the reported measured histograms.

The reported request rate is completed attempts divided by the configured
measurement duration. In-flight requests drain after the deadline and still count,
so it is not strictly completions within a wall-clock window. Report errors and
successful counts alongside that rate. Independent stage percentiles do not add
to total percentiles, and subtracting them does not isolate server compute time.

## Earlier results and benchmark evidence

| Evidence | Reported result | What limits the comparison |
|---|---|---|
| [Historical production report](../evidence/reported-performance/README.md) | 6,512 queries in 300 s; 21.71 requests/s; zero errors; p50 322.6 ms, p95 483.3 ms, p99 1.75 s | Undated prose without raw query logs or a complete run identity; described as an eight-worker test |
| [Local summary retained during this rewrite](../evidence/undated-local-summary/README.md) | 6,389 queries in 300 s at concurrency 8; 21.30 requests/s; zero errors; p50 334.847 ms, p95 554.495 ms, p99 1,208.319 ms | Unknown run date, source revision, server hardware and client environment; a different run from the prose report |
| [September 13 isolated c-4 preflight](../evidence/preflight-2026-09-13/README.md) | 16 shards, 12 publications, 1,316 exact-answer queries; paired-client p99 303 ms | Short fixture at revision `2338dcdac3ea71838c7388b49a19fe360784cbbb`; raw bundle not committed |

The c-4 preflight also reported cold preparation of 53.5 seconds, a longest
subsequent publication of 8.2 seconds, peak cgroup memory of 5.51/5.62 GiB, and
zero swap or OOM events. Those are reported preparation/publication benchmark
figures, not a completed six-hour qualification. No current Enhance-specific
standalone microbenchmark harness was found in this checkout. Legacy YPIR tests
and transparent-product benchmarks do not establish Enhance performance.

The [qualification utility and supervisor](capacity-expansion.md#supervised-hardware-evidence)
provide the reproducible path for full-shard hardware measurements. They activate
synthetic generations and must run on isolated workers, never on a serving pair.
The required duration, memory, publication, failover and online-append acceptance
criteria remain separate from this public baseline.

## Payload size and traffic

Payload size depends on the advertised logical geometry, not just the count of
populated records. For the pinned encoder, request bytes are:

```text
8 + serialized_packing_keys_len(rlwe) + ceil(logical_rows * query_bits / 8)
```

The response has a 16-byte prefix plus one encoded body per PIR instance. At
2,048 coefficients and a 20-bit response modulus, each instance contributes
5,120 bytes, so two instances yield 10,256 response bytes. This excludes HTTP,
TLS and initialization. Initialization carries separate base64 public material;
measure its actual response size when budgeting session refresh traffic.

An offline query constructed from the captured 65,536-row session has a
430,088-byte body (420.0 KiB). With a 10,256-byte response, that is 440,344 bytes
(430.0 KiB) per query. At the measured 19.4 requests/s, the derived payload rates
are 8.34 MB/s upload and 0.20 MB/s download, or 68.34 Mbit/s combined. These are
encoder-derived sizes multiplied by measured throughput, not packet-capture
measurements. The [size check](../evidence/public-baseline-2026-09-14/payload-size-provenance.json)
records the helper, input and method.

The older report's 258,056-byte upload applies to its reported 32,768-row geometry
and encoding. It must not be presented as today's upload size.

### Derived schema-8 sizes

This section describes the historical schema-8 profile.

Every figure in this section above is a schema-7 measurement: nine records per
6,633-byte row, two PIR instances. Schema 8 changes both terms. Widening the row
to 29 records divides the logical row count by roughly three, which shrinks the
upload; it also takes the row from two PIR
instances to six, which triples the response and the published public material.
The table below is **derived from the pinned encoder**, not measured: it is the
same arithmetic as the formulas above, and it reproduces the measured schema-7
sizes exactly, which is the only evidence offered for it.

| Layout and count | Logical rows | Instances | Upload | Response | Combined | Public material |
|---|---:|---:|---:|---:|---:|---:|
| Schema 7, 467,255 records | 65,536 | 2 | 430,088 | 10,256 | 440,344 (430.0 KiB) | 28,672 |
| Schema 8, 467,255 records | 16,384 | 6 | 169,992 | 30,736 | 200,728 (196.0 KiB) | 86,016 |
| Schema 8, 475,137 records | 32,768 | 6 | 258,056 | 30,736 | 288,792 (282.0 KiB) | 86,016 |
| Schema 7, 589,825 records | 131,072 | 2 | 790,536 | 10,256 | 800,792 (782.0 KiB) | 28,672 |

The 196.0 KiB row is the narrowest the schema-8 layout is ever expected to be,
and it is the shortest-lived. `logical_rows_for` rounds to a power of two, so
crossing 29 x 16,384 = 475,136 positions doubles the logical row count and adds
about 86 KiB to every query. Do not quote 196 KiB as the schema-8 query size
without stating the count it holds at.

The last row is why the comparison is still worth making. Schema 7 reaches its
own power-of-two boundary at 9 x 65,536 = 589,824 positions, after which its
query is larger than schema 8's will be for a long time. Between the two
boundaries the saving is about 34%, not the 54% that a same-day comparison of the
first two rows suggests.

The public material is published once per generation and carried in the
initialization response, which is base64-encoded: 86,016 raw bytes become roughly
115 KiB of init body against roughly 40 KiB today. Session setup is therefore
about three times more expensive under schema 8, and a client that refreshes its
session often enough gives back the per-query saving. Budget setup and query
traffic together for the session length the wallet actually uses. Express payload
rates as measured or derived explicitly; MB is decimal, while KiB and GiB are
binary units. PIR payload rate alone does not establish network-link utilization.

## Reproduce a bounded baseline

Build the release tool, choose a new evidence directory, and retain the command,
source revision, build identity, health/init responses, logs and JSON output.
Never overwrite a retained run. A single step from the repository root is:

```sh
cargo build --locked --release -p enhance-pir-load-test
mkdir -p /tmp/enhance-baseline-new
./target/release/enhance-pir-load-test \
  --server https://enhance-pir.valargroup.dev \
  --parallelism 1 --duration 60s --warmup 10s --seed 42 \
  --max-error-rate 0 --slo-p99-ms 5000 \
  --json-out /tmp/enhance-baseline-new/c1-summary.json
```

For the agreed envelope, repeat at concurrency 2, 4 and 8, waiting at least 30
seconds between steps. Check healthy serving state and compatible initialization
before each step. Probe health every five seconds and terminate after two
consecutive failed probes. Stop escalation after any measured error or p99 above
five seconds. The load tool checks its error/p99 thresholds only after a step;
the [captured supervisor](../evidence/public-baseline-2026-09-14/run.py) performs
the additional health monitoring. Its recorded paths belong to that run; copy it
to a fresh run directory rather than executing it over retained results.

These stop conditions bound this experiment. They are not a production SLO or
permission to run larger saturation tests. Follow the shared
[evidence requirements](../../evidence/README.md#required-run-metadata) and report
failed, interrupted and incomplete work as well as successful queries.
