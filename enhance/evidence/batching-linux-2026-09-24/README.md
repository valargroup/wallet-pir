# Adaptive batching on isolated 8 GiB Linux workers — September 24, 2026

## Decision

Do **not** move multi-query batching into the proposed worker-serving
architecture. Use one query per scan and no collection delay on the target
8 GiB x86 workers. The measured prototype had a configurable batch path
(`ENHANCE_BATCH_MAX_SIZE=1..8`, `ENHANCE_BATCH_MAX_DELAY_MS=0..20`); its
x86 default was size one and zero delay. The prototype is retained in the
[source patch](source.patch), not in the main server code. A local aarch64
[comparison](../batching-local-2026-09-24/README.md) found a gain for bursts;
that hardware has not been qualified as an 8 GiB worker.

At 16 offered QPS, singleton scans completed **458/480** queries with
**2.4 s p99**, versus **416/480** and **10.5 s p99** for experimental SSE2
batching up to eight. Three repeated singleton runs completed all requests at
12 and 14 QPS. The requested 0.1, 1, 2, 4 and 8 QPS sweep completed every
request correctly with no added collection wait. The scalar batch run was
slower still, but preceded a scheduler fix and is less directly comparable.

## Method

Two isolated DigitalOcean `c-4` hosts in `ams3` each ran one worker with a
full 768 MiB synthetic shard (1,081,343 records; 32,768 logical rows). The
coordinator shared one of those hosts. A separate `s-8vcpu-16gb-amd` host in
the same VPC ran the open-loop client. Every completed response was checked
against a positional fixture oracle. There were no publication or reorg
events. These hosts were created only for this measurement and then removed.
The [host manifest](metadata.json), [driver](drive.py), [server fixture
script](measure.py), raw JSON reports, metric snapshots, [source patch](source.patch),
and [checksums](SHA256SUMS) are retained. The final measured server binary
has SHA-256 `e9cc757ba787b6a9ccb0acd27ffdb98ccba5131eb212015dd144eb4f72244f1a`.

The smooth-rate sweep used one query every 10 seconds, then 1, 2, 4 and 8
QPS. It ran at batch size one and zero added wait. All completed responses
were correct, with no HTTP errors or arrivals that the client could not
start. The p99 at 0.1 QPS has only twelve observations.

| Offered QPS | Correct / offered | p50 ms | p95 ms | p99 ms |
|---:|---:|---:|---:|---:|
| 0.1 | 12 / 12 | 187.4 | 213.5 | 213.5 |
| 1 | 90 / 90 | 181.6 | 198.7 | 214.9 |
| 2 | 180 / 180 | 182.5 | 205.4 | 213.4 |
| 4 | 360 / 360 | 185.3 | 208.1 | 218.4 |
| 8 | 720 / 720 | 195.1 | 215.4 | 226.6 |

The coordinator dispatched 1,362 singleton batches across these steps.
Aggregate queue wait was 41.1 ms, about 30 microseconds per request. The
[full sweep](final-size1-low/run.json) contains counters before and after
each step. Its binary predates a later metric-label change; that change does
not affect scheduling or evaluation.

## Saturation and batch comparison

Each preliminary [scalar batch-of-eight](distributed-scalar-size8-quick/run.json),
[experimental SSE2 batch-of-eight](distributed-sse-size8-quick/run.json), and
[singleton](distributed-size1-quick/run.json) run used 30-second steps. The
SSE2 experiment was discarded because it remained slower on this hardware;
it is absent from the final source. The scalar batch run also preceded an
early worker-slot release fix, so the SSE2 and singleton runs give the
cleaner scheduler comparison.

| Configuration at 16 offered QPS | Correct / 480 | Unstarted | HTTP 429 | p99 ms |
|---|---:|---:|---:|---:|
| Scalar batch up to 8 | 143 | 233 | 104 | 12,362 |
| Experimental SSE2 batch up to 8 | 416 | 64 | 0 | 10,519 |
| Singleton, zero delay | 458 | 21 | 1 | 2,429 |

On the worker-only host in that SSE2 step, a multi-query scan averaged
196.7 ms per batch and 131.1 ms per query. The singleton step averaged
86.4 ms per query there. This is a direct observation of the extra arithmetic
cost overwhelming shared database reads on this CPU, although the compared
steps were separate runs under load.

Three repeated 60-second singleton threshold sweeps at 12, 14 and 16 QPS
and a 30-second eight-at-once burst are retained as
[`final2-threshold-1`](final2-threshold-1/run.json),
[`final2-threshold-2`](final2-threshold-2/run.json), and
[`final2-threshold-3`](final2-threshold-3/run.json). All 12 and 14 QPS
arrivals completed correctly in all three repetitions. At 16 QPS, the
three runs completed 955, 933, and 922 of 960 offered queries. They had
5, 4, and 2 unstarted arrivals, respectively; the latter two also had
23 and 36 HTTP 429 responses. This places the practical no-loss
threshold near 14 QPS for this particular topology; 16 QPS is marginal.

## Limits

The coordinator shared CPU and memory bandwidth with one worker, so these
figures are a qualification of the isolated test topology, not a production
fleet capacity claim. The shard data were synthetic and the workload did not
exercise generation changes. The original expectation of several-fold gain
from shared reads was not met on these x86 workers. A different arithmetic
kernel or CPU should be benchmarked before enabling multi-query batches in
production. Public wallet query and response formats did not change; worker
batching uses an internal endpoint.
