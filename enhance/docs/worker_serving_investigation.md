# Direct worker serving: implementation and measurement gate

Status: investigation for D6 of [the architecture update](architecture_update.md),
September 24, 2026. No direct worker serving path is implemented by this note.

## What the current code actually does

| Stage | Current owner and bound | Consequence for D6 |
|---|---|---|
| Public query route | Coordinator `/v1/enhance/query` | Public origin cannot yet select a worker. |
| Body admission | Coordinator: 4 active requests, 16 waiting for at most 2 seconds; the active permit is acquired before body reception, with a 512 KiB body limit and 30-second deadline | The global four-query ceiling stays until the public route moves. Rejected requests are also drained, bounded by the body limit. |
| Decode and validation | Coordinator `Packing::query_coefficients`; validates binding, framing, upload keys and switched query | Workers receive only decoded coefficients today. |
| Evaluation admission | Worker: two permits, acquired before the internal JSON body (1 MiB limit) is buffered | This protects the internal evaluation, but its permit ends before Axum serializes the returned JSON body. It does not account for a public upload or packed response. |
| Packing | Coordinator `Packing::pack`, in a blocking task; revalidates and deserializes upload keys a second time | Packing state and per-query CPU remain on the coordinator. |
| Worker memory admission | `Inner::budget` counts distinct database units, growth and transition reserve, plus a fixed 728 MiB overhead, against a 6.5 GiB model limit | The 728 MiB is not an established bound for per-domain packing state, query pins, concurrent public requests, or publication while those requests run. |

Sources: `coordinator.rs` query and `query_body`, `worker.rs` `evaluate`
and `Inner::budget`, `runtime.rs` `Packing`, and `control.rs` memory constants.
The [deployment guide](deployment.md) also says the worker port is private and
the worker API has no application-layer authentication. The origin needs a
controlled private upstream path before it can route directly to workers.

## Admission contract for the direct path

Acquire a worker-local request permit **before reading a public body**. Keep it
until the response has been encoded and delivered to the server's bounded output
buffer, or until the request is cancelled. The permit must also pin the chosen
published session and its packing state for the same interval. A queue may hold
only metadata and an open body stream; if the reverse proxy buffers uploads,
that buffer must have its own explicit per-worker and fleet bound. A body deadline
and exact maximum length remain required. A blocked response writer must not
retain an unaccounted packed response.

Let `A` be concurrent admitted requests, `B` the maximum buffered upload,
`Q` decoded coefficients and keys, `I` evaluation intermediate, `R` encoded
response, and `T` transient packing allocations. The admission charge is at
least `A × (B + Q + I + R + T)`, in addition to resident databases, packing
state for every retained domain, pinned generations, publication preparation,
the 512 MiB guard, and host reserve. Peak overlap matters: the terms cannot be
replaced by a per-stage maximum until lifetimes are proven disjoint. The
current 728 MiB overhead cannot be assumed to include these terms without a
production measurement. A semaphore alone bounds concurrency; memory refusal
must use the ledger with current publications and pins.

The existing `preprocess` example now emits `pack_samples_ms`, `query_bytes` and
`response_bytes` for four verified encrypted queries at each domain size.
`pack_samples_ms` times the production `Packing::pack` method after evaluation,
including its validation and key deserialization. It is an isolated elapsed timing,
not throughput or memory qualification. The previously recorded c-4
preprocessing run measured **packing setup** (about 6.1–6.3 seconds for 4K–32K
domains), not per-query packing or packing-state residency. The older
SimplePIR baseline's pack timings used a different implementation and profile.

## Routing and failure contract

The coordinator should publish a versioned placement table of domain/session
to ready replicas only after each replica has activated that exact session.
The origin may choose among those replicas, retry a different ready replica on
connection failure or an explicit pre-execution busy response, and preserve the
wallet's query bytes unchanged. It must not silently retry after an ambiguous
mid-response failure: doing so can repeat expensive work and may change
linkability. The worker checks the session binding before evaluation and returns
an explicit expired-session result for a stale generation. Publication and
retention must hold packing state while an admitted query pins it. The worker's
readiness response must distinguish service health from readiness for the
specific domain/session; a general process health check is insufficient.

The origin continues to expose the existing wallet URL and TLS boundary.
Workers remain reachable only from that origin and the coordinator's control
network. The origin needs a bounded body stream, upstream timeout, overload
mapping to 429, and response size bound. Placement publication must be atomic
from the origin's perspective; on loss of one replica, only the remaining
published-ready replica is eligible. If none is ready, fail closed with 503.

## Production measurement required before the cutover

1. On an 8 GiB Linux worker with full-size domains, record `Packing::new`
   retained bytes per domain and the cgroup peak while query packing runs at
   the intended admission limit. Sample `memory.current`, `memory.peak`, CPU,
   allocation high water, and worker model components during concurrent
   publication and retained-generation expiry. Reset `memory.peak` between
   trials or use an isolated cgroup. Record the exact binary revision and
   placement, not just the host name.
2. Run the extended `preprocess` example in release mode on the worker for an
   isolated packing baseline; use its raw per-query timings and sizes rather than a
   four-sample median as a capacity claim. Then exercise the actual worker
   HTTP path with fresh wallet queries under sustained offered rates, a slow
   upload, a slow reader, one replica unavailable, and publication. Report
   successful QPS, p50/p95/p99 end-to-end latency, 429/503 counts, and peak
   cgroup memory at each offered rate.
3. Set the worker's request count and memory charges from the worst observed
   concurrent overlap, with a margin below the 6.5 GiB model limit. Repeat
   with the intended batch scheduler once D5 lands. Reject before body reception
   whenever that reservation cannot be made. Keep the public origin on the
   coordinator until exact-answer checks and the memory gate pass.

This gate is deliberately unresolved: no full-size production worker packing
CPU or memory observation accompanies this change, and the origin's published
routing protocol is still a separate implementation task.
