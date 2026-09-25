# Status contention and admission deployment — 2026-09-25

Public Status remains disabled. These measurements do not qualify production.

## Changes deployed

- Dedicated one-thread preparation pool, independent of four query packing threads.
- Bounded role admission: four executing/eight waiting/250 ms wait; coordinator
  retains sixteen forwarding slots. Five-second request budget includes admission.
- Persistent HTTP clients, bounded idle connection reuse, explicit rejection metrics.
- Separate authenticated SSH query transport on coordinator loopback 8492, leaving
  publication and heartbeat on the existing control transport.
- Load latency includes scheduled arrival delay, with cumulative stage timings.

Final binaries:

- Coordinator: `d481baad0504093ece49c325aaa3cb2fff6bf114d21f50f945c76488bda41240`
- P4000 CUDA roles: `ecbc488fd0c3326b9eea64b9574456d54ecad79fa05bdbe3131584fe1ae04ee0`

## Load results

Each run offered 1,200 arrivals over 60 seconds at 20 QPS while live publication
continued. Unstarted arrivals count as failures; no run passed.

| Variant | Correct | HTTP failures | Unstarted | End-to-end p99 |
|---|---:|---:|---:|---:|
| Isolated preparation + admission | 1,103 | 0 | 97 | 1,729 ms |
| Same, without resource polling | 1,009 | 7 | 184 | 4,162 ms |
| Add HTTP connection reuse | 1,024 | 43 | 133 | 1,643 ms |
| Separate query/control SSH transport | 1,158 | 0 | 42 | 1,612 ms |

These short sequential trials are diagnostic, not a controlled causal comparison.
The pooled trial recorded query-round-trip p99 1,625 ms, while client preparation
p99 was 27 ms and decode p99 4 ms. All successful router permit holds were below
200 ms and worker holds below 75 ms. Delay is concentrated outside admitted router
processing. SSH TCP statistics show retransmissions and congestion-window changes;
they support transport contention as a contributor but do not fully attribute every
failed request.

In the split-transport trial, all 42 unstarted arrivals occurred in the first ten
seconds. The following five ten-second intervals each completed all 200 arrivals,
with interval p99 respectively 409, 270, 238, 390, and 396 ms. Initial transport
ramp-up remains a blocker; omitting it would hide failed offered traffic. Preserve
the complete report when evaluating changes to transport or recovery behavior.

## Verification and remaining work

- Full `make check` for initial contention/admission changes: 755 Rust tests passed,
  zero failed, five ignored; strict workspace Clippy and tool checks passed.
- Final transport/config/diagnostic changes: 35 focused Status tests and strict
  Clippy passed. Real TCP test verifies reuse across cloned HTTP clients.
- Synthetic encrypted validation passed with graceful connection shutdown for the
  pooled worker-loss case, session retry, freshness, and epoch fencing.
- Production CUDA distributed validation at 1,572,864 entries passed before the
  forwarding follow-ups; evidence is included separately.
- Public Status init returned 404; public Enhance init returned 200 after deployment.
- Resolve transport startup/recovery latency and repeat 20-QPS load, then longer
  qualification. No clean six-hour combined live-publication/load run is claimed.
- Coordinator placement of packing preparation remains conditional on transferring
  96 MiB hints and approximately 288 MiB matrices within the freshness budget.
- The previous long soak was stopped and marked interrupted before deployment.
  Its later interval overlapped build activity and is not a clean comparison.

See the two implementation plans in `enhance/docs/status_plan_*.md`. Raw timings,
metrics, and the earlier 429 attribution are retained with this evidence.
