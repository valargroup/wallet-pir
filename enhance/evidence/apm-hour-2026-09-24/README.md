# APM hourly graphs and query stage timing — 2026-09-24

Deployed directly over SSH with the production lock held. The user explicitly
approved interrupting the six-hour qualification and restarting immediately.
The prior run was stopped at 17:10:05 UTC; its artifacts remain under
`/root/architecture2-deploy`, with `sustained-interrupted-at.txt` and
`sustained-interruption.txt`. It is **not a completed six-hour qualification**.
The associated samplers were stopped so they do not attribute the new build to
the prior binary hash. No new six-hour qualification was started.

Public query admission was paused during rollout and reopened at 17:11:49 UTC.
All four server hosts use `/opt/enhance-pir/releases/apm-hour-cdc8e7f7f6d0`.
Workers were restarted before the router, ingress, and coordinator. Readiness
required both published worker replicas, ready router and ingress, and zero
coordinator-resident packing objects. Existing state and APM credentials were
preserved. The current Caddy configuration, previous service definitions, APM
binary, and stopped-coordinator control backup are in `/root/apm-hour-deploy`.

Build: Rust 1.91.0, release-fast, `-C target-cpu=skylake-avx512`, on
`roman-ipir-bench-8vcpu`. Binary hashes and source manifests are in `source.json`.
The binary was staged and its hash checked on every host before interruption.

Validation:
- 88 server library tests passed.
- 71 APM tests passed, including one-hour retention, five-minute histogram
  windows, arrival counter/reset behavior, SVG gaps, and collector failures.
- Slow upload and rejected-request tests confirm arrivals are counted before
  completion; the six HTTP metrics tests passed again after adding assertions.
- Real wallet HTTP integration through ingress/router/workers passed on macOS
  and the production-compatible Linux release build, including populated stage
  histograms and replication/publication checks.
- Homepage and coordinator, worker, and packing-router detail pages returned 200.
- Four SVG charts, stage table, nonzero arrival counts and all stage percentiles
  were observed on the public homepage. Private addresses were absent.
- Browser runtime had no connected browser; validation used served HTML, Rust
  tests and live metric checks, without claiming a visual browser review.

The new latency histograms contain matched successful samples. Worker covers
matrix-vector evaluation only; Packing covers the final pack call; Total is
router handler entry to response ready minus body-read time. Percentiles are not
additive. Traffic counts come once from public ingress, and Init from coordinator.
History is memory-only, uses one-minute samples, and starts empty on APM restart.

Rollback of this instrumentation-only change: pause queries under the production
lock, restore previous binary paths (remove `99-apm-hour.conf` drop-ins on
coordinator/ingress/workers and restore the router's recorded prior symlink),
restart in worker/router/ingress/coordinator order, verify readiness and restore
Caddy. Restore the saved APM binary atomically. Keep current control/publication
state; do not restore an old control snapshot after new publications.

Post-deployment public smoke: **180/180 exact answers**, zero errors and zero
incorrect answers, plus 26 correct warm-up queries. Both worker replicas remain
published and the coordinator advanced from generation 96 to 98 during the check.
The smoke p99 was 4292.607 ms; this is a correctness/smoke check, not a latency
SLO qualification. See `smoke.json`. The homepage contained 14 plotted points,
including all three query stages, after history warm-up; see
`homepage-check.json`. All five running server processes were hash-verified in
`running-binaries.json`.
