# Live Status dashboard verification — 2026-09-25

Deployed APM SHA-256:
`d04da53aed43306ad51742599aa86f3ada32206768b6a817f7268b6dc9a63b18`.

The Status pane now scrapes private live coordinator/router/worker telemetry,
with publication metadata from the coordinator manifest. The previous synthetic
source is no longer selected. Per user instruction, coordinator forwarding is
excluded from the displayed query table and charts. Router, worker, and packing
histograms are labeled as server measurements rather than end-to-end client p99.

Validation:

- 78 APM tests passed, including aggregation, source failure, percentile overflow,
  and the absence of coordinator forwarding from the rendered pane.
- Strict Clippy passed for all pir-apm targets.
- Live wiring smoke before the final label removal: 400/400 correct at 20 QPS for
  20 seconds, zero HTTP failures or unstarted arrivals; client p99 378 ms. This
  exercised the private diagnostic coordinator path and is not router-direct or
  six-hour qualification evidence.
- Public HTML showed live generation and resources. During the wiring smoke,
  displayed histogram p99 upper bounds were router 100 ms, worker 10 ms, packing
  75 ms. Those are scrape-window observations, not ongoing guarantees.
- Final public-page assertions are recorded in `public-page-checks.json`.
- Public Status init remains 404; public Enhance init returned 200.
- Browser runtime reported no available browser; HTML/data verification completed,
  but no visual browser inspection is claimed.

Only the APM sidecar was restarted. Status serving processes were unchanged.
Deployment sources and rollback instructions are in `enhance/docs/status_apm_rollout.md`.
