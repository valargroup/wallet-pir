# Live Status APM rollout

The public [Status pane](https://enhance-pir.valargroup.dev/apm/?pane=status)
monitors the live, separate coordinator, router, and worker processes. It no longer
scrapes the original synthetic single-process service.

## Sources and deployment

The APM sidecar runs on the Enhance coordinator. Its `/etc/default/pir-apm` sets:

```text
PIR_APM_STATUS_URL=http://127.0.0.1:8480/internal/status-apm
PIR_APM_STATUS_ROUTER_URL=http://127.0.0.1:8482/internal/status-apm
PIR_APM_STATUS_WORKER_URL=http://127.0.0.1:8481/internal/status-apm
PIR_APM_STATUS_TOPOLOGY='Enhance host → CPU router + worker'
```

The first source is local. Router and worker monitoring use the existing
`wallet-pir-control-sessions` supervised SSH forwards. HTTPS publication replaces
artifact forwarding; it does not replace these monitoring connections. Public
monitoring does not expose these private listeners. No new credentials were
created for this change.
The old synthetic monitoring tunnel on port 8384 was retired with the P4000 host.

The APM scraper fetches all three role samples every five seconds. Publication
metadata comes from the publisher's private `/control/active` manifest; init and
query outcomes come from the router, the only process that serves wallets. Failure of any
role scrape marks the aggregate unavailable/stale and preserves the previous
sample. Failure to obtain a valid manifest is shown as publication unavailable,
separately from monitoring reachability. Observation age uses the 20-second gate.

Recorded sidecar from the original APM rollout (not a current binary check):
`/opt/enhance-pir/releases/apm-live-d04da53aed43306a/pir-apm`.
SHA-256: `d04da53aed43306ad51742599aa86f3ada32206768b6a817f7268b6dc9a63b18`.
The existing systemd override and credential-loading wrapper are retained.

## Metric meanings

- **Router admitted processing:** worker transport and router packing included.
- **Worker admitted processing:** worker execution after admission.
- **Router packing:** final packing operation only.

The publisher has no wallet routes, so every query the dashboard counts reached the router.

Percentiles are five-minute histogram upper bounds, not exact percentiles.
Admission wait, upload before admission, client preparation and decoding are
excluded. They are **not end-to-end client load-test p99**. Overflow is shown as
`>5000` ms; it is not assigned an invented finite upper bound.

Dedicated Status HTTP alerts use separate controller-init and router-query counters; see
[Status HTTP alert coverage](../enhance/observability-alerting.md#status-http-alert-coverage).
The query table below remains based on admitted-work telemetry.

Role completion rates include unsuccessful admitted work. HTTP operation failure
counts are not present in the deployed role admission telemetry and are shown as
unavailable rather than zero. Active/waiting gauges and fixed rejection-reason
counters are displayed separately. Rejection counters count refused checks and
must not be summed as unique failed client lookups.

Role resources show process RSS and host memory/GPU observations. Router and
worker share the Status host, so their host readings must not be summed. GPU
utilization is shown only when a role reports it; the CPU host reports none.
Counter resets clear aggregate chart history. Restarts reset sidecar history;
latencies remain blank until requests occur across successful scrapes.

## Verification

```sh
cargo test --locked -p pir-apm
cargo clippy --locked -p pir-apm --all-targets -- -D warnings
curl -fsS 'https://enhance-pir.valargroup.dev/apm/?pane=status'
ssh-check root@167.99.42.60 -- systemctl is-active pir-apm wallet-pir-control-sessions
```

Status publication uses HTTPS, while public wallet requests retain the existing
`/v1/status/*` routes. Check those routes and independent encrypted answers to
verify serving; monitoring reachability alone does not establish serving health
or completion of load qualification.

## Rollback

The pre-change systemd override and environment file are backed up on the
coordinator at `/root/pir-apm-before-live.conf` and
`/root/pir-apm-before-live.env` (the environment backup is mode 0600).
Restore those files to their original locations, reload systemd and restart only
`pir-apm`. The prior binary is retained. This does not restart Status serving roles.
