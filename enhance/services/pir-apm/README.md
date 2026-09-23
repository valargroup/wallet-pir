# PIR APM

`pir-apm` is the operational sidecar for the active Enhance PIR coordinator.
It scrapes the coordinator's loopback-only Prometheus endpoint, renders a
dashboard, evaluates availability thresholds, and can emit Slack alerts.

The current coordinator exposes aggregate publication, capacity, and query
metrics. The dashboard shows those live values without presenting the legacy
HTTP latency and endpoint tables, whose metric families are no longer emitted.
It still shows coordinator health, readiness, host health, and alert state.

Defaults match the Enhance API and `enhance_*` metric families. The sidecar
only consumes fixed endpoint labels and aggregate fleet gauges; it never reads
query bodies or client identifiers.

For older coordinators, the dashboard also renders one query-path section per configured worker. These
five-minute windows are measured by the coordinator around each private worker
evaluation RPC and response decode. Request rate, inflight attempts, terminal
failures, and successful-attempt latency are therefore comparable across the
replicas without running another sidecar or exposing a worker metrics port.

Query latency is rendered as three nested scopes. **Observed total** runs from
request headers reaching the coordinator until the response is ready and
includes body receive time. **Post-body server** starts as soon as the complete
body is available and includes admission queueing, coordinator work, the worker
RPC, and response packing. Each worker card is the RPC subset of post-body
server time. Percentiles are calculated independently and cannot be subtracted
to derive the time spent between scopes.

Important environment variables include `PIR_APM_SCRAPE_URL`,
`PIR_APM_LISTEN`, `PIR_APM_ENVIRONMENT`, and optional
`PIR_APM_SLACK_WEBHOOK_URL`. The service example lives at `enhance/ops/deploy/pir-apm.service`;
production configuration is in `/etc/default/pir-apm`.

## Fleet overview and node pages

The current dashboard has an overview at `/apm/`, coordinator detail at
`/apm/coordinator/`, and worker detail at `/apm/workers/<name>/`. The overview
shows topology and availability; detailed counters, host resources, and memory
breakdowns live on the node pages. Expanded detail sections survive refresh.

Set `PIR_APM_WORKER_CONFIG` to the coordinator's JSON worker inventory (the
`groups` / `replicas` format). Production currently uses
`/etc/enhance-pir-v6/workers.json`. The sidecar reloads this file every 15 seconds,
retains the last valid inventory on an invalid update, and scrapes each worker's
private `/internal/metrics` endpoint with a five-second timeout and at most eight
concurrent requests. Coordinator scraping runs independently. Worker origins
are never rendered or accepted from browser requests.

Worker reachability and coordinator-published replica counts are distinct.
Failed scrapes retain the last successful values with a visible error and sample
age; values older than 45 seconds are stale. Unavailable memory samples remain
unavailable. Database byte counts describe loaded PIR data; modeled capacity
includes reservations and overhead and is not total machine RAM usage. This
monitoring does not add worker host or request-latency instrumentation.

For a sidecar-only deployment, build `cargo build --locked --release -p pir-apm`,
preserve `/usr/local/bin/pir-apm` and `/etc/default/pir-apm` for rollback, install
the binary atomically, and configure the inventory and current data directory in
that environment file. Preserve the existing encrypted Slack credential drop-in.
Run `systemctl enable pir-apm` and `systemctl restart pir-apm`, then verify the
local `/healthz`, all three public page types, and worker sample freshness.
Caddy strips `/apm` before proxying; the sidecar supports both stripped and full
page routes. Only the sidecar needs restarting.
