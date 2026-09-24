# PIR APM

`pir-apm` is the operational sidecar for the active Enhance PIR coordinator.
It scrapes the coordinator's loopback-only Prometheus endpoint, renders a
dashboard, evaluates availability thresholds, and can emit Slack alerts.

The overview includes a compact Init / Query APM table alongside the fleet
summary. Headline p50/p99 use rolling five-minute histograms. Init runs from
handler entry until response readiness. Query uses matched successful samples
from the packing routers: **Worker** measures only matrix-vector evaluation,
**Packing** measures final packing after evaluation, and **Total** runs from
router handler entry to response readiness minus body-read duration. Total
includes admission and internal transport, but not public ingress time or
response transmission. Stage percentiles are independent and are not additive.
Missing worker timing suppresses all three samples; it is never recorded as zero.

Each endpoint has one-hour latency and arriving-request charts with one-minute
points. Hover anywhere over a sample column (or focus it with the keyboard)
to see the UTC timestamp and latency in milliseconds for each visible series.
Query has a p50/p99 toggle (default p99), preserved during refresh.
History is bounded and held only in memory; restarting APM clears it. Missing
samples, scrape failures and process restarts create gaps. Headlines refresh
and telemetry samples every five seconds; fleet inventory remains 15 seconds.

Requests · last 10s counts arrivals in the ten complete monotonic seconds before
the current second. It includes uploading, in-flight and rejected requests and
appears after ten seconds of process uptime. Init is counted at the coordinator;
Query is counted once at the outer ingress. The request graph uses cumulative
arrival-counter deltas over approximately one-minute scrape windows.

Upload (client → service) and download (service → client) are payload bytes per
second over the latest successful scrape interval, not whole-machine network
traffic or average request size. Body counters advance as data is consumed or
emitted, including partial transfers. HTTP/TLS overhead is excluded. Fresh idle
counters show zero; missing instrumentation, warm-up, restarts, scrape outages,
and samples older than 45 seconds show `—` as appropriate. Each row shows the
last successful metrics sample; failed scrapes never refresh its timestamp.

Defaults match the Enhance API and `enhance_*` metric families. The sidecar
only consumes fixed endpoint labels and aggregate fleet gauges; it never reads
query bodies or client identifiers.

For older coordinators, the dashboard also renders one query-path section per configured worker. These
five-minute windows are measured by the coordinator around each private worker
evaluation RPC and response decode. Request rate, inflight attempts, terminal
failures, and successful-attempt latency are therefore comparable across the
replicas without running another sidecar or exposing a worker metrics port.

Legacy coordinator detail pages also render nested timing scopes. **Observed total** runs from
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
shows init/query APM, topology, and availability; detailed counters, host resources, and memory
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

## Public entrypoint instrumentation

The server exports `enhance_http_arrivals_total`,
`enhance_http_arrivals_last_10_seconds`, `enhance_http_request_processing_duration_seconds`,
`enhance_http_requests_total`, `enhance_http_request_body_bytes_total`, and
`enhance_http_response_body_bytes_total` with fixed `init` / `query` endpoint
labels. Histograms count responses, including errors after a complete upload.
No bodies, session identifiers, or dynamic paths enter metric labels.

By default, both rows consume the coordinator metrics endpoint. For separate
query ingress processes, set `PIR_APM_QUERY_SCRAPE_URLS` to a comma-separated list
of their private full `/internal/metrics` URLs (maximum eight). This **replaces**
the coordinator as the Query traffic source; Init still uses the coordinator.
Query latency comes from packing-router `enhance_query_stage_duration_seconds`
histograms discovered through `PIR_APM_PACKING_ROUTER_CONFIG`.
List only outer public ingress instances, never internal packing routers. Keep
these control listeners private. Sources are scraped concurrently; deltas are
computed independently across restarts and histogram buckets are combined
before calculating fleet percentiles. A missing source marks the aggregate row
unavailable rather than silently reporting partial traffic.

The sidecar and server must both be updated for populated rows. A sidecar-only
rollout against an older server safely shows unavailable metrics. Preserve
production origins during concurrent qualification runs: an isolated exercise
listener is not a replacement for a stopped production coordinator.

## Packing router hosts

Set `PIR_APM_PACKING_ROUTER_CONFIG` to the coordinator's packing-router inventory
(for example `/etc/enhance-pir/packing-routers.json`). It is the same JSON array
of `name`, private control `url`, `query_url`, and `domains` registrations used
by the server. APM only uses the name and control origin. It reloads the inventory
every 15 seconds, retaining the last valid inventory if an update is invalid.

The overview groups packing routers and evaluation workers together by shard/domain,
below the coordinator.
Each host links to `/apm/packing-routers/<name>/` with readiness, freshness,
available admission slots, outstanding evaluations, cumulative query outcomes,
packing time, intermediate payload volume, and charged packing memory. These
are service measurements; charged memory is not total host RAM. The existing
Init / Query APM table remains the public-entrypoint view.

The sidecar independently scrapes private `/internal/health` and
`/internal/metrics` endpoints, using five-second timeouts and at most eight
concurrent hosts. A reachable but unready router is distinct from a scrape
failure. Failed scrapes retain and label previous values; samples older than
45 seconds are stale. Packing-router readiness and inventory errors contribute
to the overview's fleet health. Private origins, session digests, and internal
worker addresses are never rendered on the public pages. Unknown host pages
return 404. Both stripped and `/apm`-prefixed routes are supported.

Only the sidecar and its environment need updating for this feature; no router
or coordinator restart is required.

## Domain topology

For pool placement, each shard/domain card contains its assigned packing routers
and evaluation workers. Worker membership comes from `pool.placements` in the
coordinator health response, rather than the legacy inventory group name.
Packing-router membership uses each registration's `domains` list; an empty or
omitted list means all domains, matching the server. Shared hosts can therefore
appear in multiple cards while linking to the same host detail page.

The topology shows placement revision and sample age. Invalid or failed health
responses retain the last valid placement with a warning, without refreshing its
timestamp. Missing placement never invents a group relationship; registered
hosts remain accessible under “Other registered hosts.” Unknown assigned workers
are marked as monitoring unavailable. Inventory changes update router domain
assignments without discarding a same-host metrics sample. Worker detail pages
show domain membership, and packing-router pages show configured domains.

Legacy coordinators without pool placement retain the old inventory-group view
when packing-router monitoring is not enabled. No serving or placement logic is
changed by this visualization.
