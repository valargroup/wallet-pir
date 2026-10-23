# PIR alerting and external monitoring

The distributed alert evaluator consumes coordinator, outer-ingress, worker,
packing-router, placement, and direct Zakura RPC observations. Its state and
Slack outbox live in SQLite. Collection never waits for notification delivery.
The independent `pir-monitor` checks public HTTPS and exact decrypted records
once per minute from a separate region, and monitors APM task progress.

## Sources and semantics

- Init and query HTTP outcomes and processing latency come from the configured
  outer ingress; the coordinator serves no wallet routes.
  Router stage latency remains separate and describes successful matched work.
- HTTP totals are per-source counter deltas, summed before calculating ratios.
  Histogram buckets are merged before quantiles. Missing bandwidth does not
  disable error monitoring. Missing counters, failed scrapes, and incompatible
  histograms are unavailable, not zero.
- Domain serving health reads each packing router's assignments and unexpired
  worker promises from `/internal/health`, bound to its active publication.
  Routers must match the publisher's exact manifest and generation, and their
  combined assignments must cover every published domain.
  APM resolves private worker origins to inventory names and counts each replica
  once across routers. Unassigned workers do not count toward redundancy.
  Missing, stale, inconsistent or unconfigured observations fail coverage.
  Worker metrics reachability remains a separate monitoring check.
- Publication lag compares the published anchor against a fresh direct Zakura
  RPC read. This bypasses the publisher observer but shares its node/host;
  it is not an independent second blockchain node. Tip inactivity is distinct
  from publisher lag. A published anchor above a regressed tip is not a positive
  backlog. Lag alerts require both continuous time behind the tip and time since
  successful advancement (120s warning / 300s critical). An idle chain therefore
  does not cause an immediate alert when its next block arrives. APM restarts or
  observation gaps start a new behind-tip grace period.
- Sustained backlog alerts separately detect a publisher that keeps advancing but
  cannot keep up: at least 8 blocks for 120s (warning), or 16 blocks for 300s
  (critical). These are initial operational guardrails to review during shadow
  observation, not measured service-level objectives.
- Publication failure alerts require three consecutive failures or blocked state
  continuously for 120s (warning) / 300s (critical). A single recovered canonical
  anchor rejection does not page. Publication metrics, blocked state and the
  last-advancement timestamp must be present and valid; otherwise checks become
  unknown and coverage alerts report the missing source.
- `coordinator_ready` reads the Enhance listener's `/ready`, which stays 200 while
  another product of the shared publisher has failed. With
  `PIR_APM_PUBLISHER_URL` set to the publisher's loopback management origin, APM
  reads its `/health` product report (which always returns 200) and raises
  `publisher_product_<product>` (critical) when a product's phase is not
  `running` for 60s. A missing or malformed report makes these unknown and
  raises `coverage_publisher_*`; a product that leaves a fresh report retires.
- Publication advancement time changes only after a durable publication commit;
  failed preparation never resets it. Restart starts without an advancement
  timestamp until the first successful commit. Failure counts reflect completed
  failed publication attempts. The removed coordinator commit/abort handshake
  has no pending-queue rules; its retained incidents retire after fresh placement
  and inventory observations. Router memory and timing collection uses charged
  packing bytes and does not require the removed resident-object counter.
- Unknown data cannot recover an incident. Recovery requires two distinct fresh
  healthy samples. Valid inventory changes retire incidents for removed targets.
- `/healthz` is HTTP liveness; `/readyz` is monitoring readiness. Public aggregate
  `/apm/monitor-status` includes collector/evaluator/delivery progress, incident
  state, direct chain observation, and delivery health, never credentials or
  request contents. `monitor-pir.valargroup.dev/monitor-status` is the independent
  checker's aggregate status.

## Status HTTP alert coverage

When Status monitoring is configured, `status_init_5xx` and `status_query_5xx`
alert on HTTP 5xx divided by completed responses in the rolling five-minute
window (default >5%, at least ten completions). `status_init_latency` and
`status_query_latency` warn when processing p99 exceeds 2s / 5s respectively
for 120 seconds, with at least twenty completions. Error ratio, sample minimums,
and hold time use the shared policy; Status latency budgets can be set separately
(up to 5s, the highest finite Status histogram boundary).
These conditions use the same durable incident/outbox and shadow/active mode as
Enhance, including two distinct fresh healthy samples to recover.

Init outcomes come from the Status controller and query outcomes from the router,
including its separate query listener. Router HTTP timing includes request-body
extraction, admission wait, worker transport and packing until response creation;
it excludes network delivery of the response and client decoding. This is not
client end-to-end latency or the pane's admitted-work histogram. Worker calls and
the controller's diagnostic query-forwarding route are not added to router totals.
HTTP 4xx responses are counted as completed responses, but not server errors.
Pre-admission HTTP 5xx failures are included; rejection-check counters are not
summed as requests. Overflow latency remains >5s, not an invented finite value.

HTTP counters carry an observation schema version and process incarnation. Old
roles without these counters, malformed/inconsistent counters, scrape failures,
staleness, and counter resets yield unknown rules, not healthy zeroes. A fresh
idle pair of valid samples is healthy. A new process or observation gap starts a
new delta baseline; two samples are required. Missing coverage also raises
`coverage_status_init_*` / `coverage_status_query_*` after 45s warning / 120s
critical. Roll out updated Status roles before APM so old telemetry is not
mistaken for coverage. Unconfigured Status installations have no Status rules.

## Configuration

Install `tools/enhance/deploy/pir-apm-observability.conf` as a systemd drop-in,
retaining the deployed Slack credential wrapper and replacing only its binary
path. Set `PIR_APM_ORACLE_ANCHOR_HEIGHT` and `PIR_APM_ORACLE_ANCHOR_HASH` from the
pinned production oracle. The direct RPC loop validates that block hash each
sample so oracle-invalid and service-answer-mismatch are separate conditions.

`PIR_APM_ALERT_MODE=shadow` evaluates and persists the new rules without sending
them; existing alerts remain active. `active` disables legacy sending and enables
the durable outbox. Active mode requires a state path, RPC URL/cookie, and peer
status URL. Promote both APM and external checker only after the shadow gate.
An incident already firing in shadow is queued once on promotion.

Optional `PIR_APM_ALERT_POLICY` names a JSON file. Fields and defaults:

```json
{
  "error_ratio": 0.05,
  "error_min_requests": 10,
  "latency_min_samples": 20,
  "init_latency_seconds": 2,
  "query_latency_seconds": 5,
  "status_init_latency_seconds": 2,
  "status_query_latency_seconds": 5,
  "latency_hold_seconds": 120,
  "unavailable_seconds": 30,
  "redundancy_seconds": 120,
  "publication_warning_seconds": 120,
  "publication_critical_seconds": 300,
  "publication_warning_blocks": 8,
  "publication_critical_blocks": 16,
  "publication_failure_hold_seconds": 120,
  "quality": {
    "history_unpublished_warning_seconds": 30,
    "history_unpublished_critical_seconds": 60,
    "txid_unpublished_critical_seconds": 180,
    "lag_warning_blocks": 2,
    "lag_hold_seconds": 300,
    "last_success_warning_seconds": 300,
    "last_success_critical_seconds": 900,
    "last_success_hold_seconds": 120,
    "history_rebuild_warning_seconds": 60,
    "txid_rebuild_warning_seconds": 30,
    "rebuild_critical_seconds": 120,
    "prepare_warning_seconds": 32,
    "prepare_critical_seconds": 64,
    "capacity_warning_days": 30,
    "capacity_critical_days": 7,
    "host_floor_ratio": 0.2,
    "unit_forecast_ratio": 0.9,
    "unit_memory_ratio": 0.85,
    "unit_memory_hold_seconds": 600
  }
}
```

The `quality` object holds the Transparent service and capacity thresholds
described in [Transparent service alerts](#transparent-service-alerts) and
[Transparent RAM capacity](#transparent-ram-capacity). Any field may be left
out to keep its default; warnings must not exceed their critical limits.

The independent checker requires:

- `PIR_MONITOR_ORIGIN=https://enhance-pir.valargroup.dev`
- `PIR_MONITOR_APM_STATUS_URL=https://enhance-pir.valargroup.dev/apm/monitor-status`
- `PIR_MONITOR_ORACLE`: protected JSON file with `anchor_height`, `anchor_hash`,
  and `records` containing `position` and `record_hex`.
- `PIR_MONITOR_ORACLE_SHA256`: SHA-256 of the exact file bytes.
- `PIR_MONITOR_STATE_PATH=/var/lib/pir-monitor/incidents.sqlite`
- `PIR_MONITOR_ALERT_MODE=shadow` initially; `active` after qualification.

Its incidents link to `PIR_MONITOR_ORIGIN`'s APM dashboard, except the service
probes' (Status, Transparent and receiver), which link to
`PIR_MONITOR_PUBLIC_STATUS_URL`, by default
`https://monitor-pir.valargroup.dev/monitor-status`.

Package the independently extracted **production** oracle with its canonical
anchor; synthetic test fixtures cannot establish live correctness. Validate its
anchor with Zakura before deployment. Never update expected bytes using answers
from the PIR service being tested. If the anchor becomes noncanonical, freeze
correctness classification and regenerate/review the oracle from canonical
source records.

Both processes receive the existing production Slack webhook through encrypted
systemd credentials and `credential-exec.py`/the existing APM wrapper. Do not put
it in environment files, Terraform variables, logs, or checked-in artifacts.
The node RPC cookie is loaded as a systemd credential; restart APM after cookie
rotation so it receives the new value.

## Delivery and response

Transitions and their outbox records commit together. An independent worker uses
five-second requests and retries transport errors, 429, and 5xx with backoff and
jitter (five-minute cap); integer Retry-After is honored. Other 4xx responses are
configuration failures retried every five minutes. Events remain ordered per
incident, even while unrelated incidents can proceed. Critical reminders occur
every 30 minutes; warnings have no repeated reminders. Delivery is at least once:
a timeout after Slack accepted a message may produce a duplicate incident ID.

Investigate by check family:

| Check | First action |
| --- | --- |
| `query_5xx`, `init_5xx`, latency | Compare ingress outcomes with router outcomes; inspect admission and upstream service logs. |
| `publication_*` | Compare direct node tip and manifest; inspect publication attempts and the last durable advancement. |
| `domain_*` | Check published placement and eligible replicas, then router authority and generation-specific worker health. |
| `coverage_*` | Restore the named scraper/source/inventory before trusting retained values. |
| `canary_*` | Inspect transport/category and pinned oracle validity; distinguish wrong bytes from unavailable service. |
| `external_monitor`, `apm_progress` | Inspect systemd, loop progress, and state-database permissions on the named monitoring host. |
| `*delivery*` | Check credential presence, sanitized last failure, and oldest pending age; preserve the outbox. |
| `scaling_*` | Read `scaler/status.json` and `scaler/journal/operation.json` on the coordinator. A fenced operation proceeds only after `resolve-apply` ([elastic recent](../transparent/elastic-recent.md#actuator-operations)). |

## Deployment and rollback

1. Run APM, checker, telemetry/control/router tests and clippy. Build release
   binaries on the CI host with `tools/enhance/scripts/build-observability.sh`.
   It explicitly overrides the repository's developer `target-cpu=native` with
   the fleet's Haswell baseline. A CLI `--help` check is insufficient: run the
   isolated native workload on the deployment host before changing ExecStart,
   and verify glibc compatibility. Never deploy an AMD-native CI build to the
   Intel fleet.
2. Add `monitor.tf` to the deployed production Terraform root. Use the existing
   coordinator wrapper/lock to save a plan with `pir_monitor_enabled=true`.
   Inspect JSON: exactly four new monitor resources and no unrelated changes.
   Apply that saved plan and persist the enabled variable in protected production
   tfvars so a later plan does not attempt removal.
3. Install the monitor service, Caddy configuration, protected pinned oracle, and
   scoped credential. Preserve existing binaries and drop-ins. Update additive
   server telemetry first, then APM and the checker, initially in shadow mode.
4. Collect 24 hours of shadow evidence: no unexplained incidents or missing
   sources, one successful canary per minute, bounded resources, and healthy
   collector/delivery progress. Do not shorten this gate because compilation
   or spot checks pass.
5. Promote both services to active. Verify a clearly labeled firing/recovery
   through the real durable outbox and confirm receipt in the Slack destination.
   Exercise monitoring-task failures without interrupting serving processes.
6. Observe 24 additional hours of active alerting and record final acceptance.

For rollback, restore the prior APM ExecStart/drop-ins and restart only APM;
stop external checker sending or return it to shadow mode. Keep SQLite databases,
including WAL files, with their owning process stopped or use SQLite backup.
Retain the monitor infrastructure unless separately approved for deletion.
Additive server metrics and queue timestamps are backward-compatible and can
remain deployed. Never delete incident/outbox state to clear an alarm.

## 2026-09-26 rollout handoff

The implementation is deployed in **shadow mode** on the production coordinator
and the independent NYC monitor. Existing legacy Slack alerting remains enabled
during this gate. The new evaluator records events durably but does not send
rule-generated Slack messages until activation. Explicit verification messages
were delivered through both real outboxes; Roman confirmed receipt of both
firing/recovery pairs.

A 100-second monitor-process pause produced and recovered the expected
`external_monitor` critical shadow incident. The pause was automatically reversed
by a separate systemd timer. Serving processes were not paused for this test.
A publication-lag warning also fired and recovered during catch-up; review its
frequency in the full observation window before choosing to activate.

The evidence collector runs as `pir-observability-evidence.service` on
`wallet-pir-monitor-01`, writing one aggregate sample per minute to
`/var/lib/pir-monitor/rollout-evidence/` for 72 hours. The checked-in evidence
manifest records the final deployment time and earliest shadow-gate review.
Collection is not automatic promotion. Review the full 24-hour window before
changing `PIR_APM_ALERT_MODE` and `PIR_MONITOR_ALERT_MODE` to `active`, then observe
another 24 hours. Extend collection if activation is delayed. Preserve the
SQLite databases across mode changes and restarts.

The initial CI build inherited `target-cpu=native` and failed with SIGILL on the
Intel coordinator. Serving roles were rolled back, then rebuilt with an explicit
Haswell baseline. The corrected server passed an isolated native smoke workload
on the Intel coordinator (4,203 correct answers, zero query errors, six
publications). This smoke result is not a long-duration workload qualification;
its report deliberately retains `qualification: unqualified`.

APM/checker tests and clippy passed. The existing native packing-router tests
using the v7 session fixture remain incompatible with the native v9 format;
those two failures are not represented as passing validation. See the evidence
manifest and logs for the exact scope of completed checks.

## Publication incident investigation and restarted observations

The [2026-09-26 investigation](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/observability-fix-2026-09-26/README.md)
records the issues, corrections, tests, and new observation window. Its deployment
manifest supersedes the original shadow start time. Earlier evidence remains
retained; incident/outbox databases are not cleared. The updated observer retains
publication measurements, chain state, and active incident details so the next
review can distinguish stalls from sustained backlog and transient retries.

## Seven-day quality history and Transparent page

Each configured Transparent service has its own page,
`/apm/quality/?service=<name>`, and a tab on `/apm/`; `/apm/transparent/` opens
the `transparent` service. `/apm/quality/?service=enhance` and `?service=status`
expose the same history controls for existing services. Worker links retain the
selected service. The aggregate API is
`/apm/api/quality?service=<name>&range=1h` (`24h` and `7d` also supported).

A service page shows, for the selected range:

- **Publication freshness**: the node's height against the published height,
  the lag in blocks, how long the oldest unpublished block has waited, time since
  the last successful publication, and p50/p95/p99 of the publication cycle and
  of publication visibility, plus the canonical-anchor and public-origin checks.
- **Recent rebuild**: the txid display cycle and each of its stages (build,
  verify, digest, write, prebuild, ship, prepare, activate) at p50/p95, or the
  history publication cycle; and each worker's time to prepare a new revision.
- **Host memory**: one line per host behind the service, memory in use as a
  share of the host's memory, linking to the capacity page.
History begins at deployment; it does not backfill earlier observations.

Set `PIR_APM_HISTORY_PATH` to a writable SQLite file (defaults to `quality.sqlite`
next to the incident database). Five-second observations are persisted as minute
aggregates, retained for seven days, and merged into 1/5/30-minute display bins.
Histograms merge buckets, never percentiles. Process changes, invalid histograms,
counter resets and missing observations break continuity. Gauges retain their
latest value in each bin. Database failure and dropped batches are exposed by
the API; history work runs outside request and scrape tasks.

`PIR_APM_TRANSPARENT_CONFIG` names a root-owned JSON file listing the monitored
Transparent services. Production monitors only the v12 history and the one-query
txid display, both under `/v2` on the shared public origin; the v1 API is being
retired and is not monitored:

```json
{
  "host_snapshot": "/var/lib/pir-apm/hosts.json",
  "capacity_alert_mode": "shadow",
  "services": [
    {"service": "transparent", "kind": "history",
     "publisher_url": "http://10.142.0.3:8096/metrics",
     "status_url": "http://10.142.0.3:8096/v1/status",
     "roster": "/opt/transparent-publisher/v12/roster.json",
     "router_metrics_url": "file:///var/lib/pir-apm/edge.prom",
     "min_recent_replicas": 1,
     "public_origins": ["https://transparent-pir.valargroup.dev"],
     "path_prefix": "/v2", "alert_mode": "shadow"},
    {"service": "txid", "kind": "txid",
     "controller_url": "http://127.0.0.1:8098",
     "workers": [{"id": "transparent-pir-txid-k8-01", "role": "display-worker", "upstream": "10.142.0.18:8095"}],
     "public_origins": ["https://transparent-pir.valargroup.dev"],
     "path_prefix": "/v2", "alert_mode": "shadow"}
  ]
}
```

- `service` is a unique name of 1–32 lowercase letters, digits and hyphens. It
  names the service's page, its alert family and every one of its incident keys.
- `kind` is `history` (a publisher with a worker roster) or `txid` (a display
  controller with its display workers). Rules choose by kind, never by name.
- `path_prefix` is empty or `/v<digits>`. The public-origin check reads
  `<origin><path_prefix>/shards/init` for history and
  `<origin><path_prefix>/txid/init` for txid, and requires every origin to serve
  the coordinator's current map. Operator probes keep `/v1/health`, `/v1/ready`
  and `/v1/status` on the private listeners.
- `alert_mode` (`shadow` or `active`, default `shadow`) is the service family's
  rollout mode. `capacity_alert_mode` is the capacity family's. APM reads this
  file only at start, so the file can disagree with the running process. Each
  family records the mode it last evaluated in as `mode:quality-<service>` in
  the incident database's `metadata` table; that is the running mode. Change
  modes in production only through `transparent-alerts-*`
  ([deployment](../transparent/deployment.md#transparent-alert-activation)),
  which edits only `alert_mode`, restarts APM and verifies the running mode.
- History: `publisher_url` (its `/metrics`), optional `status_url` (default:
  the publisher's origin plus `/v1/status`), `roster` (reread every 15 s; roles
  `archive-owner` and `recent-replica`), optional `router_metrics_url`,
  `synthetic_status` and `min_recent_replicas` (1–32, default 2: the ready
  recent replicas below which the redundancy warning fires; the v12 fleet runs
  one recent worker by design and sets 1).
- Txid: `controller_url`, scraped at `/metrics` (role `display-controller`) with
  its published block and map read from `/v1/status`; `workers` with role
  `display-worker`, each scraped at `/metrics` and `/v1/ready`.

Source ids are `<service>-publisher`, `<service>-controller`, `<service>-edge`
and `<service>-synthetic`, plus each worker's id; worker ids must be unique across
services. The old single-service object is refused.

The config is read once at start, so swap the APM binary and this file
together and restart. A file that cannot be read, parsed or validated (including
an unreadable history roster or a worker id shared by two services) disables
only Transparent monitoring: APM logs one `transparent_config_invalid` JSON line
(field and service names, never file contents or URLs), keeps Enhance and Status
monitoring and alerting, shows a warning on the Enhance and quality pages, and
raises the `quality_transparent_config` warning in the shared `quality` family.
Open Transparent incidents are left as they are, not retired, until a valid
config is back.

The existing fleet roster is authoritative. No public metrics route is added.
Host resources come from per-host APM sidecars (`pir-apm-sidecar`), not SSH.
Each managed host runs one, deployed as the `apm-sidecar.observer` role from
the deployment inventory, measuring its filesystems, required volumes, memory,
swap, configured units and hooks every 15 s and serving the result over mTLS
on port 9443. The APM polls every enrolled sidecar every 5 s as the
`pir-apm-collector` identity of the dedicated monitoring CA, raises its
`host` family (below) and writes `host_snapshot` in the former sampler's shape
for the hosts whose observer settings carry `snapshot` (`sources`, main `unit`,
optional `data_path` and `size`); with `roster` in its observer inventory,
every enrolled or draining roster member is an expected host. Aggregate Caddy
metrics are read by the router's sidecar on loopback and written to
`edge_output`. `tools/scripts/wallet-pir-monitoring.py` renders the observer
inventory, reports coverage gaps against Terraform outputs, declared external
hosts and the roster, issues monitoring certificates and installs credentials
under the production lock. Each snapshot entry has `unit`, `host_id`, `size`,
`swap_total_bytes`, `swap_free_bytes` and `units`, a map from unit name to
`memory_current_bytes`, `memory_anon_bytes` (`anon` plus `shmem` from the
cgroup's `memory.stat`), `memory_peak_bytes`, `memory_max_bytes` (null when
unlimited) and `oom_kills`; a value the sidecar does not report is left out.
Caddy 2.6 metrics must be enabled in the generated router configuration. Only
the deployed top-level `subroute` handler boundary is counted; nested handler
totals must not be added. This includes responses and refusals at that route. Caddy size
estimates and service payload byte counters have different semantics.

A schema cutover moves these inputs. Since v11 (2026-10-03) the fleet roster is
`/opt/transparent-publisher/v11/roster.json` and the continuous load writes
`/srv/transparent-activity/canonical-load/v11/status.json`; point the APM config
and the observer inventory's `roster` at them, then restart `pir-apm`, which
reads both only at start. The pre-v11 roster and
`/opt/transparent-5qps-20260929/status.json` stopped changing at the cutover, so
an unmoved APM shows retired workers and a stale load. The v11 load pins worker
binaries in `canonical-load/v11/pins.json`. `/etc/pir-quality/qualified-workers.json`
belongs to the stopped v10 load and is kept as v10 rollback state; neither APM
nor the v11 load reads it.

`tools/enhance/scripts/retarget-transparent-quality.py` makes both moves. Its
`apm` mode, on the coordinator, rewrites only the named history service's
(`--service`, default `transparent`) `roster`, and its `synthetic_status` when
`--load-status` is given, and restarts only `pir-apm`; its `monitor` mode rewrites only the Transparent probe's canary,
fixture and pins after the new command passes once, then restarts `pir-monitor`.
Without `--apply` it only prints the changes. `--apply --backup-dir DIR` copies
each changed file into DIR first; restore those copies and restart the same
service to roll back. It never changes an alert mode, so check the
service's `alert_mode` and `PIR_MONITOR_SERVICE_ALERT_MODE` are `shadow` before
the move and promote nothing until the canary passes.

Transparent HTTP instrumentation measures arrivals, cancellations, status
classes, consumed request bytes, emitted response bytes, body errors and dropped
responses. Timing ends when a response is constructed, not at client receipt.
Worker evaluation/cache/admission timings remain separate. Public revision,
assignment, client address, script, row selection and query bytes are not labels
in the quality store. A cancelled handler releases the in-flight count; nested
live/static routing records each request once.

Publisher cycle and visibility distributions are separate from time since the
last publication. Canonical verification uses direct node RPC and both public
origins; a publication that changes between reads is unknown, not a mismatch.
This check runs separately from collection. RPC remains the same chain node,
not an independently operated consensus oracle.

`PIR_APM_SERVICE_MONITOR_URL` optionally reads the dedicated monitor's aggregate
Status and Transparent results. `PIR_MONITOR_SERVICE_PROBES_CONFIG` contains an
array of `{service, command, timeout_seconds}` records; the host drop-in
[`pir-monitor-service-quality.conf`](../../tools/enhance/deploy/pir-monitor-service-quality.conf)
sets it and loads the probes' node cookie. Commands are absolute
argument arrays, timeouts are 1–45 seconds, and output is bounded and sanitized.
Build the Transparent native `quality-canary` separately from native Enhance
features. Pin the fixture checksum and independently verify its canonical
anchor. Never construct expected answers from the encrypted server response.
The existing Enhance canary remains unchanged. A probe's `service` is one of
`status`, `transparent`, `receiver` or `txid`; the `txid` probe runs
`txid-quality-canary`, one private `/v2/txid` lookup checked against a pinned
entry hash ([Transparent deployment](../transparent/deployment.md#independent-canaries-under-v2)).

The Transparent canary refuses with `oracle_invalid` whenever the service's shard
schema differs from its fixture's, so a schema cutover fails it on every sample
until both change. Give it the continuous load's fixture (for v11,
`/srv/transparent-activity/canonical-load/v11/fixture.json`, exported from the
gate-verified initial publication) and derive its pins with
`tools/enhance/scripts/transparent-canary-pins.py --fixture FILE --shard-map
<live publication>/shards.json --rpc-url URL --cookie FILE`. It refuses a fixture
naming any revision the live map no longer serves sealed, and prints the fixture
SHA-256 and the anchor: the terminal block of the highest fixture shard, checked
at the node. Rebuild the canary from source that supports the new schema; its
query binds to the fixture's schema, not to the compiled default.

New alert families have independent modes, all defaulting to `shadow`:
`PIR_APM_QUALITY_ALERT_MODE` for the shared `quality` family (history writes,
the Enhance inventory, and Enhance and Status sources), each Transparent
service's `alert_mode`, the config's `capacity_alert_mode`, and
`PIR_MONITOR_SERVICE_ALERT_MODE`. Existing active
alerts and outbox state retain their mode. New rules cover metric/resource
coverage, publication identity/freshness, readiness/redundancy, HTTP outcomes,
latency, OOM/disk/memory, history writes, and independent probe availability and
correctness. Thresholds are operational guardrails, not an SLO commitment.

### Rollout and rollback

1. Build portable Linux artifacts with `target-cpu=haswell`; record source and
   artifact checksums. Run aggregate, restart, cancellation and exact-query tests.
2. Run APM on a separate loopback port with separate incident/history files and
   an override **EnvironmentFile listed last**. systemd EnvironmentFile values
   override Environment entries. Validate all sources and public-safe JSON.
3. Preserve current binary paths, systemd overrides, incident databases and
   encrypted Slack credentials. Acquire the existing production deployment lock
   before changes. Switch APM only after staging verification.
4. A Transparent worker update must use the existing canary and full-fleet
   deployment gates. Pause continuous load before intentional worker restarts;
   verify pinned binary identity and health before resuming. Never delete a
   correctness/OOM latch as routine deployment cleanup.
5. Observe new rules in shadow for 24 hours, review every candidate and test
   firing/recovery through the existing notification outbox. Then activate only
   the new families and observe another 24 hours. These elapsed-time gates cannot
   be satisfied by source tests or a short smoke test.
6. Roll back binaries/config overrides to recorded paths and restart affected
   services. Retain SQLite history and incident/outbox files. Demoting a family
   to shadow does not discard a previously announced incident's recovery.

## Transparent scaler alerts

The optional `scaling` family reads the Track A scaler's
[`scaler/status.json`](../transparent/elastic-recent.md#processes-and-files)
on the same host at every five-second evaluation. It is enabled only when
`PIR_APM_SCALER_STATUS` names that file by absolute path, requires
`PIR_APM_STATE_PATH`, and shares the incident database and Slack outbox.
`PIR_APM_SCALING_ALERT_MODE` is `shadow` (default) or `active`, independent of the
other families; `active` without a status file refuses to start. Without
`PIR_APM_SCALER_STATUS` no status file is read and Enhance, Status and quality
alerting are unchanged; the only write is retiring stored `scaling_*` incidents
(below).

Reads stop at 1 MiB. A missing, oversized, unparseable, `schema` ≠ 1 or
future-dated file, or contents with `updated_unix` older than 90 s, make every
rule unknown, and unknown never recovers an incident. Only the heartbeat rule
reads stale contents. An absent or `null` field makes the rules that need it
unknown. Every condition has resource `transparent-scaler`.

| Key | Severity | Fires when | Hold |
| --- | --- | --- | --- |
| `scaling_status_coverage` | warning | file or `heartbeat_unix` missing, unreadable or invalid | 180 s |
| `scaling_heartbeat` | warning | `now − heartbeat_unix > 180 s` | – |
| `scaling_operation_deadline_<id>` | warning | `operation.deadline_exceeded` | – |
| `scaling_operation_fenced_<id>` | critical | `operation.fenced` | – |
| `scaling_budget` | warning | `budget.actions_left` or `budget.destroys_left` ≤ 0 | – |
| `scaling_demand_held` | warning | `desired_recent > serving_recent` with non-empty `holds` | 30 min |
| `scaling_forecast_warning` | warning | `forecast.days_to_recent_budget < 30` | – |
| `scaling_forecast_critical` | critical | `forecast.days_to_recent_budget < 7` | – |
| `scaling_recent_serving` | critical | `serving_recent == 0` | 60 s |
| `scaling_awaiting_operator` | warning | non-empty `awaiting_operator` | – |
| `scaling_orphan` | warning | non-empty `orphans` | – |

Per-operation keys end in the operation id (characters outside `[A-Za-z0-9._-]`
become `_`), so one stuck operation is one incident. Once fresh status no longer
names that operation, its incident recovers. Every recovery needs two fresh healthy
samples: distinct `updated_unix` values, or distinct heartbeats for
`scaling_heartbeat`. Critical incidents repeat every 30 minutes like the others.

Roll out with the same gates as the quality families: 24 hours in shadow, then 24
hours active. Removing `PIR_APM_SCALER_STATUS` stops evaluation and retires every
stored `scaling_*` incident on resource `transparent-scaler` at the next
evaluation, through the normal `RETIRED` transition: an incident whose `FIRED`
reached Slack gets exactly one closing message from the outbox, a shadow-only one
gets a shadow event and no message, and other families' incidents and modes are
untouched. An enabled family whose status file is missing or unreadable is not
retired: its rules are unknown, its incidents stay active and
`scaling_status_coverage` fires.

## Enhance memory and growth alerts

The `capacity` family watches Enhance worker and packing-router memory and
warns before the database outgrows its placement. It shares the incident
database and Slack outbox. `PIR_APM_CAPACITY_ALERT_MODE` is `shadow` (default)
or `active`, independent of the other families.

Workers and the packing router append their host memory and their own systemd
cgroup's memory to `/internal/metrics`: `<prefix>host_memory_available_bytes`,
`host_memory_total_bytes`, `cgroup_memory_bytes`, `cgroup_swap_bytes`,
`cgroup_memory_high_events_total` (times the service crossed `MemoryHigh` and
was pushed into reclaim), `cgroup_memory_max_events_total` and
`cgroup_oom_kills_total`. The prefix is `enhance_worker_` or
`enhance_packing_router_`. A field that cannot be read is omitted, never zero.
These are the signals the focused memory gate fails on; process RSS alone does
not show them. Workers also report the modelled resident need of their current
assignment (`model_total_bytes` against `model_limit_bytes`). The coordinator
reports `enhance_capacity_remaining_rows`, the rows left before placement needs
more workers, and `enhance_rows_before_next_carveout`, the rows left before the
growing shard fills.

Resources are `worker_<name>`, `router_<name>` and `placement`.

| Key | Severity | Fires when | Hold |
| --- | --- | --- | --- |
| `capacity_coverage_<resource>` | warning | role unreachable, stale, or missing a memory field | 120 s |
| `capacity_memory_<resource>` | warning | host memory available < 15% | 120 s |
| `capacity_swap_warning_<resource>` | warning | cgroup swap > 64 MiB | 300 s |
| `capacity_swap_critical_<resource>` | critical | cgroup swap > 512 MiB | 120 s |
| `capacity_reclaim_<resource>` | warning | `memory.high` events increased in the last 5 minutes | – |
| `capacity_oom_<resource>` | critical | OOM kills increased in the last 5 minutes | – |
| `capacity_model_worker_<name>` | warning | assignment modelled at ≥ 85% of the resident limit | 300 s |
| `capacity_model_limit_worker_<name>` | critical | assignment modelled over the resident limit | 120 s |
| `capacity_placement_warning` | warning | < 32,768 placement rows (one shard) left | – |
| `capacity_placement_critical` | critical | < 8,192 placement rows left | – |
| `capacity_shard_rollover` | warning | < 4,096 rows before the growing shard fills | – |

At the October 2026 growth rate of roughly 200 rows a day, the placement
warning gives about five months and the rollover warning about three weeks.
Both are row counts rather than forecasts, so a growth burst shortens the
notice. A counter that decreases is a service restart and starts a new window.
Stale or missing samples are unknown and never recover an incident. Workers or
routers removed from a valid inventory retire their incidents.

Deploy the worker and router binaries first, so coverage is already healthy
when the APM starts evaluating. Then follow the quality-family gates: 24 hours
in shadow, review every candidate, then activate.

## Transparent service alerts

Each configured Transparent service is its own alert family (`quality-<name>`)
in its config `alert_mode`; keys are `quality.<name>.<rule>`, so the Slack title
and incident id name the service. Every rule reads the newest sample: a stale or
missing value is unknown, never fires and never counts as a recovery. Holds are
applied by the incident engine. "Coordinator" is the history publisher or the
txid display controller.

| Key (after `quality.<name>.`) | Kinds | Severity | Fires when | Hold |
| --- | --- | --- | --- | --- |
| `inventory` | both | warning | roster or worker list invalid | 45 s |
| `publication_canonical` | both | critical | published block hash differs from the node's | – |
| `publication_origins` | both | critical | a public origin serves another map (only with origins) | 30 s |
| `publication_*_coverage` | both | warning | that check unavailable | 45 s |
| `publication_warning` | history | warning | oldest unpublished block > 30 s | – |
| `publication_critical` | history | critical | oldest unpublished block > 60 s | 60 s |
| `publication_critical` | txid | critical | oldest unpublished block > 180 s | – |
| `publication_lag` | both | warning | coordinator ≥ 2 blocks behind its node | 5 min |
| `last_success_warning` | both | warning | no successful publication for 5 min while the node is ahead | 2 min |
| `last_success_critical` | both | critical | the same for 15 min | 2 min |
| `stalled`, `halted` | txid | critical | controller reports stalled or halted | – |
| `withdrawn` | history | critical | public coverage withdrawn | 60 s |
| `rebuild_warning` | both | warning | > 5% of 15-min coordinator cycles above 60 s (history) or 30 s (txid) | 60 s |
| `rebuild_critical` | both | critical | > 5% of 15-min coordinator cycles above 120 s | 60 s |
| `prepare_<worker>_warning`, `_critical` | both | warning / critical | > 5% of 15-min revision prepares above 32 s / 64 s | 60 s |
| `ready_<worker>` | archive owner, display worker | critical | not ready on the published map | 60 s |
| `recent_availability` | history | critical | no recent replica ready on the published map | 60 s |
| `recent_redundancy` | history | warning | fewer ready recent replicas than the service's `min_recent_replicas` (default 2; a one-replica fleet sets 1, leaving only `recent_availability`) | 120 s |
| `coverage_<source>` | both | warning | source unavailable | 45 s |
| `http_coverage_<source>` | history | warning | publisher or worker lacks HTTP observation version 1 | 45 s |
| `http_errors_<source>` | both | warning | 5xx > 5% with ≥ 10 responses in 5 min | – |
| `latency_<source>_<endpoint>` | both | warning | p99 > 2 s (metadata) or > 5 s (setup, query), ≥ 20 samples | 120 s |

History keeps its 30 s and 60 s unpublished-age rules, which already page
earlier than 180 s; the txid display seals every block, so it has only the 180 s
page and relies on the lag rule for its warning. Time since the last success
counts only while the node's height is above the published height, so an idle
chain does not fire it, and a coordinator that reports 0 (no success since its
restart) is unknown. Both levels hold 2 minutes
(`last_success_hold_seconds`): after a quiet chain the first new block is
already older than 5 minutes since the last success, and the hold gives the
coordinator time to publish it. The cycle is `transparent_publication_cycle_seconds`
(history) or `txid_display_cycle_seconds` (txid); prepare is
`transparent_shard_prepare_seconds{phase="total"}` (history workers) or
`transparent_display_prepare_seconds` (display workers). A limit fires when
more than 5% of the window's samples lie above the smallest bucket edge at or
above it; samples beyond the last finite edge count as above. The cycle
histograms have edges at …10, 30, 60, 120, 600 s and prepare at …16, 32, 64,
128, 300 s, so the defaults sit on edges; a limit between edges acts as the
next edge up. A percentile interpolated inside a bucket would fire far below
its limit (a steady 34 s prepare read as a 62 s p95). A 15-minute window
without a cycle or prepare is unknown. Display workers have no outer HTTP layer
yet, so their 5xx and latency rules stay unknown until they export `pir_http_*`.
Histogram deltas need a `*process_start_time_seconds` gauge on each scraped
process to detect restarts.

Host memory, disk and OOM rules moved to the sidecar `host` family; incidents
the former `memory_`, `disk_`, `oom_` and `host_coverage_` rules left open are
retired. When a service's inventory is valid, incidents of a source it no longer lists
are retired, and incidents of a service no longer configured (or keys of the old
single-service shape) are retired by the shared family.

## Host family (sidecars)

The `host` family has its own mode (`mode` in `/etc/pir-apm/observers.json`)
and keys `host:<host>:check:<check>` and `host:<host>:coverage`. Sidecars decide
each check; the APM propagates status and reason. Built-in checks: `disk:<mount>`
per writable local block filesystem (deduplicated by device, root-reserved
blocks excluded; below 15% available warns at once), `volume:<path>` per
required volume bound to its filesystem UUID (absent, replaced by its parent
directory, another filesystem or read-only is critical from the first
observation), `memory` (< 15% available for 120 s warns), `swap`,
`unit:<unit>:{active,restarts,oom}` (an automatic restart warns and a new OOM
kill is critical for 5 minutes), `certificate` (serving certificate expires in
< 30 days warns, < 7 critical), `hook:<name>` and `edge`. A check older than
45 s, a missing configured check, an unreachable, unauthenticated, malformed,
other-schema, other-host or other-configuration document is unknown and never
recovers an incident; coverage warns after 45 s without a valid document,
including hosts never observed. Recovery needs two distinct healthy
observations. Enrollment persists until the host is retired in the deployment
inventory (`decommissioned`) or by the scaler (roster `intent: retired`).

## Transparent RAM capacity

The APM's host snapshot (`host_snapshot`) has one entry per source with
`at`, memory, disk, restarts and `oom_kills`, and may add `host_id` (stable host name; several sources may share
a host), `size` (DigitalOcean size), `swap_total_bytes`, `swap_free_bytes` and
`units`: `{"<unit>": {"memory_current_bytes", "memory_anon_bytes",
"memory_peak_bytes", "memory_max_bytes" (null when unlimited), "oom_kills"}}`.
When several sources share a host, the host row takes the newest observation
and the largest worker cache residency among them. Hosts with no worker,
such as the coordinator and router, appear under their host id. Absent fields
are unknown.

Every fresh observation is folded into a daily row per host and per unit (UTC
days, kept 180 days, in the quality history database): the lowest available
memory, the highest sampled unit anonymous memory (current use only when the
host does not report it), its MemoryMax, the highest worker cache residency and
the latest size. A unit's current use and `memory.peak` include page cache the
kernel reclaims under pressure, and `memory.peak` covers the unit's whole run,
so neither feeds a forecast or alert; the page shows both for reference.
Minute history stays seven days.

A straight-line least-squares fit through the daily peaks of the last 14 days
and of the last 30 days forecasts, for a host, the days until available memory
falls below 20% of total, and for a unit, the days until its peak passes 90% of
its MemoryMax. A window with fewer than three days is unknown; a flat or falling
trend never reaches the floor; a peak already past the floor is 0 days. Alerts
use the sooner of the two known forecasts.

`/apm/capacity/` lists one row per host and per unit: size, RAM (or MemoryMax),
the 24-hour peak, the unit's peak against MemoryMax, its current use and
lifetime peak (page cache included, for reference), headroom from the 24-hour
peak to the floor, growth per week, days to floor on each window, the next size
and its monthly price, and **reprovision** when either forecast is under 30
days. The next size follows s-2vcpu-4gb ($24) → s-4vcpu-8gb ($48) →
s-8vcpu-16gb ($96) → m-4vcpu-32gb ($168) → m-8vcpu-64gb ($336) →
m-16vcpu-128gb ($672) → m-32vcpu-256gb ($1344); an `-intel` size moves to the
same next tier. The 24-hour peak is kept in memory and restarts with the APM.

These alerts form the `transparent_capacity` family, apart from the Enhance
`capacity` family, in the config's `capacity_alert_mode` (default `shadow`).
The host memory below 15% and new-OOM rules stay in each service family.

| Key | Severity | Fires when | Hold |
| --- | --- | --- | --- |
| `transparent_capacity_floor_warning_<host>` | warning | sooner forecast < 30 days to the host floor | 30 min |
| `transparent_capacity_floor_critical_<host>` | critical | sooner forecast < 7 days | 30 min |
| `transparent_capacity_unit_warning_<host>_<unit>` | warning | sooner forecast < 30 days to 90% of MemoryMax | 30 min |
| `transparent_capacity_unit_critical_<host>_<unit>` | critical | sooner forecast < 7 days | 30 min |
| `transparent_capacity_unit_memory_<host>_<unit>` | warning | unit anonymous memory > 85% of MemoryMax | 10 min |

Forecasts older than 15 minutes, or for a host not observed in 15 minutes, are
unknown. A host or unit not observed for an hour leaves the alerts, which
retires its incidents; the page lists it as stale for up to a day, then drops it.
