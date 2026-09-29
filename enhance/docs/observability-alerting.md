# PIR alerting and external monitoring

The distributed alert evaluator consumes coordinator, outer-ingress, worker,
packing-router, placement, and direct Zakura RPC observations. Its state and
Slack outbox live in SQLite. Collection never waits for notification delivery.
The independent `pir-monitor` checks public HTTPS and exact decrypted records
once per minute from a separate region, and monitors APM task progress.

## Sources and semantics

- Init HTTP outcomes and processing latency come from the coordinator. Query
  outcomes and processing latency come from the configured outer ingress.
  Router stage latency remains separate and describes successful matched work.
- HTTP totals are per-source counter deltas, summed before calculating ratios.
  Histogram buckets are merged before quantiles. Missing bandwidth does not
  disable error monitoring. Missing counters, failed scrapes, and incompatible
  histograms are unavailable, not zero.
- Domain serving health combines published eligible replica counts with ready
  routers that have an eligible worker for that domain. Worker metrics
  reachability is a separate monitoring check. Standby workers do not count
  toward domain redundancy.
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
- Queue timestamps are operational metadata only. Existing persisted decisions
  without timestamps begin age tracking when first loaded after upgrade.
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

Install `enhance/ops/deploy/pir-apm-observability.conf` as a systemd drop-in,
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
  "publication_failure_hold_seconds": 120
}
```

The independent checker requires:

- `PIR_MONITOR_ORIGIN=https://enhance-pir.valargroup.dev`
- `PIR_MONITOR_APM_STATUS_URL=https://enhance-pir.valargroup.dev/apm/monitor-status`
- `PIR_MONITOR_ORACLE`: protected JSON file with `anchor_height`, `anchor_hash`,
  and `records` containing `position` and `record_hex`.
- `PIR_MONITOR_ORACLE_SHA256`: SHA-256 of the exact file bytes.
- `PIR_MONITOR_STATE_PATH=/var/lib/pir-monitor/incidents.sqlite`
- `PIR_MONITOR_ALERT_MODE=shadow` initially; `active` after qualification.

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
| `publication_*`, `pending_*` | Compare direct node tip and manifest; inspect publication attempts and oldest unacknowledged decision. |
| `domain_*` | Check published placement and eligible replicas, then router authority and generation-specific worker health. |
| `coverage_*` | Restore the named scraper/source/inventory before trusting retained values. |
| `canary_*` | Inspect transport/category and pinned oracle validity; distinguish wrong bytes from unavailable service. |
| `external_monitor`, `apm_progress` | Inspect systemd, loop progress, and state-database permissions on the named monitoring host. |
| `*delivery*` | Check credential presence, sanitized last failure, and oldest pending age; preserve the outbox. |

## Deployment and rollback

1. Run APM, checker, telemetry/control/router tests and clippy. Build release
   binaries on the CI host with `enhance/ops/scripts/build-observability.sh`.
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

The [2026-09-26 investigation](../evidence/observability-fix-2026-09-26/README.md)
records the issues, corrections, tests, and new observation window. Its deployment
manifest supersedes the original shadow start time. Earlier evidence remains
retained; incident/outbox databases are not cleared. The updated observer retains
publication measurements, chain state, and active incident details so the next
review can distinguish stalls from sustained backlog and transient retries.

## Seven-day quality history and Transparent page

`/apm/transparent/` is a separate service page. `/apm/quality/?service=enhance`
and `?service=status` expose the same history controls for existing services.
Worker links retain the selected service. The aggregate API is
`/apm/api/quality?service=transparent&range=1h` (`24h` and `7d` also supported).
History begins at deployment; it does not backfill earlier observations.

Set `PIR_APM_HISTORY_PATH` to a writable SQLite file (defaults to `quality.sqlite`
next to the incident database). Five-second observations are persisted as minute
aggregates, retained for seven days, and merged into 1/5/30-minute display bins.
Histograms merge buckets, never percentiles. Process changes, invalid histograms,
counter resets and missing observations break continuity. Gauges retain their
latest value in each bin. Database failure and dropped batches are exposed by
the API; history work runs outside request and scrape tasks.

`PIR_APM_TRANSPARENT_CONFIG` names a root-owned JSON file:

```json
{
  "publisher_url": "http://127.0.0.1:8094/metrics",
  "roster": "/opt/transparent-publisher/roster.json",
  "router_metrics_url": "file:///var/lib/pir-apm/edge.prom",
  "synthetic_status": "/opt/transparent-5qps-20260929/status.json",
  "host_snapshot": "/var/lib/pir-apm/hosts.json",
  "public_origins": [
    "https://transparent-pir.valargroup.dev",
    "https://enhance-pir.valargroup.dev"
  ]
}
```

The existing fleet roster is authoritative. No public metrics route is added.
The host sampler uses explicitly configured private SSH targets and loopback
Caddy metrics. Its output contains resource numbers and fixed source names.
Caddy 2.6 metrics must be enabled in the generated router configuration. Only
its `reverse_proxy` handler boundary is counted; nested handler totals must not
be added. This boundary excludes direct file-server responses. Caddy size
estimates and service payload byte counters have different semantics.

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
array of `{service, command, timeout_seconds}` records. Commands are absolute
argument arrays, timeouts are 1–45 seconds, and output is bounded and sanitized.
Build the Transparent native `quality-canary` separately from native Enhance
features. Pin the fixture checksum and independently verify its canonical
anchor. Never construct expected answers from the encrypted server response.
The existing Enhance canary remains unchanged.

New alert families have independent modes: `PIR_APM_QUALITY_ALERT_MODE` and
`PIR_MONITOR_SERVICE_ALERT_MODE`, both defaulting to `shadow`. Existing active
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
