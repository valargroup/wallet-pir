# Init / Query homepage APM

The updated APM sidecar was installed on enhance-pir-coordinator-01 at
2026-09-24 15:46 UTC. Public `/apm/` now renders an Init / Query table above
Deployment topology. Coordinator and both worker detail pages returned HTTP 200
after installation. The service's environment and encrypted Slack credential
configuration were preserved.

Artifact SHA-256:
`14e3f12d61447f2e26a0ab094e32d19667b27eafc427668b219e0b26a26a8e39`

Host build directory: `/opt/enhance-pir-apm/homepage-20260924`.
Rollback binary/configuration directory:
`/opt/enhance-pir-apm/backup-homepage-20260924T154612Z`.
The standalone sidecar build used the workspace's exact dependency versions and
checksums, verified before a locked Linux release build. The old binary can be
staged beside `/usr/local/bin/pir-apm`, renamed atomically into place, then
`pir-apm.service` restarted. No configuration change is needed to roll it back.

## Remaining server deployment dependency

Both public rows currently show unavailable metrics. During inspection,
production port 8080 had no listener, the production coordinator was stopped,
and port 8280 belonged to an isolated qualification exercise. The user confirmed
another deployment was in progress. This rollout changed only the APM binary;
it did not restart the coordinator/workers or route public traffic to the
exercise listener.

The concurrent server deployment needs the new `http_metrics` module and its
coordinator/query-ingress integration from this workspace. For direct coordinator
queries the existing APM origin remains sufficient. If the new query ingress
service template is used on the coordinator, configure:

```text
PIR_APM_QUERY_SCRAPE_URLS=http://127.0.0.1:8083/internal/metrics
```

Verify the actual private control listener before applying this example. Multiple
public ingress instances require all their private metrics URLs, comma-separated.
These replace coordinator query metrics; do not include internal packing routers.
Restart only APM after its environment changes. Once serving is restored, verify
fresh counters after two successful scrapes and processing percentiles after
completed init/query requests. Zero traffic must not invent latency samples.

## Validation

- 62 APM tests passed, including live multi-source HTTP scraping, histogram
  aggregation, restart handling, missing metrics, idle rates, stale rendering,
  and preservation of the per-service page layout.
- 85 server library tests passed, including five instrumentation tests for slow
  upload exclusion, streaming bytes, early rejection, failed uploads, and response
  admission guard ownership.
- Server HTTP suite: eight passed, three ignored hardware/large-fixture tests.
- Separate ingress / packing / worker round-trip test: one passed.
- Local synthetic HTTP metrics produced the expected p50/p99 and transfer rates
  in the rendered homepage. Stopping the source suppressed retained numbers and
  preserved their sample age.
- Linux release smoke checks passed before installation on a private port.
- Public homepage, coordinator page, and both worker pages returned HTTP 200.
- Browser visual inspection was unavailable because no browser was connected.

Synthetic values were used only by the local test fixture, never production.

## Ingress configuration update (16:05 UTC)

Updated the running sidecar environment to scrape Query from
`http://127.0.0.1:8083/internal/metrics` and use
`/srv/enhance-pir-v7/canonical` for host disk reporting. The private metrics URL
was verified against the installed query-ingress service. Init continues to
scrape coordinator port 8080. APM restarted successfully. Configuration rollback
is in `/opt/enhance-pir-apm/backup-ingress-20260924T160533Z/pir-apm.env`.

The staged architecture2 binary contains the new HTTP metric families. At the
16:08 UTC check, production still ran `ac7cf9d` (zero HTTP metric samples), and
the installed query-ingress service was inactive. The separate deployment was
still updating its integration-test binary. No coordinator, ingress, or proxy
changes were made by this APM update. Fresh production latency/traffic verification
therefore remains dependent on that deployment's service cutover.

## Packing-router host support (16:16 UTC)

Deployed the APM sidecar with packing-router discovery from
`PIR_APM_PACKING_ROUTER_CONFIG=/etc/enhance-pir/packing-routers.json`.
The overview now includes `enhance-pir-packing-01`, linking to
`/apm/packing-routers/enhance-pir-packing-01/`. The page shows serving readiness,
sample age, admission slots, outstanding evaluations, cumulative query outcomes,
packing memory, intermediate traffic, and cumulative packing time. Public
entrypoint APM and existing node detail pages are preserved.

Artifact SHA-256:
`43a9ffa5d9c2f37a705101d21bef8e90e3d265571c58a20fa1ff9f8a37eaeb74`.
Source and release artifact: `/opt/enhance-pir-apm/packing-20260924`.
Rollback binary and environment:
`/opt/enhance-pir-apm/backup-packing-20260924T161619Z`.
Only the sidecar and its environment were changed; no server services restarted.

All 65 APM tests passed. A private-port smoke check against the live packing
router verified discovery, a fresh sample, and omission of private origins.
After rollout, the public overview, packing-router page, coordinator page, and
both worker pages returned HTTP 200. The packing router reported reachable but
not ready, controller epoch zero, and zero resident objects at that check; the
page accurately displayed that state. The concurrent server rollout is separate
from this dashboard update.

## Domain topology correction (16:26 UTC)

Replaced the detached packing-router tier and legacy worker-group boxes with
per-domain cards. The live domain 0 card contains `enhance-pir-packing-01` and
both assigned evaluation workers. Worker assignments come from coordinator
`pool.placements`; router assignments use the router inventory's domain filters
(empty means all domains). Shared routers can appear in multiple domain cards.
Placement revision/freshness, missing assignments, stale retained placement,
and other registered hosts are explicit. Host detail pages show domain membership.
No serving or placement logic, server service, or APM configuration was changed.

All 68 APM tests passed, including multi-domain association and stale-placement
regressions. A private-port test against live health/inventory verified all three
hosts inside domain 0 before rollout. The public homepage and four service pages
returned HTTP 200 afterward. The router was ready and both workers reachable.
The Init / Query table remains in place, and the old `shard-group-01` box is gone.

Artifact SHA-256:
`76faaad3df263bb383daafb6d2ff14303964ccf30c3a9f40de83e044ba9d7f50`.
Source and artifact: `/opt/enhance-pir-apm/domains-20260924`.
Rollback: `/opt/enhance-pir-apm/backup-domains-20260924T162608Z`.
