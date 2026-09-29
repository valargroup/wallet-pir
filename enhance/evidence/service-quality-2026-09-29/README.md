# Service-quality rollout, 2026-09-29

This is a deployment snapshot, not completion of the elapsed-time gates.

The separate Transparent APM page and shared Enhance/Status history are deployed.
Both independent native probes passed from the dedicated monitor host. Deliberate
wrong-pin and wrong-answer controls returned `oracle_invalid` and `answer_mismatch`
respectively, with nonzero exit status. No production answer was used as its own
expected value. Transparent fixture SHA-256:
`1316d72b0d72ad1aaf58b2d7ab2a4e8c6953c249767ff2fd8b824c68be6094bb`.
Its original plaintext source map still matched the fixture provenance; the sealed
anchor at height 3494910 independently matched node RPC before/after queries.

Linux builds use `target-cpu=haswell`, with separate Transparent and Enhance native
feature graphs. Local tests cover cancellation/body accounting, nested routing,
source resets/gaps, histogram merging, canonical label order, history restart and
retention, alert-family promotion isolation, probe unknown states and rollout/load
failure behavior. Captured deployed service/Caddy metrics are parser fixtures.
No CI run is claimed for this local branch.

The publisher and first recent worker passed upgrade verification. The supervised
canary/fleet run is `/opt/pir-quality/releases/20260929/fleet-rollout-2` on the
coordinator, under `transparent-quality-rollout-2.service`. Its six-hour / 300-block
canary gate must pass before the remaining workers can upgrade. The first run in
`fleet-rollout` failed during observer startup because historical shard 173 is
absent in v10; the upgrade itself passed. That failure is retained and no observation
time was reused. Continuous load resumed with explicit per-worker binary pins.

APM state remains in the existing incident database. Aggregate history is separate:
`/var/lib/pir-apm/service-quality.sqlite`. A restart retained collected history and
SQLite `quick_check` returned `ok`. Initial staging data is retained, but measurements
before the final canonical-label parser deployment are commissioning data, not
qualification evidence. The Caddy parser now selects its actual top-level subroute
scope and preserves latency versus byte units.

Existing alerts remain active. New families remain shadow. The 72-hour aggregate
observer is `pir-quality-observation.service`, writing
`/var/lib/pir-apm/quality-observation`. Full shadow review, notification-path exercise,
new-family activation and 24-hour active observation are **not complete**. The
worker rollout can progress automatically through its existing measured gates;
alert promotion still requires review of the completed observation.

Rollback records are in `/opt/pir-quality/releases/20260929/rollback` and the monitor's
`/opt/pir-monitor/quality-rollback-*` files. Stop a running rollout before rollback.
Preserve SQLite/outbox state and critical load latches. The monitor's scoped node
RPC cookie must be refreshed and its service restarted if the node rotates that
credential; failure is an unavailable oracle, never a successful probe.
