# Remaining work

Schema 9 (33 records per 24,321-byte row) is implemented and tested locally.
The last retained public observation found schema 7; see the [status
record](status.md) for what the local work does and does not establish. The gaps
below are open.

## Schema-9 cutover

- **Deploy nothing until wallet consumers are ready.** Schema 9 and earlier
  profiles have no overlap: each client rejects the other's `init`. This repository publishes no
  client package and has no FFI bindings, so its consumers are whoever pinned a
  commit SHA and built `enhance-pir`. That set is not knowable from here. Either
  enumerate and update them, or decide to serve both layouts from separate
  origins, before the cutover -- not after.
- Generate and independently review a correctness certificate for the actual
  schema-9 snapshot and setup, meeting the selected per-query `2^-128` target.
  The synthetic `2^-143` fixture bound does not satisfy this production gate.
- Prepare the schema-9 journal into `/srv/zakura/enhance-data-r33` and confirm the
  receipt names both the release and `records-per-row=33`. The schema-8
  `/srv/zakura/enhance-data-r29` journal and `/srv/enhance-pir/artifacts-r29` are
  rollback data and must survive the cutover untouched.
- Measure, on the fleet, what isolated qualification cannot settle: server query
  time on Intel `c-4` hardware, the real public upload/response/init byte counts,
  and worker residency under production query load rather than a fixture. Worker
  Schema-8 residency across a full retained window and publication time were
  measured in isolation -- 5,760 MiB peak and 11.4 s at three shards, in
  [the rollout evidence](../evidence/schema8-rollout-2026-09-20/README.md) -- but
  that run is an AMD host with synthetic records and is explicit that memory
  under load, failover and process uptime need separate evidence. It is not the
  six-hour, 300-publication qualification.
- **Requalify group residency before production.** The prior three-shard
  schema-8 fixture measured
  5,760 MiB against a 6,144-MiB `MemoryHigh`; four measure 6,144 MiB with 2,073
  reclaim events. Schema 9 raises three-shard capacity to 811,008 positions but
  retains the same database shape per shard. The ownership contract assigns
  shard four to the second group. See
  [capacity expansion](capacity-expansion.md).
- Re-run `online_topology`'s full-group test somewhere it fits. Crossing the
  group boundary now means six schema-9 shard runtimes across two pairs;
  resident preprocessing -- the same arithmetic as the capacity question above,
  and more than the shared CI runners have.

## Carried forward

- Reconcile the current binary SHA, host inventory, active data paths and release
  receipts. Determine which migration steps have already been completed before
  executing the [deployment](deployment.md) or [expansion](capacity-expansion.md) runbooks.
- Record intended wallet-client conformance and adoption, including schema
  validation, session refresh, authenticated recovery and mixed-transaction handling.
- Recover existing acceptance evidence or qualify the exact candidate on isolated
  c-4 workers for six hours and 300 publications. Cover memory/swap, exact answers,
  retained sessions, replica failure, online range-boundary append and publication lag.
- Assemble the combined acceptance receipt from that evidence and reconcile the
  required 24-hour production observation, legacy-worker retirement and expansion
  enablement prerequisites. A public baseline or short fixture report is insufficient.
- For future performance comparisons, retain matched workload, coverage, geometry,
  client environment and server identity. Add repeated measurements and isolated
  server benchmarks before drawing saturation or hardware-capacity conclusions.

If an operator explicitly waives qualification or initial observation, use the
[operator-acceptance receipt](capacity-expansion.md#explicit-operator-acceptance).
Record waived gates as waived. This documentation update grants no waiver.
