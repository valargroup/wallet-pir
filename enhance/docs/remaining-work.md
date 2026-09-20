# Remaining work

Schema 8 (29 records per 21,373-byte row) is implemented and tested locally.
Schema 7 is what the public origin serves; see the
[status record](status.md) for the September 20 reading and for what the local
work does and does not establish. The gaps below are open.

## Schema-8 cutover

- **Deploy nothing until wallet consumers are ready.** Schema 8 and schema 7 have
  no overlap: each client rejects the other's `init`. This repository publishes no
  client package and has no FFI bindings, so its consumers are whoever pinned a
  commit SHA and built `enhance-pir`. That set is not knowable from here. Either
  enumerate and update them, or decide to serve both layouts from separate
  origins, before the cutover -- not after.
- Prepare the schema-8 journal into `/srv/zakura/enhance-data-r29` and confirm the
  receipt names both the release and `records-per-row=29`. The live
  `/srv/zakura/enhance-data-v7` journal and `/srv/enhance-pir/artifacts-v7` are the
  rollback data and must survive the cutover untouched.
- Measure, on the fleet, what isolated qualification cannot settle: server query
  time on Intel `c-4` hardware, the real public upload/response/init byte counts,
  and worker residency under production query load rather than a fixture. Worker
  residency across a full retained window and publication time are now measured
  in isolation -- 5,760 MiB peak and 11.4 s at three shards, in
  [the rollout evidence](../evidence/schema8-rollout-2026-09-20/README.md) -- but
  that run is an AMD host with synthetic records and is explicit that memory
  under load, failover and process uptime need separate evidence. It is not the
  six-hour, 300-publication qualification.
- Decide the capacity question in [capacity expansion](capacity-expansion.md).
  Three schema-8 shards measure 5,760 MiB against a 6,144-MiB `MemoryHigh`, so the
  margin is about 380 MiB and the fourth shard is the one to watch. A second group
  takes no shards until the first holds sixteen, so adding one buys nothing.
  Not urgent at 467,912 positions; not optional for much longer.
- Re-run `online_topology`'s full-group test somewhere it fits. Crossing the
  group boundary now means 32 schema-8 shard runtimes, which is roughly 12 GiB of
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
