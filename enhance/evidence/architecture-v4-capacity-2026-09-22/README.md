# V4 capacity demand validation — September 22, 2026

The coordinator now persists growth observations, count/role capacity forecasts,
and expansion requests before publication, including polls with an unchanged
anchor. A positive configured fallback prevents missing observations from implying
zero growth. Forecasts preview successive loan and return placements and keep
one outstanding pair request across polls, reorgs, and restarts. Registration
satisfies that request without changing its identity. The four-group ceiling
produces a capacity warning instead of a fifth-pair request.

This is not automatic cloud expansion: the infrastructure adapter, resource
journal, and qualification receipts remain unfinished. Forecasts do not prove
memory admission under retained revisions, query pins, or reorg overlap.

## Evidence

- [Capacity tests](capacity-tests.log): four passed, covering the five-shard
  lender boundary, available spare pairs, nonzero fallback, reorgs, same-second
  observations, clock rollback, durable store reopen, and the fleet ceiling.
- [V4 library tests](v4-lib-tests.log): eleven passed before the final two extra
  capacity tests were added; the final capacity suite above includes those tests.
- [Inventory HTTP test](inventory-http.log): passed atomic registration, failures,
  replay, immutable existing endpoints, and restart persistence.
- [Clippy](clippy.log): server targets/features passed with warnings denied.
- [Process manifest](manifest.json): four workers and a coordinator on one shared
  macOS host. The harness configures an eight-row/second fallback to trigger demand
  with a small fixture, observes `successor-5-pair-2` as pending, atomically adds
  the second pair to inventory, and verifies the same request becomes registered.
- [Load report](load-c2.json): 1,664 correct answers, zero incorrect answers, zero
  errors, and 41.215 ms p99 at concurrency two over approximately 15 seconds.

The fixture starts at 67 records and appends during serving. The second group
remains idle: this tests demand persistence, registration and continued queries,
not a full-size cross-group placement transition or host memory qualification.
The process manifest records binary identities; [source hashes](source-sha256.json)
identify the relevant source snapshot. No cloud or production changes occurred.

Reproduce after building optimized binaries:

```sh
python3 enhance/ops/scripts/test-v4-local.py --out /tmp/v4-capacity-evidence \
  --seconds 15 --concurrency 2 --expand-inventory
cargo test --locked --profile release-fast -p enhance-pir-server --lib v4::capacity
```

Remaining work is listed in the [implementation status](../../docs/architecture_2-implementation.md).
