# V4 reservation fallback and consolidation — September 22, 2026

The coordinator now retries a failed elective consolidation reservation once with
ordinary placement after every affected worker acknowledges abort. It uses a new
attempt identity for the same unpublished generation and still validates the
canonical anchor before commit. The retry applies only before preparation; it
cannot roll back a committed publication. Missing abort acknowledgements retain
the recovery requirement. Worker abort now fences late reserve commands even
when the original reservation never reached that worker.

## Evidence

- [Multi-group HTTP test](multi-group-http.log): passed in 275.03 seconds. Four
  worker handlers and a coordinator run in one test process. The fixture first
  publishes five sealed 32K shards plus a growing 4K shard, then advances to six
  sealed shards plus a growing 4K shard. A destination rejects the six-shard
  reservation after both preflight checks passed. The canonical update retries
  with its original placement and a fresh attempt. A later generation moves the
  sealed shard into the older group, filling six sealed slots. Exact encrypted
  queries for the moved shard's first record succeed through both the old source
  route and the new destination route.
- [Serving HTTP regression](serving-http.log): passed encrypted queries, retained
  and expired sessions, refresh, cleanup, failover, failed commit notification
  recovery, and restart.
- [V4 tests](v4-tests.log): all 15 selected tests passed, including abort-before-
  reserve fencing, runtime exact answers, controller durability, and admission.
- [Clippy](clippy.log), formatting and documentation checks passed.

Commands:

```sh
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http \
  consolidation_reservation -- --ignored
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http distributed_round_trip
cargo test --locked --profile release-fast -p enhance-pir-server --lib v4
```

Allow at least 32 GiB RAM and 64 GiB free disk for the multi-group fixture. Its
resource-advice annotation was corrected after the run; test behavior was unchanged.
[Source hashes](source-sha256.json) identify the resulting files.

This is local functional evidence, not an off-host load or six-hour 8 GiB worker
qualification. All workers share one process and host, no per-worker cgroup is
applied, and the records are synthetic. No production services or infrastructure
changed. Recovery with unavailable replicas, memory-aware alternative placement,
and the remaining [deployment gates](../../docs/architecture_2-implementation.md)
are still outstanding.
