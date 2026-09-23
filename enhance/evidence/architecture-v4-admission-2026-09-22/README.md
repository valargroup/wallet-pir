# V4 elective-consolidation admission — September 22, 2026

Before persisting a publication operation containing elective consolidation, the
coordinator now checks the complete proposed destination assignment on both
replicas. The private `/internal/v4/admit` endpoint performs the existing worker
role and memory-admission calculation without reserving or preparing anything.
If either destination fails this check, the coordinator keeps the ordinary
canonical-update assignment and defers consolidation. Actual reservations still
repeat admission and enforce the required readiness quorum.

## Validation

- [Consolidation HTTP test](consolidation-http.log): passed against four real
  worker HTTP handlers with full-size plan metadata. A failed destination peer
  deferred the move; two successful checks admitted a sixth sealed shard. The
  test verifies no durable worker mutation and no coordinator operation was
  created by preflight. A six-shard active assignment was rejected. A preexisting
  candidate also deferred the move and remained intact.
- [V4 tests](v4-tests.log): all 14 selected tests passed, including real encrypted
  runtime checks, fencing, durable publication, capacity, and placement models.
- [Serving HTTP regression](serving-http.log): passed exact encrypted answers,
  retained sessions, expiry/refresh, cleanup, failover, commit recovery, and restart.
- [Clippy](clippy.log): server targets/features passed with warnings denied.

The preflight test uses metadata and synthetic content digests; it does not
materialize six shards, measure 8 GiB hardware, or prove the complete multi-group
consolidation workflow. Advisory success cannot prevent a later reservation
failure if pins or replica availability change. Fallback after that authoritative
failure, memory-aware alternative placement, and infrastructure integration remain
unfinished. Earlier load runs do not qualify this later source change.

Commands:

```sh
cargo test --locked --profile release-fast -p enhance-pir-server --lib elective_consolidation
cargo test --locked --profile release-fast -p enhance-pir-server --lib v4
cargo clippy --locked -p enhance-pir-server --all-targets --all-features -- -D warnings
```

[Source identities](source-sha256.json) identify the changed files. No cloud
resources or production services changed. See the remaining
[implementation and deployment gates](../../docs/architecture_2-implementation.md).
