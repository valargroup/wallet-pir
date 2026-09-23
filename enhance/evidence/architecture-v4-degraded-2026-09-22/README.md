# V4 degraded publication and catch-up — September 22, 2026

Committed-operation recovery now follows the activation acknowledgements in the
durable decision. A replica excluded before reservation has no committed candidate
to recover and cannot block that decision's completion. The coordinator still
requires recovery from acknowledged participants; this change does not erase an
unavailable participant's unfinished candidate or waive new-group readiness.

## Evidence

[The HTTP regression](serving-http.log) passed in 83.17 seconds. After bootstrap,
one peer is unavailable before reservation while the other peer's commit
notification is deliberately rejected. The newly committed view answers queries,
and notification recovery completes while the excluded peer remains unavailable.
That peer misses three published generations, then prepares the current complete
assignment when it returns. With the other replica temporarily unavailable, a
fresh client retrieves the newly appended record from the recovered peer.

The same test checks retained sessions, expiry/refresh, cleanup, later failover,
and coordinator restart. Health reports `published_replica_counts` of one during
degraded publication and two after catch-up. The corresponding per-shard
`enhance_v4_shard_published_ready_replicas` metric is checked too. These describe
the ready routes recorded at publication; they are not continuous liveness probes.

[All 15 selected v4 tests](v4-tests.log), [Clippy](clippy.log), formatting, and
Markdown link checks passed. [Source hashes](source-sha256.json) identify the
changed files.

```sh
cargo test --locked --profile release-fast -p enhance-pir-server --test v4_http distributed_round_trip
cargo test --locked --profile release-fast -p enhance-pir-server --lib v4
cargo clippy --locked -p enhance-pir-server --all-targets --all-features -- -D warnings
```

This uses a small synthetic fixture and worker HTTP listeners in one test process.
It does not qualify off-host load, memory limits, recovery across shard boundaries,
or a replica that acknowledged activation and then became unavailable. No cloud
resources or production services changed. See the remaining
[implementation and deployment gates](../../docs/architecture_2-implementation.md).
