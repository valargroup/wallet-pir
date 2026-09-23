# Integrated v4 regression — September 23, 2026

This run validates the combined cache/row recovery, admission-aware new-shard
placement, pending-decision exclusion, memory-triggered demand and operations
journal handoff against the existing protocol/recovery suite.

## Results

- [Server library](library-tests.log): all 25 v4 tests passed, including durable
  control state, memory demand, admission, runtime equivalence and row repair.
- [Protocol library](protocol-tests.log): all three matching v4 tests passed,
  covering malformed binding, lifecycle boundaries and identity through reorgs.
- [HTTP campaigns](http-tests.log): all eight passed, none ignored or filtered,
  in 378.34 seconds with two test threads. This explicitly includes the normally
  ignored full-size loan/return and multi-group consolidation campaigns, plus
  commit/abort recovery, retained-query failover, inventory expansion, row-loss
  repair and alternative placement/memory-demand handoff.
- [Operations/tooling](operations-tests.log): 108 enhance operations tests ran,
  106 passed and two Linux-only tests skipped on macOS; all 17 release/tooling
  tests and the 3-test/4-test filter suites passed.
- `git diff --check` passed.

Commands:

```bash
cargo test -p enhance-pir-server --lib v4::
cargo test -p enhance-pir-server --test v4_http -- --include-ignored --test-threads=2
cargo test -p enhance-pir --lib v4
make check-ops-enhance check-tools
```

The HTTP tests use actual PIR computations and encrypted client queries. Some
failure modes are injected at HTTP middleware; the infrastructure handoff runs
only the local operations journal CLI. This run does not invoke cloud provisioning
or SSH, perform live Linux service deployment, establish full-wallet canonical
conformance, or qualify 8 GiB memory/latency over six hours. No production mutation
occurred. The source is still an uncommitted candidate, not a clean release.

Recent evidence-directory checksums matched. The older bundle validation's
`SHA256SUMS` describes the original archive inventory, not the contents of its
evidence directory; those archived binaries were not revalidated in this run.
