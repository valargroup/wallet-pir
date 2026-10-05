# Validation results

Every run tested commit `2bb5c0299eb752b68a5ad5fad9b156b046bef8c9` on the
development hub, in the task-owned release-fast lane through `tool-exec`.
Before each run, page cache in the task's own cgroup was released with
`memory.reclaim`; see the README for why.

| Command | Result |
|---|---|
| `cargo test --locked --profile release-fast -p transparent-native -p transparent-shard-server --lib --test round_trip --test revisions_and_cache` | pass: 15, 52, 18 (2 ignored, pre-existing) and 18 tests; 4 min 40 s; [focused.log](focused.log) |
| `cargo clippy --locked --profile release-fast -p transparent-native -p transparent-shard-server --all-targets -- -D warnings` | pass, at the same source |
| `make check-fast BASE=4c85b6c20ced1e2077245491e77d3afc98bfd644` | exit 0, every stage passed; 31 min 47 s; run once; [check-fast.log](check-fast.log) (stage and test-result lines) |

An earlier focused run at `576e756f` failed two disk-cache tests on cgroup
memory admission. That run is kept in [focused-first-run.log](focused-first-run.log).
