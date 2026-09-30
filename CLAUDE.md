# Repository guidance

The root workspace contains Enhance PIR and transparent shard recovery as
top-level components. Keep each product's crates, services, tools, operations,
documentation and evidence under `enhance/` or `transparent/`; keep only shared
infrastructure and checks at the root. `enhance/crates/transparent-spend-pir` is
preserved legacy source, excluded from the active workspace and CI. Active Enhance
crates do not depend on it.
`transparent/tools/transparent-measure` uses the current shard stack and remains part of the
backfill workflow; its extrapolated baseline is not a whole-wallet measurement.

Follow [AGENTS.md](AGENTS.md) for shared iteration and validation rules. Use
`make check-fast BASE=<sha>` during iteration; comprehensive checks belong to
final validation or CI. Optimized `release-fast` runs full-shard tests without
deployment LTO. Preserve Cargo package/binary names when moving tools.

`demos/legacy-spendability` is a preserved independent workspace, excluded from
root CI. Do not introduce active dependencies on it. Use `make demo-check` when
editing the demos. Preserve the production workflow's legacy service rollback path.

## Documentation and evidence

Start at `docs/README.md` and the relevant product index. Contracts own requirements,
architecture describes source, deployment owns operating targets, status owns dated
observations and unresolved conflicts, and remaining work owns acceptance gates.
Never infer a live deployment from code or a workflow default.

Evidence belongs under `<product>/evidence/`. Retain acceptance results, significant
failures, unresolved incidents and their supporting raw inputs/provenance. Delete
superseded diagnostics only when no retained claim or executable fixture needs them;
record the deletion and historical source revision. Keep retained raw bytes immutable.
Update documentation links instead of creating forwarding stubs or archive copies.
See `evidence/README.md` for metadata and historical-path rules.
