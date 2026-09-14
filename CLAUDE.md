# Repository guidance

The root workspace contains Enhance PIR, transparent shard recovery and their
validation tools. Keep protocol/client code in `pir/`, production services in
`server/`, Rust/Python development harnesses in `tools/`, and operations in `ops/`.
`transparent-spend` remains an Enhance journal/type dependency but is not served.
`tools/transparent-measure` uses the current shard stack and remains part of the
backfill workflow; its extrapolated baseline is not a whole-wallet measurement.

Run `make check` before submitting changes. Release mode is required for the
full-shard cryptographic tests. Preserve Cargo package/binary names when moving tools.

`demos/legacy-spendability` is a preserved independent workspace, excluded from
root CI. Do not introduce active dependencies on it. Use `make demo-check` when
editing the demos. Preserve the production workflow's legacy service rollback path.

## Documentation and evidence

Start at `docs/README.md` and the relevant product index. Contracts own requirements,
architecture describes source, deployment owns operating targets, status owns dated
observations and unresolved conflicts, and remaining work owns acceptance gates.
Never infer a live deployment from code or a workflow default.

Evidence belongs under `evidence/<product>/`. Retain acceptance results, significant
failures, unresolved incidents and their supporting raw inputs/provenance. Delete
superseded diagnostics only when no retained claim or executable fixture needs them;
record the deletion and historical source revision. Keep retained raw bytes immutable.
Update documentation links instead of creating forwarding stubs or archive copies.
See `evidence/README.md` for metadata and historical-path rules.
