# Repository guidance

The root workspace contains Enhance PIR, Status PIR, transparent shard recovery,
transaction-detail (txid) PIR and the receiver directory. Cargo packages live
directly under `src/<product>/` (`enhance`, `status`, `transparent`, `txid`,
`receiver`); packages used by more than one product live under `src/shared/`.
The publisher and query-server hosts live at `src/pir-publisher` and
`src/pir-query-server`. Each product publisher lives beside that product's
query server, and `src/shared` holds libraries only.
Keep operations and standalone scripts under `tools/<product>/`, documentation
under `docs/<product>/` and evidence under `evidence/<product>/`; shared automation
lives under `tools/{ci,dev,scripts,lib,deploy,infra}/`. A product never depends
on another product's application package, and serving packages never depend on
chain reading or ingestion packages (`tools/ci/boundary.py`).
`tools/transparent/src/measure` uses the current shard stack and remains part of the
backfill workflow; its extrapolated baseline is not a whole-wallet measurement.

Follow [AGENTS.md](AGENTS.md) for shared iteration and validation rules. Use
`make check-fast BASE=<sha>` during iteration; comprehensive checks belong to
final validation or CI. Optimized `release-fast` runs full-shard tests without
deployment LTO. Preserve Cargo package/binary names when moving tools.

Preserve the production workflow's legacy service rollback path.

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
