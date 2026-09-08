# Repository guidance

The root workspace includes Enhance PIR, active transparent shard recovery, and
retained research crates. For Enhance, keep protocol-facing
types and the client in `pir/enhance`; keep ingestion, persistence, HTTP, and
worker orchestration in `server/enhance-pir-server`.

Run `make check` before submitting changes. Release mode is required for the
full-shard cryptographic tests.

`demos/legacy-spendability` contains inactive nullifier and witness demos. It
is excluded from the root workspace and CI. Do not introduce dependencies from
active crates to it. Check it manually with `make demo-check` only when editing
the archived demos.

`docs/archive` is historical context and does not specify current behavior.
Operational files belong under `ops/`. The production workflow performs a
coordinated rollout and must preserve the one-time legacy service rollback path.

## Transparent PIR documentation authority

Before transparent PIR design, sizing, implementation or deployment work, read
`docs/transparent-pir/README.md` and the relevant linked document. The contract
owns requirements; deployment owns accepted target parameters; status separates
source-verified implementation from observed live state; remaining-work owns the
unchecked gates; evidence owns measurement provenance. Report conflicts rather
than choosing an old recommendation from a search result.

`pir/transparent-history`, `server/transparent-history-server`,
`server/transparent-measure` and older Python retrieval/reuse/navigation tools
are retained research, not the active transparent-shard protocol. Do not derive
fleet sizing from their demos, fixtures or partial-journal measurements.
Superseded transparent prose is deleted; update inbound links rather than making
archive copies or forwarding stubs. Version control retains design history.
Preserve raw measurement provenance and distinguish projections from measurements.
