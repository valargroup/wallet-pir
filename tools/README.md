# Development tools

Production code and product tools live under `enhance/`, `transparent/` and
`receiver/`. The root tools directory contains repository-wide checks. Product
tools keep their existing Cargo package names and CLI flags.

| Location | Purpose | Entry point |
|---|---|---|
| `enhance/tools/loadtest/` | Enhance encrypted-query load generation | `make load-test` |
| [transparent-loadtest](../transparent/tools/transparent-loadtest/README.md) | Wallet simulations, block comparison and interactive reports | `make transparent-sim-help` |
| [transparent-regression](../transparent/tools/transparent-regression/fixtures/README.md) | Frozen accepted-anchor correctness fixtures and parent-filter conformance | `make transparent-regression` |
| `transparent/tools/transparent-measure/` | Current-shard recovery measurement used by the backfill workflow | `cargo run --release -p transparent-measure -- --help` |
| [filters](../transparent/tools/filters/README.md) | Sample collection, filter encoding and format-specific validation | Python CLI help |
| [parent-filters](../transparent/tools/parent-filters/README.md) | Active opt-in parent-filter selection and paired HTTP evaluation | Python CLI help |
| `check-doc-links.sh` | Documentation/evidence links and heading anchors | `make check-docs` |

`transparent-measure` compares exact recovery against an independent journal
traversal; its compact baseline is an explicitly labelled extrapolation. It is
not the removed transparent-history research backend.

Record measurements using the [evidence requirements](../evidence/README.md).
Historical research tools remain available at the revision in the
[cleanup ledger](../docs/cleanup-2026-09-14.md).
