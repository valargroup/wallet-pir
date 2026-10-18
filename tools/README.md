# Development tools

Cargo packages, including Rust development tools, live under `src/<product>/`.
The root tools directory contains repository-wide checks; standalone product
scripts live under `tools/<product>/`. Product tools keep their existing Cargo
package names and CLI flags.

| Location | Purpose | Entry point |
|---|---|---|
| `src/enhance/loadtest/` | Enhance encrypted-query load generation | `make load-test` |
| [transparent-loadtest](../src/transparent/transparent-loadtest/README.md) | Wallet simulations, block comparison and interactive reports | `make transparent-sim-help` |
| [transparent-regression](../src/transparent/transparent-regression/fixtures/README.md) | Frozen accepted-anchor correctness fixtures and parent-filter conformance | `make transparent-regression` |
| `src/transparent/transparent-measure/` | Current-shard recovery measurement used by the backfill workflow | `cargo run --release -p transparent-measure -- --help` |
| [filters](transparent/filters/README.md) | Sample collection, filter encoding and format-specific validation | Python CLI help |
| [parent-filters](transparent/parent-filters/README.md) | Active opt-in parent-filter selection and paired HTTP evaluation | Python CLI help |
| `check-doc-links.sh` | Documentation/evidence links and heading anchors | `make check-docs` |

`transparent-measure` compares exact recovery against an independent journal
traversal; its compact baseline is an explicitly labelled extrapolation. It is
not the removed transparent-history research backend.

Record measurements using the [evidence requirements](../evidence/README.md).
Historical research tools remain available at the revision in the
[historical cleanup ledger](https://github.com/valargroup/wallet-pir/blob/3765536ee363af7646cc70d8c11ea4da27818ef5/docs/cleanup-2026-09-14.md).
