# Repository automation

Product Cargo packages live under `src/<product>/`. Enhance development tools
(`enhance-pir-client`, `enhance-pir-exercise`, `enhance-pir-load-test` and the
standalone `enhance-noise-analysis` workspace) are Cargo packages under
`tools/enhance/`; `pir-monitor` lives under `tools/shared/pir-monitor/`.
The root tools directory contains CI, development and fleet automation. Product
scripts, deployment resources and tests live under `tools/<product>/`. Product
tools keep their existing Cargo package names and CLI flags.

| Location | Purpose |
|---|---|
| `ci/`, `dev/` | Repository checks and local development helpers |
| `scripts/` | Shared operator commands, including `wallet-pir-deploy.py` |
| `lib/wallet_pir_ops/` | Shared deployment, locking, release and recovery implementation |
| `deploy/`, `infra/` | Shared deployment resources and Terraform roots |
| `tests/operations/` | Shared operations and deployment tests |

The [fleet operator guide](../docs/operations.md) describes deployment and
recovery. Operations bundles include only explicitly allowed runtime resources;
the rest of this directory is not installed on the fleet. Existing installed
release directories and their recorded paths are immutable.

| Location | Purpose | Entry point |
|---|---|---|
| `enhance/loadtest/` | Enhance encrypted-query load generation | `make load-test` |
| `enhance/client/` | Enhance wallet-protocol CLI (`enhance-pir-cli`) | `cargo run -p enhance-pir-client --bin enhance-pir-cli -- --help` |
| `enhance/exercise/` | Local Enhance coordinator/worker exercise | `cargo run -p enhance-pir-exercise -- --help` |
| [noise-analysis](enhance/noise-analysis/README.md) | Standalone Enhance noise analysis | `cargo build --locked --release --manifest-path tools/enhance/noise-analysis/Cargo.toml` |
| `shared/pir-monitor/` | Independent public PIR correctness monitor | [observability](../docs/enhance/observability-alerting.md) |
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
