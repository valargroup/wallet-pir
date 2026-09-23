# Wallet PIR

This repository implements two private retrieval products for Zcash wallets:

- **[Enhance PIR](enhance/docs/README.md)** retrieves the encrypted Ironwood
  output data needed to complete a note from its output position.
- **[Transparent PIR](transparent/docs/README.md)** combines public activity
  filters with private script-history recovery at a wallet-accepted chain anchor.

Each product index links its protocol, architecture, deployment, dated status,
and outstanding acceptance gates. Implementation is not proof of live rollout.

## Enhance performance

On September 23, 2026, a five-minute production test through the public HTTPS
origin completed **9,809 correct encrypted queries with no errors**, sustaining
**32.68 requests/second** with eight closed-loop clients. The load driver ran on
the coordinator host, so the figures include the public HTTPS route but not
geographically remote network latency.

| End-to-end latency | Time |
|---|---:|
| p50 | 242.6 ms |
| p95 | 263.2 ms |
| p99 | 348.9 ms |

The published schema-11 database held **585,542 records** of 653 bytes each
(364.6 MiB of raw records) in one 32,768-row logical shard. The deployed v6/q48
coordinator admitted four active queries, with up to 16 waiting for two seconds;
two worker replicas served the shard. All measured and warmup answers matched
the canonical journal oracle. The test ran against the database size and
configuration recorded at its start, not a fixed-size synthetic fixture.

See the [dated measurement record](enhance/evidence/query-admission-production-2026-09-23/README.md)
for commands, raw reports and limitations. The
[older reported result](enhance/evidence/reported-performance/README.md)
remains available for historical context. The
[APM dashboard](https://enhance-pir.valargroup.dev/apm/) shows live fleet performance.

## Repository layout

| Directory | Purpose |
|---|---|
| `enhance/` | Enhance crates, services, tools, operations, documentation and evidence |
| `transparent/` | Transparent crates, services, tools, operations, documentation and evidence |
| `ops/` | Shared production infrastructure, coordinator configuration and deployment contracts |
| `docs/` | Repository-wide indexes, migration records and personal working notes |
| `evidence/` | Shared evidence policy, checksums and historical-path mappings |
| `tools/` | Repository-wide documentation checks and their tests |
| `demos/legacy-spendability/` | Preserved independent nullifier/witness workspace, excluded from root CI |

See the [tooling index](tools/README.md) for development commands and the
[evidence index](evidence/README.md) for measurement provenance and retention.

## Develop

```sh
make check
cargo run --release -p enhance-pir-server --bin enhance-pir-server -- --help
cargo run --release -p enhance-pir --features cli --bin enhance-pir-cli -- --help
make transparent-sim-help
```

Rust package and binary names are stable across the repository reorganization.
Full-shard cryptographic tests use release mode. The legacy demos remain
independently buildable with `make demo-check`.

## Operations

Use the [Enhance runbook](enhance/docs/deployment.md) or
[transparent runbook](transparent/docs/deployment.md) for deployment.
The [APM dashboard](https://enhance-pir.valargroup.dev/apm/) exposes observed
Enhance fleet health and performance. Dated evidence describes only its recorded
revision, workload and coverage; it does not establish present capacity.
