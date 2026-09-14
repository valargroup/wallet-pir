# Enhance and transparent PIR

This repository implements two private retrieval products for Zcash wallets:

- **[Enhance PIR](docs/enhance-pir/README.md)** retrieves the encrypted Ironwood
  output data needed to complete a note from its output position.
- **[Transparent PIR](docs/transparent-pir/README.md)** combines public activity
  filters with private script-history recovery at a wallet-accepted chain anchor.

Each product index links its protocol, architecture, deployment, dated status,
and outstanding acceptance gates. Implementation is not proof of live rollout.

## Enhance performance

A previously reported five-minute production test with eight workers completed
**6,512 encrypted queries with no errors**, sustaining **21.71 requests/second**.

| End-to-end latency | Time |
|---|---:|
| p50 | 322.6 ms |
| p95 | 483.3 ms |
| p99 | 1.75 s |

With the tested 32,768-row by 4,096-column configuration, each query uploads
258,056 bytes (252.0 KiB) and downloads 10,256 bytes (10.0 KiB), for **262.0 KiB
combined**. At the reported throughput, this corresponds to 5.60 MB/s upload and
0.22 MB/s download, or 46.6 Mbit/s combined, excluding HTTP and TLS overhead.

See the [measurement record](evidence/enhance/reported-performance/README.md)
for provenance and limitations, and the
[APM dashboard](https://enhance-pir.valargroup.dev/apm/) for live fleet performance.

## Repository layout

| Directory | Purpose |
|---|---|
| `pir/` | Protocol libraries, wallet clients and stores; retained Enhance spend compatibility types |
| `server/` | Ingestion, publication, query services, block benchmark server and operational dashboard |
| `tools/` | Load tests, regression and measurement harnesses, filter utilities and repository checks |
| `ops/` | Deployment scripts, infrastructure, service configuration and operations tests |
| `docs/` | Current product documentation and personal working notes |
| `evidence/` | Curated dated measurements, acceptance records and reproducibility inputs |
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

Rust package and binary names are stable across the tooling reorganization.
Full-shard cryptographic tests use release mode. The legacy demos remain
independently buildable with `make demo-check`.

## Operations

Use the [Enhance runbook](docs/enhance-pir/deployment.md) or
[transparent runbook](docs/transparent-pir/deployment.md) for deployment.
The [APM dashboard](https://enhance-pir.valargroup.dev/apm/) exposes observed
Enhance fleet health and performance. Dated evidence describes only its recorded
revision, workload and coverage; it does not establish present capacity.
