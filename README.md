# Wallet PIR

This repository implements private retrieval services for Zcash wallets:

- **[Enhance PIR](docs/enhance/README.md)** retrieves the encrypted Ironwood
  output data needed to complete a note from its output position.
- **[Transparent PIR](docs/transparent/README.md)** combines public activity
  filters with private script-history recovery at a wallet-accepted chain anchor.
- **[Status PIR](docs/status/architecture_status.md)** observes recent transaction
  status privately through its own controller, router and worker roles.
- **[Receiver PIR](docs/receiver/README.md)** lets a restored wallet retrieve payments
  indexed under an Ironwood receiver.

Each product index links its protocol, architecture, deployment, dated status,
and outstanding acceptance gates. Implementation is not proof of live rollout.

## Enhance performance

The current v7 deployment and its qualification limits are recorded in the
[September 24 report](evidence/enhance/immutable-v7-2026-09-24/README.md). The
measurement below is the historical v6 baseline.

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

See the [dated measurement record](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/query-admission-production-2026-09-23/README.md)
for commands, raw reports and limitations. The
[older reported result](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/reported-performance/README.md)
remains available for historical context. The
[APM dashboard](https://enhance-pir.valargroup.dev/apm/) shows live fleet performance.

## Repository layout

| Directory | Purpose |
|---|---|
| `src/<product>/` | Cargo packages for `enhance`, `status`, `transparent`, `txid` and `receiver`, including Rust development tools |
| `src/shared/` | Focused packages shared by more than one product |
| `docs/` | Repository-wide indexes and notes; product documentation under `docs/<product>/` |
| `evidence/` | Evidence policy and checksums; product evidence under `evidence/<product>/` |
| `tools/` | CI, development, infrastructure and deployment automation; product scripts and operations under `tools/<product>/` |

See the [tooling index](tools/README.md) for development commands and the
[evidence index](evidence/README.md) for measurement provenance and retention.

## Develop

```sh
make doctor
make check-fast BASE=origin/main
# Explicit comprehensive local validation: make check-full
cargo run --release -p enhance-publisher --bin enhance-pir-server -- --help
cargo run --release -p enhance-pir-client --bin enhance-pir-cli -- --help
make transparent-sim-help
```

Rust package and binary names are stable across the repository reorganization.
Full-shard cryptographic tests use the optimized `release-fast` profile.
See [development commands](docs/development.md) and [agent guidance](AGENTS.md).

## Operations

Start with the [fleet operator guide](docs/operations.md) for the shared topology,
versioned operations bundle, role configuration and lifecycle interfaces.

Use the [Enhance runbook](docs/enhance/deployment.md) or
[transparent runbook](docs/transparent/deployment.md) for deployment.
The [APM dashboard](https://enhance-pir.valargroup.dev/apm/) exposes observed
Enhance fleet health and performance. Dated evidence describes only its recorded
revision, workload and coverage; it does not establish present capacity.
