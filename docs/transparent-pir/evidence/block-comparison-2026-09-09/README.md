# Amsterdam PIR versus compact-block recovery

The final full-chain comparison passed **80/80 exact recoveries**: 20 concurrent wallets for each method in the mixed suite and 20 for each method in the fresh suite. Every configured server metrics target was observed without scrape gaps. Filter CPU/RSS remain unavailable because that service does not export them.

| Suite | Method | Exact | Upload MB | Download MB | Median seconds | Slowest seconds |
|---|---|---:|---:|---:|---:|---:|
| mixed | pir | 20/20 | 135.80 | 277.68 | 2.01 | 30.49 |
| mixed | blocks | 20/20 | 0.00 | 38445.23 | 8.13 | 235.91 |
| fresh | pir | 20/20 | 205.17 | 1275.92 | 20.36 | 48.26 |
| fresh | blocks | 20/20 | 0.00 | 185101.41 | 799.32 | 809.79 |

MB and GB are decimal. Total HTTP payload traffic is upload plus download; headers and TLS framing are excluded. Total-traffic savings are mixed: 98.92%, fresh: 99.20%. Per-wallet, per-profile, stage bytes, requests/retries, sampled memory, CPU and process writes are retained in the raw reports.

## Reproduction and provenance

See [manifest.json](manifest.json), [command.sh](command.sh), [conditions.json](conditions.json), [hosts.json](hosts.json) and [summary.json](summary.json). The manifest identifies the Linux client source archive plus its explicit SQLite-store override, the frozen client/server binaries, controller revision and dependency lock. Both methods use the same client executable. The immutable dataset contains heights 0–3,473,686, pinned to the accepted anchor and genesis in the manifest. All six encoded representations were checksum-verified before service readiness. Its 3,474 batches occupy 114,896,778,048 bytes; transparent-only gzip is 9,252,019,442 bytes.

The 16 prior-ledger seeds were recovered and verified before timing, then copied identically into both mixed runs. Fresh runs start with empty stores and use independently journal-derived full-history digests. Client caches start cold; server caches are not evicted. Mixed runs PIR first, fresh runs blocks first, one pair per suite. Earlier diagnostic runs may have warmed servers. Public TLS traffic originates on the existing 16-vCPU/32-GiB Amsterdam load generator. The baseline has 4 vCPUs/8 GiB; the shared PIR fleet has four recent 4-vCPU/8-GiB workers and two archive 8-vCPU/64-GiB workers, with heterogeneous binary hashes recorded before/after.

Full HTML and raw results remain at the remote/local directories recorded in the manifest. To open the local report:

```sh
make transparent-sim-open SIM_OUT=/tmp/transparent-sim-compare-amsterdam-private-apm-20260909
```

The repository retains compressed JSON, request/APM streams, scenarios and logs under `raw/`. `SHA256SUMS` binds the evidence files. `make-check.log.gz` records the successful complete repository check; the two coverage-append test logs cover macOS and Linux.

## Corrections and retained diagnostics

The first full run exposed quadratic SQLite coverage maintenance: each appended block batch rewrote all prior checkpoints. Commit `1d95ebf` makes a new checkpoint an indexed append and retains replacement/replay handling for existing checkpoints. Its regression test verifies bounded SQL writes after 100 prior checkpoints. The interrupted run is preserved in `quadratic-store-*.json.gz`; it is not counted as a successful comparison.

The next run passed all 80 recoveries, but a change in the operator Mac public IP interrupted its SSH metrics forwarding. `operator-apm-gap-report.json.gz` preserves those valid recovery results and missing APM. The final repeat uses private Amsterdam-only SSH metrics connections, independent of the Mac. No private keys were copied; metrics remain private. Baseline startup, transport and supported-script deployment smokes are retained separately and are not full-chain performance evidence.

## Interpretation limits

- Synthetic profile weights and fixed known scripts do not model a wallet population, seed derivation or HD gap discovery. Paired scripts are restricted to the common PIR coverage (nonempty, not leading OP_RETURN, at most 40 bytes).
- The baseline serves exported compact per-block protobufs over controlled HTTP, gzip-compressed per batch of up to 1,000 blocks. It measures per-block ingestion, not latency of a deployed lightwalletd gRPC service. Incremental combined-minus-shielded bytes are representation arithmetic, not incremental latency.
- One pair per suite establishes these observed outcomes, not capacity, dependable tail latency or daily-active-user limits. Regional Amsterdam latency is not mobile/WAN latency.
- PIR server counters include publication and other shared traffic. Worker CPU/RSS is observed, but filter CPU/RSS and proxy costs are absent. Hardware differs; do not infer equal-hardware system cost. Missing metrics in diagnostics are unavailable, never zero.
- Preparation, export and supervisor preflight/postflight are excluded from measured wallet traffic. Per-wallet totals include measured retries and range-boundary overfetch.

The benchmark host, dataset and remote results are retained for reproduction.
