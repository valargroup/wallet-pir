# Status PIR synthetic backend

This is an isolated backend experiment for the [status architecture](architecture_status.md).
It adds no external wallet integration and enables no status routes in the
production server. The `status-pir` binary is a dedicated entry point.

The [September 25 GPU run](../evidence/status-backend-2026-09-25/README.md)
passed encrypted correctness and a one-minute fixed-source 20-QPS test. The
five-second publication gate failed; see the measured update costs in that report.

## Implemented experiment

- 8,192 rows, 256 fixed 80-byte slots per row, and the 75% entry ceiling.
- Full-txid bucket lookup, canonical record validation, state precedence, and
  complete-block eviction on global or bucket overflow.
- Explicit coverage-incomplete errors on misses without a sufficient local bound.
- Domain-separated q48 queries with fresh randomness and request IDs. The backend
  protocol name is `status-pir-v1-synthetic-q48`, deliberately distinct from a release.
- Independent coordinator/ingress, router, and worker HTTP listeners. They run
  in one process for this experiment and share an in-memory publication controller.
  The status routes are merged with an actual Enhance coordinator using isolated
  state and an unready fixture inventory; no Enhance publication loop runs.
- CUDA or CPU evaluation, CPU packing, bound intermediates, freshness checks,
  bounded HTTP bodies, fixed admission, and retained observation views.
- Candidate preparation reuses identical 2,048-row units. Changed units and
  packing material are rebuilt. This measures a baseline, not row-level deltas.

The source is an explicitly synthetic, deterministic set of complete blocks,
mempool entries, and fork observations. Synthetic txids and block hashes are
generated independently by the oracle. A fixture refresher re-observes the
unchanged source; it is not evidence of live node polling or publication latency
under chain traffic. Every sample response is checked against the oracle.

The 52-byte envelope is `SPQ1`, a 32-byte manifest digest, and a fresh 16-byte
request ID. The digest binds protocol, network, salt, generation, recovery epoch,
coverage, anchor, observation time, entry count, row digest, and public material.
Queries append uploaded packing keys and the q48 selection. Responses echo the
envelope and append the packed ciphertext. No selected row or txid is sent in
plaintext. Worker intermediates use the same envelope followed by little-endian
u64 coefficients on the private loopback API.

## Build and validate

```sh
cargo test --locked -p enhance-pir --lib status::tests
cargo test --locked -p enhance-pir-server --lib status::index::tests
cargo build --locked --profile release-fast -p enhance-pir-server --bin status-pir --features cuda
RAYON_NUM_THREADS=4 LD_LIBRARY_PATH=/usr/local/cuda-12.2/lib64 \
  target/release-fast/status-pir validate --cuda \
  --entries 1572864 --state-dir /tmp/status-pir-validation
```

Omit `--features cuda` and `--cuda` for a CPU run. CUDA selection fails explicitly
without a working GPU/runtime. Build with the intended CPU flags; the reference
P4000 host supports Haswell, not the newer coordinator's AVX-512 target.

Validation runs encrypted HTTP observations, known/unknown coverage, response
and request binding rejection, fresh randomness, invalid txid length, absence
of a transaction-payload route, a mempool-to-mined update, and stale-source
rejection. It compares both initial and updated composed hints with upstream
full-database preprocessing. It reports preparation stages and reused units.
It also tests worker listener loss and recovery-epoch rejection of old views.

The unit tests deliberately construct a 257-entry bucket to exercise complete
block eviction and refusal when mandatory mempool contents alone overflow.

## Isolated deployment and probes

Install the checksummed binary as `/opt/status-pir/status-pir`, create
`/home/paperspace/status-pir-state`, and install the
[Status service](../ops/deploy/status-pir.service). The service listens only on
loopback: coordinator/ingress 8380, worker 8381, router 8382. It has a 16 GiB
process ceiling and no swap allowance. This is not independent per-role resource
qualification or a production deployment template.

The APM update adds a fourth loopback listener at 8383. Its
`/internal/status-apm` endpoint reports bounded aggregate request counters,
histograms, preparation timings, snapshot metadata, and host/GPU resources;
`/internal/metrics` exposes aggregate Prometheus metrics. A dedicated SSH
forward connects it to the PIR APM sidecar at loopback port 8384. The public
dashboard separates Enhance and Status panes and labels the Status source as
synthetic. Fixture reaffirmation changes observation age but does not count as
a new generation publication.

```sh
ssh -N -L 8380:127.0.0.1:8380 status-pir-p4000-ams1
```

In another terminal, run a backend-only probe:

```sh
target/release-fast/status-pir probe --origin http://127.0.0.1:8380 \
  --entries 1572864 --queries 40 --concurrency 1
```

An offered-load test on the host uses:

```sh
target/release-fast/status-pir probe --entries 1572864 \
  --queries 1200 --concurrency 4 --qps 20
```

Each measured lookup includes initialization, public-material download, client
setup, encrypted query, and validation. Failures and arrivals skipped due to
client concurrency limits are reported and cause a nonzero exit. This is a
conservative cold-client workload, not cached-session throughput. A fixed-source
load run does not qualify concurrent block publication.

## Boundaries before rollout

The backend does not implement live canonical/mempool ingestion, durable status
journaling/recovery, distributed control acknowledgments, process-separated
roles, row-level hint/GPU updates, or the six-hour concurrent-publication gate.
Restart reconstructs the synthetic fixture. Controller state is in memory and
must not be used for production recovery authority. No claim of malicious-server
data authentication or full-history absence is made.

External wallet work requires a separate user confirmation. No Vizor, Dart,
Swift, C ABI, or external wallet-library repository changes are part of this backend.
