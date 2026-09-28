# Status PIR backend

For the current private live rollout, see [distributed Status qualification](status_distributed.md).
The sections below retain the isolated fixture baseline. Public Status remains disabled.

This is an isolated backend experiment for the [status architecture](architecture_status.md).
It adds no external wallet integration and enables no status routes in the
production server. The `status-pir` binary is a dedicated entry point.

The [September 25 GPU run](../evidence/status-backend-2026-09-25/README.md)
is historical v1 evidence. Its timings cannot qualify compact v2. Current
[compact v2 measurements](../evidence/status-compact-v2-2026-09-25/README.md)
cover CPU/CUDA interoperability and private live publication, with public
release blockers recorded explicitly.

## Implemented experiment

- 8,192 rows, 256 fixed 40-byte slots per row, and the 75% entry ceiling.
- Full-txid bucket lookup, canonical record validation, state precedence, and
  complete-block eviction on global or bucket overflow.
- Explicit coverage-incomplete errors on misses without a sufficient local bound.
- Domain-separated q48 queries with fresh randomness and request IDs. The backend
  protocol name is now `status-pir-v2-q48`, matching the separately pinned wallet contract.
  Historical fixture captures used `status-pir-v1-synthetic-q48`.
- Independent coordinator/ingress, router, and worker HTTP listeners. They run
  in one process for this experiment and share an in-memory publication controller.
  The status routes are merged with an actual Enhance coordinator using isolated
  state and an unready fixture inventory; no Enhance publication loop runs.
- CUDA or CPU evaluation, CPU packing, bound intermediates, freshness checks,
  bounded HTTP bodies, fixed admission, and retained observation views.
- Candidate preparation reuses identical 2,048-row units and unchanged packing
  blocks. Sparse changed coefficients use differential hints; dense changes use
  full reconstruction. See [distributed publication](status_distributed.md).
- Rapid synthetic activations retain only the immediately previous material
  generation. Older session IDs receive a conflict and require a fresh init and
  query; already admitted work remains pinned to its original generation.

The source is an explicitly synthetic, deterministic set of complete blocks,
mempool entries, and fork observations. Synthetic txids and block hashes are
generated independently by the oracle. A fixture refresher re-observes the
unchanged source; it is not evidence of live node polling or publication latency
under chain traffic. Every sample response is checked against the oracle.

The 52-byte envelope is `SPQ2`, a 32-byte manifest digest, and a fresh 16-byte
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
P4000 host supported Haswell, not the newer coordinator's AVX-512 target; the
CPU Status host uses `x86-64-v3`.

Validation runs encrypted HTTP observations, known/unknown coverage, response
and request binding rejection, fresh randomness, invalid txid length, absence
of a transaction-payload route, a mempool-to-mined update, and stale-source
rejection. It compares both initial and updated composed hints with upstream
full-database preprocessing. It reports preparation stages and reused units.
It also tests worker listener loss and recovery-epoch rejection of old views.

The unit tests deliberately construct a 257-entry bucket to exercise complete
block eviction and refusal when mandatory mempool contents alone overflow.

### Live-source observation and isolated serving

`status-pir observe-live` reads a bounded canonical block interval and one
complete mempool snapshot from the configured Zakura RPC. It checks every raw
block against the canonical hash at its height, checks that the tip is unchanged
at the end, and builds a Status index covering all transaction types. It prints
aggregate collection/index facts without txids or transaction contents. With
`--samples N`, it caches the canonical window across observations, reports
changed-row counts, and retains observed disconnected blocks as fork records.
It does not publish or serve. Pass `--state-dir` to persist a compact block and
fork cache with fsync and atomic rename. A restart must still observe the
canonical source and current mempool before it can build a candidate.

```sh
target/release-fast/status-pir observe-live \
  --rpc-url http://127.0.0.1:8232 --cookie /path/to/zakura/.cookie \
  --salt-hex <64-hex-character-public-index-salt> --window-blocks 64 \
  --samples 60 --interval-ms 1000
```

`status-pir serve-live` is an isolated loopback integration path. It opens the
source checkpoint and durable publication counter, starts a new recovery epoch,
builds from a live observation, reobserves the source after preparation, and
publishes only if its anchor remains canonical and its original observation is
still fresh. If the source changes during preparation, the candidate keeps its
original timestamp; a newer source observation gets another candidate. Later
unchanged observations renew the manifest using the committed counter. Failed
polls and disconnected candidates do not renew freshness. It uses the synthetic protocol identifier and all HTTP roles still
share one process, so it is not the production service.

```sh
target/release-fast/status-pir serve-live \
  --rpc-url http://127.0.0.1:8232 --cookie /path/to/zakura/.cookie \
  --salt-hex <64-hex-character-public-index-salt> --window-blocks 64 \
  --state-dir /tmp/status-pir-live
```

On the coordinator host, `probe-live` selects a txid from a canonical block,
checks the manifest anchor independently through RPC, and verifies the encrypted
answer without logging the txid or query body. It can target an isolated
loopback `serve-live` listener with `--origin` and needs the same local RPC
cookie path. The [SSH trial](../evidence/status-live-trial-2026-09-25/README.md)
records the first deployed synthetic load and live encrypted checks.

The default 64-block window is for source inspection and the diagnostic is
bounded to 4,096 blocks. It does not establish
the production retention window or publication cadence.

## Isolated deployment and probes

The synthetic single-process `status-pir serve` deployment and its APM tunnel
ran on the Paperspace P4000 and were retired with that host on 2026-09-27; see
[CPU Status host](../evidence/status-cpu-host-2026-09-27/README.md). To repeat
the fixture experiment, run `status-pir serve --entries 1572864 --state-dir <dir>`
on an isolated host. It listens only on loopback: coordinator/ingress 8380,
worker 8381, router 8382, and APM 8383. This is not a production template.

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

The probe can schedule a six-hour synthetic run with `--seconds 21600 --qps 20
--concurrency 16 --report-jsonl /tmp/status-load.jsonl --max-p99-ms 1000`.
It streams one outcome per scheduled arrival and bounds the number of retained
tasks. This remains a fixture test: it does not qualify live publication.

Each measured lookup includes initialization, public-material download, client
setup, encrypted query, and validation. Failures and arrivals skipped due to
client concurrency limits are reported and cause a nonzero exit. This is a
conservative cold-client workload, not cached-session throughput. A fixed-source
load run does not qualify concurrent block publication.

## Boundaries before rollout

The isolated live path now has canonical ingestion, a durable observation cache,
and a durable generation/recovery counter. It still needs integration with the
deployed Enhance coordinator and HTTPS origin, authenticated publication and
acknowledgments to independent router/worker processes, full-size concurrent
preparation qualification, a release protocol with reviewed client compatibility,
and the six-hour concurrent-publication gate. Its material controller is still
in memory and rebuilds only after a fresh observation on restart. It makes no
claim of malicious-server data authentication or full-history absence.

## Compact v2 qualification boundary

The `status-pir-v2-q48` contract uses 40-byte slots, 12,288-byte padded rows,
6,144 u16 columns and a 96 MiB database. Setup, bucket and manifest domains
use `status-pir/v2/`; request envelopes use `SPQ2`. The HTTP route prefix
remains `/v1/status/`, but v1 manifests and material are incompatible.
The 1,572,864-entry admission ceiling and 20-second freshness limit are unchanged.
Block hashes remain internal source/publication metadata, not wallet-visible
inclusion evidence. All retained v1 timing/resource captures are historical and
protocol-incompatible; they cannot qualify v2.
