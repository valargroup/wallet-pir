# Status PIR backend

For the current private live rollout, see [distributed Status qualification](status_distributed.md).
The sections below retain the isolated fixture baseline. Public Status remains disabled.

This is an isolated backend experiment for the [status architecture](architecture_status.md).
It adds no external wallet integration and enables no status routes in the
production server. The `status-pir` binary is a dedicated entry point.

The [September 25 GPU run](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/status-backend-2026-09-25/README.md)
is historical v1 evidence. Its timings cannot qualify compact v2. Current
[compact v2 measurements](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/status-compact-v2-2026-09-25/README.md)
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
cargo test --locked -p status-query-server --lib protocol::tests
cargo test --locked -p status-query-server --lib index::tests
cargo test --locked --profile release-fast -p pir-query-server \
  --features status,native-reinspiring --lib status::
cargo build --locked --profile release-fast -p pir-publisher --features status,native-reinspiring
cargo build --locked --profile release-fast -p pir-query-server --features status,cuda
RAYON_NUM_THREADS=4 LD_LIBRARY_PATH=/usr/local/cuda-12.2/lib64 \
  target/release-fast/pir-publisher status validate-distributed --cuda \
  --entries 1572864 --state-dir /tmp/status-pir-validation
```

Omit `--features cuda` and `--cuda` for a CPU run. CUDA selection fails explicitly
without a working GPU/runtime. Build with the intended CPU flags; the reference
P4000 host supported Haswell, not the newer coordinator's AVX-512 target; the
CPU Status host uses `x86-64-v3`.

`validate-distributed` starts `pir-query-server` worker and router processes
beside a fixture publisher and checks encrypted answers and remote fencing. The
`pir-query-server` Status tests cover the wallet boundary over real HTTP:
an independent wallet decrypts every fixture case, and corrupt answers,
malformed envelopes and stale manifests are refused. The `status-query-server`
tests compare initial and updated composed hints with upstream full-database
preprocessing and check recovery-epoch rejection of old views.

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

On the coordinator host, `probe-live` selects a txid from a canonical block,
checks the manifest anchor independently through RPC, and verifies the encrypted
answer without logging the txid or query body. Its `--origin` is the router's
wallet origin, and it needs the same local RPC cookie path. The [SSH trial](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/status-live-trial-2026-09-25/README.md)
records the first deployed synthetic load and live encrypted checks.

The 64-block `--window-blocks` default suits source inspection. Every source
path, including the live controller, is capped at 4,096 blocks
(`MAX_INSPECTION_BLOCKS`). The live controller has run at that cap since
2026-09-27. Its limits and trade-offs are in
[Observation window](architecture_status.md#observation-window). This command
does not establish the production publication cadence.

## Isolated deployment

The synthetic single-process `status-pir serve` deployment and its APM tunnel
ran on the Paperspace P4000 and were retired with that host on 2026-09-27; see
[CPU Status host](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/status-cpu-host-2026-09-27/README.md).
The single-process `serve`, `serve-live`, `validate` and `probe` modes are
removed: wallet routes are served only by `pir-query-server` roles, so fixture
experiments use `validate-distributed`.

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
