# Optional CUDA matrix-vector evaluation

CUDA is an opt-in build and a per-worker runtime choice. The default build and
normal release bundles remain CPU-only. The CUDA worker is `pir-query-server`,
not the publisher. GPU hosts do not run the publisher as the worker.
Build it with:

```sh
cargo build --locked --release -p pir-query-server --bin pir-query-server \
  --features enhance,native-reinspiring,cuda
```

One process, one Enhance role. The worker config names the listener, data
directory, matrix backend and the HTTPS synchronization of its shards:

```json
{
  "management_listen": "127.0.0.1:18080",
  "enhance": {
    "enabled": true,
    "settings": {
      "role": "worker",
      "listen": "127.0.0.1:8091",
      "data_dir": "/srv/enhance-pir/worker",
      "matvec": {"matvec_backend": "cuda", "cuda_device": 0},
      "synchronization": {
        "publisher_origin": "https://enhance-pir.valargroup.dev",
        "trusted_ca": null,
        "download_budget_bytes": 8589934592,
        "cache_limit_bytes": 8589934592,
        "shards": [0]
      }
    }
  }
}
```

```sh
./target/release/pir-query-server --config /etc/enhance-pir/worker.json --check
./target/release/pir-query-server --config /etc/enhance-pir/worker.json
./target/release/pir-query-server check-backend --matvec-backend cuda --cuda-device 0
```

`check-backend` accepts `--matvec-backend cpu|cuda` and `--cuda-device N`. The
same selection can be supplied to that command through `ENHANCE_MATVEC_BACKEND`
and `ENHANCE_CUDA_DEVICE`; explicit flags take precedence. The running worker
reads the backend from its config file, not from those variables. Custom builds
are unqualified candidates; do not replace binaries inside a checksummed release
bundle. Follow the normal exact-binary qualification process before using a
custom CUDA build for serving capacity.

CPU is the default when the worker config omits `matvec`, even in a
CUDA-capable binary. Device zero is selected when CUDA is requested without an
ordinal. An explicit device with CPU is rejected. Backend options belong to the
worker role config, not the publisher. `pir-publisher enhance build-info` has no
CUDA field. Startup checks the CUDA driver, NVRTC, and selected device before
opening worker state or binding a listener. Missing feature support, missing
libraries, or an invalid device fails explicitly, without falling back to CPU.
CUDA 12 driver/NVRTC libraries must be available to the service user at runtime;
builds and CPU-only CI do not require those libraries.

## Ownership and failure behavior

Each immutable unit owns its persistent GPU database and reusable scratch space.
Retained generations sharing a unit reuse that same upload. Both fresh builds and
verified artifact reloads use the selected backend. Artifact identities, hashes,
protocol/schema versions, and worker placement policy do not encode the backend.
To roll back, restart the worker with `matvec_backend` set to `cpu` and no
`cuda_device` in its config. Existing artifacts remain usable. Backend
configuration cannot change inside a running worker.

The worker still retains its host database for preprocessing and persistence.
Existing memory admission estimates host memory only. GPU memory is additional:
account for all unique retained/candidate units, their scratch buffers, and CUDA
context/module overhead. Failed device allocation refuses preparation and releases
partial candidate allocations; published generations remain referenced. Backend
failures never trigger artifact rebuilding. Runtime device errors return HTTP 503;
invalid query coefficients remain HTTP 400. Detailed backend errors are logged
server-side without query contents. Health output reports the configured backend
and resolved device ordinal (zero when omitted for CUDA).

Packing and publisher preparation remain on CPU. Matrix-only speedups must not be
reported as whole-request PIR speedups. CPU capacity qualification does not qualify
a GPU worker.

## Deployment

CUDA artifacts are produced only when CUDA qualification is selected. Before
deploying a revision to the GPU worker, explicitly qualify it if its ordinary
qualification omitted CUDA:

```sh
gh workflow run ci-qualify.yml --ref main -f family=enhance -F include-cuda=true
```

Use the exact revision of that successful run; a CPU-only qualification does not
produce or qualify a CUDA artifact. The GPU worker runs the CUDA
`pir-query-server`; the host does not run the publisher as the worker. The
deploy inventory lists it with `"artifact_kind": "enhance-pir-native-cuda"`, and
`wallet-pir-deploy.py deploy enhance` takes the full-CI CUDA bundle through
`--extra-archive`; see [deployment](deployment.md#deploy-cli).

## Shard assignment

A GPU worker synchronizes the shards its `synchronization.shards` names from
the publisher, like a CPU worker. Packing routers name the workers that serve
each domain in their own `synchronization.workers`, optionally marking
`preferred` workers (which must also be assigned): an idle preferred worker
takes the next query, otherwise the least-loaded ordinary worker does. A GPU
worker is added there rather than through publisher placement.

## Validation

The optional `pir-cuda` library owns CUDA dependency activation and kernel
construction. Enhance and Status retain runtime selection and lifecycle policy.
Default builds of that library have no CUDA dependencies. Product `cuda` features
forward to it; ordinary CPU lint and correctness never enable them.

PR and main CI select CUDA only for backend code, evaluator preparation/evaluation
contracts, CUDA tests and build logic, or changes to the backend's locked
dependency graph. Renames and deletions count. Unrelated products, documentation,
workspace membership and unrelated lockfile entries do not select CUDA. An
unavailable comparison requires CUDA checks rather than silently dropping them.
Main compares against successful CI, so a later unrelated push cannot discard
CUDA changes from a canceled run. CPU-only cache preparation does not advance
that comparison.
The selector prints its reasons before any CUDA job starts. Hosted CUDA checks
use dynamic loading and install no NVIDIA driver or toolkit.

Full and nightly CPU runs do not implicitly include CUDA. To request driver-free
CUDA checks at a branch revision, dispatch **CI** with `include-cuda=true`; use
the qualification command above for a main-revision artifact. Hardware tests
remain an explicit deployment gate. CPU correctness and driver-free CUDA
configuration tests are separate commands:

```sh
cargo test --locked -p pir-query-server --features enhance --test operator
cargo test --locked -p pir-query-server --features enhance,cuda --test operator
cargo test --locked -p enhance-query-server --features cuda --lib \
  backend_load_failure_does_not_rebuild_or_replace_published_units
```

On an isolated NVIDIA host, explicitly invoke hardware tests (absence of CUDA is
an error, not a skipped success):

```sh
cargo test --locked --profile release-fast -p enhance-query-server --features cuda \
  --lib cuda_encrypted_bootstrap_round_trip_and_cached_restart -- --ignored
cargo test --locked --profile release-fast -p pir-e2e-tests --features enhance-tests,cuda \
  --test http cuda_independent_consumers_answer_updates_and_close_on_withdrawal -- --ignored
cargo test --locked --profile release-fast -p enhance-query-server --features cuda \
  --lib cuda_partial_preparation_failure_preserves_published_and_retries -- --ignored
cargo test --locked --profile release-fast -p enhance-query-server --features cuda \
  --lib cuda_backend_evaluation_failure_over_http_releases_permit_and_recovers -- --ignored
```

The HTTP test covers encrypted requests through independently synchronizing
workers, a packing router and ingress, then an update and withdrawal. It does
not establish GPU retention, failover, restart or capacity qualification.
Sample `nvidia-smi` during the run to record
total device memory, and record CPU limits and thread count.

Failure regressions also run with CPU kernels in ordinary library tests. The GPU
variants wrap real CUDA kernels and inject preparation failure after the second
candidate unit has allocated/uploaded its database. Both fresh construction and
cached reload must release candidate kernels, preserve published answers, and
retry successfully. This tests failure handling after allocation; it does not
force physical device exhaustion or qualify GPU admission limits.

The HTTP failure regression injects an error inside kernel evaluation, verifies
HTTP 503 with no partial answer or private error text, then retries through the
same single-permit worker. Wrong-length and unreduced queries must return HTTP
400 without invoking the backend. Injection support exists only in test builds.

[Failure-regression evidence](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/enhance/cuda-p4000-2026-09-24/failure-regressions/README.md) records the CPU and P4000 runs.

## CI artifact and manual GPU deployment

CUDA-selected full qualification on main builds `enhance-pir-native-cuda-<full-sha>` separately from the
CPU/native bundles. Its digest-pinned Ubuntu 22.04 build uses Rust 1.97.1,
glibc 2.35 and `x86-64-v3`, matching the older GPU host ABI. The archive's
server binary is the CUDA `pir-query-server`, together with the native client
and load-test binaries, checksums, source identity, strict build metadata and
isolated CUDA validation scripts. Successful CI establishes build
provenance; live CUDA correctness remains a deployment gate.

Use **Deploy Enhance CUDA worker** with `ref` set to a full main-ancestor SHA
whose full CI succeeded. `mode=preflight` runs the hardware checks without activation;
`mode=deploy` runs those same checks and then activates the release. The operator
comes from the workflow dispatch revision, independently of the qualified service
artifact revision; both revisions are retained in the CI receipt.
The workflow accepts only the optional GPU worker, takes the shared production
lock, and preserves its state root, arguments, CUDA library path and placement.
Preflight uploads verified validation binaries and runs `cuda-smoke.py`: two
workers from the CUDA `pir-query-server` and the coordinator from the native
`pir-publisher` with an Enhance-only fixture configuration, on device 0, to satisfy the reservation quorum. GPU hosts
do not run the publisher as the worker. Both replicas must publish native CUDA
health; their summed evaluation counter must increase during exact fixture
queries. Each uses a separate loopback port and temporary state root. Preflight
does not restart the production unit.
Deployment swaps the CUDA `pir-query-server` transactionally, verifies native v9/CUDA
health and executable identity, then runs public exact-answer queries at 1 QPS
for 60 seconds after a paced 10-second warmup. Acceptance requires zero errors,
GPU evaluation counter growth, router availability, and no worker restart or OOM.
Before measurement the verifier gives the restarted worker one 60-second budget
to report a ready active publication on CUDA device 0 beside a ready packing
router; after measurement both must still be ready.
Readiness loss after measurement and any process, restart or OOM drift fail
immediately.
Repeat deploys rerun validation without restarting an unchanged unit. No deliberate
production failover or throughput qualification is part of this workflow.

The root deployment runner on the coordinator needs a root-owned operator
inventory at `/etc/enhance-pir/cuda-deploy-inventory.json`; the accompanying
`cuda-deploy-inventory.example.json` documents its shape. Verify both SSH host
keys out of band and pin the exact known-hosts file SHA-256 in that inventory.
Set repository variable `WALLET_PIR_CUDA_SSH_KNOWN_HOSTS` to that public host-key
inventory. The existing `WALLET_PIR_DEPLOY_SSH_KEY` secret supplies the identity;
its public key must authenticate to the coordinator and the sudo-capable GPU
account. Credentials remain runtime material. The independent oracle must use
public canonical records at positions in the current native dataset.

Transaction journals and baseline persist under `/var/lib/wallet-pir-deploy/cuda`.
Under the production lock, the operator creates that directory with mode `0700`,
or tightens an existing root-owned directory without replacing its ledger.
Symlinks, foreign ownership and group- or world-writable directories are refused.
If an earlier reviewed deployment used another ledger, `expected_current_sha256`
may name its verified running GPU hash. Review that deployment's committed receipt
first. Under the lock, adoption requires that exact running hash at its immutable
release path, the same unit files, and byte-identical settings except the executable
path. Arguments, limits, file changes and unfinished transactions still refuse.
The workflow retains the prior baseline bytes and both baseline digests in its
private evidence. It does not replace or reset transaction history. Omit the input
for ordinary deployments; baseline drift then remains a refusal.

A stale pre-repair public oracle may be corrected with `oracle_replacement` and
`oracle_sha256`. The replacement must come from the independent canonical extractor;
review its retained provenance first. Under the production lock the operator keeps
all positions in order and permits only missing fee metadata to be filled (the
existing native v9 fee-repair policy). It retains both exact oracle files and their
digests. Ciphertext, expiry, transparent flags and already-known fees cannot change.
The same zero-error public query gate still runs after deployment.

For an explicit GPU rollback, run this same workflow with `mode=rollback` and the
exact `transaction` from its deployment receipt. `ref` supplies a qualified CUDA
client bundle for verification; the server restored is the transaction's retained
previous executable. Rollback uses the same ephemeral CI identity, production
lock, unit-conflict checks and native CUDA health checks, then repeats public
exact-answer queries and GPU progress checks. It refuses transactions for other
workers and does not accept baseline adoption in rollback mode.

Any activation or post-deploy verification failure rolls back touched targets.
The workflow uploads metadata, smoke results, public query reports and rollback
status even on failure. For an interrupted transaction, resume the existing
transactional CLI's rollback using the same inventory and state directory before
retrying deployment; do not clear the journal or force through configuration drift.
