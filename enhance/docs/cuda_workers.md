# Optional CUDA matrix-vector evaluation

CUDA is an opt-in build and a per-worker runtime choice. The default build and
normal release bundles remain CPU-only. Build a CUDA-capable worker with:

```sh
cargo build --locked --release -p enhance-pir-server --features cuda
./target/release/enhance-pir-server worker \
  --data-dir /srv/enhance-pir/worker --listen 127.0.0.1:8091 \
  --matvec-backend cuda --cuda-device 0
```

The same configuration can be supplied through `ENHANCE_MATVEC_BACKEND=cuda`
and `ENHANCE_CUDA_DEVICE=0`; explicit CLI arguments take precedence. Existing
systemd units using an environment file can read these variables without changes
to `ExecStart`. Custom builds are unqualified candidates; do not replace binaries
inside a checksummed release bundle. Follow the normal exact-binary qualification
process before using a custom CUDA build for serving capacity.

CPU is the default even in a CUDA-capable binary. Device zero is selected when
CUDA is requested without an ordinal. An explicit device with CPU is rejected.
Backend options belong to the `worker` subcommand, not the coordinator.
Startup checks the CUDA driver, NVRTC, and selected device before opening worker
state or binding a listener. Missing feature support, missing libraries, or an
invalid device fails explicitly, without falling back to CPU. CUDA 12 driver/
NVRTC libraries must be available to the service user at runtime; builds and
CPU-only CI do not require those libraries.

## Ownership and failure behavior

Each immutable unit owns its persistent GPU database and reusable scratch space.
Retained generations sharing a unit reuse that same upload. Both fresh builds and
verified artifact reloads use the selected backend. Artifact identities, hashes,
protocol/schema versions, and worker placement policy do not encode the backend.
To roll back, restart with CPU selection and remove `ENHANCE_CUDA_DEVICE`; existing
artifacts remain usable. Backend configuration cannot change inside a running worker.

The worker still retains its host database for preprocessing and persistence.
Existing memory admission estimates host memory only. GPU memory is additional:
account for all unique retained/candidate units, their scratch buffers, and CUDA
context/module overhead. Failed device allocation refuses preparation and releases
partial candidate allocations; published generations remain referenced. Backend
failures never trigger artifact rebuilding. Runtime device errors return HTTP 503;
invalid query coefficients remain HTTP 400. Detailed backend errors are logged
server-side without query contents. Health output reports the configured backend
and resolved device ordinal (zero when omitted for CUDA).

Packing and coordinator behavior remain on CPU. Matrix-only speedups must not be
reported as whole-request PIR speedups. CPU capacity qualification does not qualify
a GPU worker. The optional-worker pool policy lets a GPU mirror a CPU worker's
domains without making GPU availability part of the publication quorum.

## Optional pool assignment

The coordinator accepts `--pool-policy /etc/enhance-pir/pool-policy.json` alongside
`--pool-placement`. The policy file is loaded on each reconciliation loop. For a
three-worker pool with two required CPU replicas, this policy mirrors the second
CPU worker onto a GPU while preserving the two-copy publication requirement:

```json
{
  "domain_replication": {},
  "optional_mirrors": {"enhance-pir-gpu-01": "enhance-pir-worker-02"},
  "domain_optional_workers": {}
}
```

`domain_replication` overrides the required CPU count for a domain ID; an entry
such as `"7": 3` requires three non-optional workers for domain 7.
`domain_optional_workers` assigns additional optional workers by domain ID.
Both settings allow later domains to use different counts and GPUs. Mirror rules
follow the source worker's actual placement, including sealed domains, subject
to the GPU's own admission limit. Inventory additions remain append-only.

The packing router probes optional workers every two seconds. It removes a
worker from new query selection after two failed one-second probes, or immediately
after an explicit rejection or connection failure, and restores it after two
successful probes confirming the exact published generation. An ambiguous
in-flight evaluation is never replayed. CPU replicas remain eligible throughout.

## Validation

Ordinary correctness and configuration tests run without a GPU:

```sh
cargo test --locked -p enhance-pir-server --test matvec_config
cargo test --locked -p enhance-pir-server --features cuda --test matvec_config
cargo test --locked -p enhance-pir-server --features cuda --lib \
  backend_load_failure_does_not_rebuild_or_replace_published_units
```

On an isolated NVIDIA host, explicitly invoke hardware tests (absence of CUDA is
an error, not a skipped success):

```sh
cargo test --locked --profile release-fast -p enhance-pir-server --features cuda \
  --lib cuda_encrypted_bootstrap_round_trip_and_cached_restart -- --ignored
cargo test --locked --profile release-fast -p enhance-pir-server --features cuda \
  --test http cuda_distributed_round_trip_retention_failover_and_restart -- --ignored
cargo test --locked --profile release-fast -p enhance-pir-server --features cuda \
  --lib cuda_partial_preparation_failure_preserves_published_and_retries -- --ignored
cargo test --locked --profile release-fast -p enhance-pir-server --features cuda \
  --lib cuda_backend_evaluation_failure_over_http_releases_permit_and_recovers -- --ignored
cargo run --locked --release -p enhance-pir-server --features cuda --example cuda_domain
```

The HTTP test covers encrypted requests through real coordinator/worker listeners,
retention, failover, and restart. The domain benchmark compares CPU and GPU over
wallet-pir's actual unit composition at 32768 rows by 12288 coefficients. It checks
100 queries per repetition, three repetitions, one and two concurrent callers,
and reports preparation separately from evaluation. Sample `nvidia-smi` during
the run to record total device memory, and record CPU limits and thread count.

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

[Failure-regression evidence](../evidence/cuda-p4000-2026-09-24/failure-regressions/README.md) records the CPU and P4000 runs.

## CI artifact and manual GPU deployment

Full CI on main builds `enhance-pir-native-cuda-<full-sha>` separately from the
CPU/native bundles. Its digest-pinned Ubuntu 22.04 build uses Rust 1.91.0,
glibc 2.35 and `x86-64-v3`, matching the older GPU host ABI. The archive includes
native v9 server/client/load binaries, checksums, source identity, strict build
metadata and isolated CUDA validation scripts. Successful CI establishes build
provenance; live CUDA correctness remains a deployment gate.

Use **Deploy Enhance CUDA worker** with `ref` set to a full main-ancestor SHA
whose full CI succeeded. Start with `mode=preflight`, then use `mode=deploy`.
The workflow accepts only the optional GPU worker, takes the shared production
lock, and preserves its state root, arguments, CUDA library path and placement.
Preflight uploads verified validation binaries and runs fresh loopback fixture
processes with a single CUDA replica. It does not restart the production unit.
Deployment swaps the server binary transactionally, verifies native v9/CUDA
health and executable identity, then runs public exact-answer queries at 1 QPS
for 60 seconds after a paced 10-second warmup. Acceptance requires zero errors,
GPU evaluation counter growth, router availability, and no worker restart or OOM.
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
Any activation or post-deploy verification failure rolls back touched targets.
The workflow uploads metadata, smoke results, public query reports and rollback
status even on failure. For an interrupted transaction, resume the existing
transactional CLI's rollback using the same inventory and state directory before
retrying deployment; do not clear the journal or force through configuration drift.
