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
a GPU worker, and this change does not introduce GPU-aware placement or fleet deployment.

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
cargo run --locked --release -p enhance-pir-server --features cuda --example cuda_domain
```

The HTTP test covers encrypted requests through real coordinator/worker listeners,
retention, failover, and restart. The domain benchmark compares CPU and GPU over
wallet-pir's actual unit composition at 32768 rows by 12288 coefficients. It checks
100 queries per repetition, three repetitions, one and two concurrent callers,
and reports preparation separately from evaluation. Sample `nvidia-smi` during
the run to record total device memory, and record CPU limits and thread count.
