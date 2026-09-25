# Native distributed integration on ipir-sp rc.5

The Enhance native path already exists on wallet-pir main and is serving the
controlled production trial. This integration replaces its trial-commit pin
with `v0.1.0-rc.5`, resolving to `674116d0` in Cargo.lock. The tag was published
from [ipir-sp PR #23](https://github.com/valargroup/ipir-sp/pull/23); merge that
upstream PR before merging this integration PR. rc.4 alone lacks the mapped
codecs, native query-mask accessor, and power-of-two CUDA interface.

The native protocol remains `ironwood-enhance-pir-v8-native-poc`, control version
3. Preparation stays on the coordinator, matrix-vector evaluation on workers,
and packing on the router. Live publications, mapped state, GPU preference and
admission settings are preserved. This dependency update does not deploy services
or change the default backend for existing clients.

## Build matching native components

Use Rust 1.91 and build Enhance separately from transparent services to avoid
Cargo feature unification selecting the native path in shared server code.

```sh
# Coordinator, packing router and CPU workers: existing Skylake AVX-512 hosts.
RUSTFLAGS='-C target-cpu=skylake-avx512' cargo build --locked --profile release-fast \
  -p enhance-pir-server --features native-reinspiring,cuda

# GPU worker's host CPU supports AVX2, not AVX-512.
RUSTFLAGS='-C target-cpu=haswell' cargo build --locked --profile release-fast \
  -p enhance-pir-server --features native-reinspiring,cuda

# Matching client / load tool.
cargo build --locked --profile release-fast \
  -p enhance-pir --features cli,native-reinspiring
cargo build --locked --profile release-fast \
  -p enhance-pir-load-test --features native-reinspiring
```

Copy each binary into a distinct versioned output directory before the next
build. Preserve the GPU service's CUDA runtime library path. Do not mix a legacy
client or server build with the native protocol.

Existing native installations keep their `/srv/enhance-pir-native-poc` state.
A v7-to-native cutover requires separate coordinator, worker, router and ingress
roots: copy only a stopped/coherent canonical record journal, rebuild native
control state, and retain the prior state. Never reuse v7 controller manifests
as native state. Follow the production deployment lock and service coordination
in [architecture2-ssh.md](architecture2-ssh.md); this document does not authorize
an automatic rollback.

## Focused validation

```sh
cargo check --locked -p enhance-pir-server -p enhance-pir-load-test \
  --features native-reinspiring,cuda
QUALIFY_PUBLICATIONS=1 QUALIFY_QUERY_LANES=2 QUALIFY_QUERIES_PER_LANE=3 \
  cargo test --locked -p enhance-pir-server --features native-reinspiring \
  --test packing_http
cargo test --locked -p enhance-pir-server --test packing_http
```

CI runs the native distributed HTTP fixture separately from the legacy suite,
including a publication while queries are in flight. CUDA compilation without a
GPU is not CUDA hardware qualification. Prior production measurements are in
`enhance/evidence/reinspiring-production-trial-2026-09-25`; they measured the trial
revision, not the newly tagged binary. Native cryptographic production gates
remain those documented by ipir-sp; the profile is still experimental.
