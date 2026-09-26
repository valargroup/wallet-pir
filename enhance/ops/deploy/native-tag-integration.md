# Native distributed integration on ipir-sp rc.6

The Enhance native path exists on wallet-pir main and served the controlled
production trial with the rc.5 revision. This source moves to `v0.1.0-rc.6`,
the ipir-sp tag containing [PR #24](https://github.com/valargroup/ipir-sp/pull/24)
and [PR #25](https://github.com/valargroup/ipir-sp/pull/25). rc.5 lacks the
two-mask output mode and rounded public-mask publication.

The native wire changes. Clients upload only the `K_g` packing key and decode
under both published masks, rounded to 29 bits:

| Native Enhance, per shard | rc.5 (`v8-native-poc`) | rc.6 (`v9-native-two-mask-m29`) |
| --- | ---: | ---: |
| Packing-key upload | 55,296 B | 27,648 B |
| Public session material | 98,304 B | 89,088 B |
| Response body | 33,792 B | 33,792 B |

The protocol is `ironwood-enhance-pir-v9-native-two-mask-m29`, control version
4, and prepared artifacts live under `prepared-native-packing-v2` with format 3.
v8 and v9 clients, routers and coordinators are mutually incompatible and must not
be mixed. Preparation stays on the coordinator, matrix-vector evaluation on workers,
and packing on the router. This dependency update does not deploy services or
change the default backend for existing clients.

The same feature switches Status PIR to `status-pir-v3-native-two-mask-m29`.
See [architecture_status.md](../../docs/architecture_status.md#privacy-and-trust-boundary).

ipir-sp's correctness certificates for two-mask 29-bit publication cover only
the recorded fixture snapshot. Neither the Enhance snapshot nor the Status
geometry has a certificate, and runtime certificate enforcement is not
implemented. The profile remains experimental.

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
  --test packing_http --test status_wallet
cargo test --locked -p enhance-pir --features native-reinspiring --lib native
cargo test --locked -p enhance-pir-server --test packing_http
```

CI runs the native distributed HTTP fixture separately from the legacy suite,
including a publication while queries are in flight. CUDA compilation without a
GPU is not CUDA hardware qualification. Prior production measurements are in
`enhance/evidence/reinspiring-production-trial-2026-09-25`; they measured the
one-mask trial revision, not the two-mask rc.6 binary. Native cryptographic production gates
remain those documented by ipir-sp; the profile is still experimental.
