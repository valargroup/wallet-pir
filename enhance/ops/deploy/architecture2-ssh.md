# Architecture update 2: direct SSH deployment

The September 24 implementation preserves wallet protocol EPQ7. The server
binary includes `http_metrics.rs`; deployment of the `pir-apm` sidecar is separate.

Production topology:

- Caddy on coordinator 10.142.0.3 sends `/v1/enhance/query` to loopback ingress
  8082. Manifest and other existing routes remain on coordinator 8080.
- Ingress control and query metrics: loopback 8083, `/internal/metrics`.
- Packing router 10.142.0.14 listens on private query 8092 and control 8093.
  Its 8-GiB host runs a 7-GiB, zero-swap service with four admitted requests.
- Coordinator artifacts bind only 10.142.0.3:8084. The dedicated nftables rule
  permits only 10.142.0.14. DigitalOcean firewalls restrict router controls to
  the coordinator and worker evaluation to coordinator/router tags.
- Worker pool starts with the existing .15/.16 replicas, replication factor 2.
  Automatic host expansion is not enabled by this cutover.

Build with Rust 1.91 and `RUSTFLAGS='-C target-cpu=skylake-avx512'` on the Linux
build host. Repository default `target-cpu=native` is unsafe for artifacts copied
from the newer build CPU to these hosts. Record the source manifest, toolchain,
flags and SHA-256 of every deployed binary. Do not overwrite an executing binary;
install to a versioned release directory and update systemd's ExecStart.

Before cutover, qualify the standalone router on its actual host with isolated
fixture state, publication overlap, exact-answer fresh wallet queries and failure
checks. Never reuse the fixture's persistent router fence directory for production.
The evidence directory records the exact measured assignment; an object limit is
not itself evidence that all assignments at that limit were measured.

For cutover, hold `/run/lock/wallet-pir-production.lock` on the coordinator. Save
the latest Caddy config and units, temporarily reject queries, stop the coordinator,
and back up its control directory while stopped. Stage and restart workers with
the checked binary, preserving their data. Start the production router and ingress,
then start the coordinator with `--packing-router-config`, `--artifact-listen`,
`--query-ingress`, `--pool-placement --frontier-replicas 2`. Wait for router and
ingress readiness, two current worker replicas, and coordinator
`resident_packing_objects: 0`, then switch Caddy's query target to 8082. Verify
answers against independently extracted canonical records, both worker counters,
new publication progress, HTTP metrics and service/cgroup evidence.

Rollback preserves the current recovery and publication history. First stop public
query admission and the coordinator; stop ingress and packing router processes so
no prior serving authority remains. If pooled placement is enabled, run the new
binary's `restore-legacy-placement --control-dir <canonical>/control` offline. It
refuses pending operations/decisions or placements that cannot be represented by
the original complete pairs. Resolve those conditions before rollback. Remove the
architecture2 coordinator override, restore the prior binary and Caddy query target
8080, and restart/verify. The new workers accept legacy evaluation requests.
Do not restore an old control backup after new publications or recovery decisions.

The coordinator `/metrics` supplies init HTTP metrics; ingress
`/internal/metrics` supplies query HTTP metrics. Preserve the APM and transparent
filter Caddy routes when editing the query target.
