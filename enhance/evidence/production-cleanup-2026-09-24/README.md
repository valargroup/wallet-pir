# Enhance PIR production cleanup deployment — 2026-09-24

Deployed the original cleanup PR #111 to the existing production Enhance v7 fleet. PR #97 was excluded. All five server processes now run the same verified artifact; existing APM remains unchanged. No durable formats, wire protocol, state directories, runtime flags, or fences were changed.

The machine-readable [manifest](manifest.json) binds the source, artifact checksums, native checks, rollout, and [raw public smoke result](public-exact-smoke.json).

## Provenance and build

- Source: `71be21fe49b96f032db23983b2218c5b59aab0a9` (merged #111).
- Git tree: `ce4fbef60b6488fd87eed2f2d741eb46fb0e69a4`.
- Subsequent main changes through `cb77029` are CI/documentation only; runtime source is equivalent.
- Previous deployed server SHA-256: `cdc8e7f7f6d075e1aecf4eb42930774ad79f580fac628e3c6cf09d30f9e73ae1`. Its dirty deployment source snapshot was based on `b57ce61` plus APM changes; comparison against committed `b3893bc` found only test/formatting differences, so no newer runtime behavior was dropped.
- Build host: `roman-ipir-bench-8vcpu` (`209.38.33.233`), isolated source/target `/root/wallet-pir-production-71be21f`.
- Rust `1.91.0`, locked dependencies, `release-fast`, `RUSTFLAGS='-Dwarnings -C target-cpu=skylake-avx512'`, `CFLAGS=-mpclmul`, `CXXFLAGS=-mpclmul`, eight build jobs. These preserve deployed CPU requirements; fleet support was verified.
- Command: `cargo +1.91.0 build --locked --profile release-fast -p enhance-pir-server -p enhance-pir -p enhance-pir-load-test --bins --features enhance-pir/cli`.
- Immutable post-test staging: `/root/wallet-pir-production-71be21f/release-staging`, containing binaries and `SHA256SUMS`. Cargo integration tests selected different feature unification and temporarily replaced the target-directory server executable; repeating the documented canonical build restored the original checksum before copying into immutable staging.
- Server SHA-256: `ba576af27d7fe56ada942d35366f99191b2a70b2fbf40562573d06f0f36586da`.
- CLI SHA-256: `48bdcbb923ef6fb25e586b0ff9db088d097ad1bc85397ecf5bd27569610241cf`.
- Load-test SHA-256: `aa1d27d8c4a496f6bc886f5af88c5e7f26f31c64ab0c7e178391a9ca8a144543`.

## Validation and activation

Native Linux checks passed before activation: all 107 server library tests (116.82 s), seven shared query-serving tests, and real `packing_http` integration (one test, 32.91 s). CLI help verified. The full local HTTP suite also passed all eight enabled tests; three hardware-scale fixtures remained explicitly ignored.

The [full GitHub CI workflow](https://github.com/valargroup/wallet-pir/actions/runs/36034009549) passed for cleanup head `31cfb14` (the same runtime tree merged by `71be21f`): both product test suites and lints, all nine registered Enhance integration targets, and optional CUDA compilation. The hosted server library suite passed 107 tests; HTTP passed eight with three existing hardware-scale ignores; `packing_http` passed in 48.07 seconds.

The full local `make check` on cleanup head `31cfb14` completed successfully: 703 Rust tests passed, zero failed, five explicitly ignored, plus workspace formatting/clippy, documentation links, operations, and helper checks.

Fast CI initially exhausted its 6-GiB runner limit when independent real-crypto fixtures ran concurrently. Follow-up `cb77029` serializes fast library tests. A [focused native cap check](native-lib-6g-manifest.json) passed all 107 server tests in 116.36 seconds under `MemoryMax=6G` and `MemorySwapMax=0`, peaking at 2.90 GiB. This measures allocation behavior separately from GitHub runner scheduling. The [fast CI retry for `cb77029`](https://github.com/valargroup/wallet-pir/actions/runs/36036029723) subsequently passed on the recovered runner.

A production operator lock serialized rollout. Before every unit change, the current executable and complete unit configuration were checked against captured baselines. Each unit received only an additional ExecStart drop-in pointing to the new immutable binary, preserving the existing argument tail and environment references. Previous binaries and complete unit text were backed up on each host.

| Role | Host | Activation UTC | Verification |
| --- | --- | --- | --- |
| Worker 01 | `10.142.0.15` | 17:53:41 | Current generation 130 and retained 126–130 restored; both replicas ready |
| Worker 02 | `10.142.0.16` | 17:54:06 | Current generation 130 and retained 126–130 restored; both replicas ready |
| Packing router | `10.142.0.14` | 17:54:32 | Coordinator rebuilt/activated in-memory view by 17:55:34 at generation 131 |
| Query ingress | coordinator host | 17:55:47 | Active view restored automatically at generation 131 |
| Coordinator | `167.99.42.60` | 17:56:08 | Stopped-state control backup taken; recovered generation 131 with no pending decisions |

The public query route alone returned controlled HTTP 503 from **17:53:26 to 17:56:22 UTC (2 min 56 s)**. Coordinator control/artifact listeners stayed available while workers/router/ingress rolled. Outstanding router requests were drained before rollout. The exact original Caddy configuration was restored and verified by parsed-JSON equality, including other routes and APM configuration.

The existing six-hour graph qualification, started 17:26:24 UTC, was deliberately interrupted at 17:53:25 for deployment. Only its verified load-test child PID 3372164 received SIGTERM; parent PID 3372163 exited after recording the result. Its original directory `/root/apm-hour-deploy/graph-review-sustained-20260924` is preserved, including an explicit `interrupted-deployment-71be21f.json` marker and `finished.json` with exit -15. The campaign was **not completed or restarted**. It had recorded 1,622 successful init requests and zero init errors over 1,621 seconds; its systemd qualification unit remains failed from the intentional interruption.

## Post-deployment result

Public origin: `https://enhance-pir.valargroup.dev`.

Exact-answer smoke used the independently extracted existing canonical oracle `/root/architecture2-deploy/public-oracle.json`, two clients, offered 2 QPS, 10-second warmup and 60-second measurement, seed 240924111, maximum error rate zero. Exit status zero:

- 120/120 measured answers correct; zero incorrect answers, errors, or unstarted arrivals.
- 128 correct warmup answers; zero warmup errors or incorrect answers.
- Achieved 2.013 correct queries/s; p50 124.735 ms, p95 259.711 ms, p99 4,024.319 ms. This is correctness/smoke evidence, not a latency SLO or sustained qualification claim.
- Final readiness: protocol `ironwood-enhance-pir-v7`, generation **132**, anchor **3494829**; publication advanced after restart. Both replicas retained generations 128–132, router/ingress ready, no blocked reason, pending commits, pending aborts, or active operation. Coordinator resident packing objects/bytes remained zero.
- All five final process hashes matched the new server SHA. APM PID **3371380** and SHA-256 `d6fdb815cba45dbb219ae8fa2e36a0a38b707da1722a8d393445eaa3dc166ac3` were unchanged.

## Evidence and rollback

Each production host retains root-only `/root/enhance-cleanup-71be21f` with baseline JSON, previous unit text/binary, timestamped `events.jsonl`, new configuration hash, and final process audit. Coordinator also retains readiness snapshots after each role, full Caddy backup, exact smoke JSON/log/exit code, maintenance timestamps, and stopped-state `coordinator-control-before` evidence. These may include sensitive configuration and must not be copied into the repository wholesale.

New release: `/opt/enhance-pir/releases/cleanup-71be21f-ba576af27d7f/enhance-pir-server`.
Previous immutable release remains `/opt/enhance-pir/releases/apm-hour-cdc8e7f7f6d0/enhance-pir-server`.
Our only service configuration additions are `zz-cleanup-71be21f.conf` under each relevant `/etc/systemd/system/<unit>.d/`.

No rollback was needed. If rollback becomes necessary, preserve current durable state and fencing, coordinate query drain, verify no concurrent unit edits, then use the host's `python3 /root/enhance-cleanup-71be21f/deploy.py rollback <unit.service>` one unit at a time and verify readiness. The helper removes only our drop-in after checking its expected current configuration hash, restarts against the old binary, and verifies the old executable checksum. Do not restore historical state/control backups to bypass fencing. Router/ingress require coordinator reconciliation before being considered ready.
