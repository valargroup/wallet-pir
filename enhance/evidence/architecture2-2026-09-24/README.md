# Architecture update 2: SSH rollout, September 24, 2026

The new serving path is deployed at `https://enhance-pir.valargroup.dev`.
Caddy reopened query admission at 16:18:12 UTC after a 139-second cutover.
The coordinator, both evaluation workers, dedicated packing router, and ingress
run server SHA-256 `c74e23194074f4e55ba72db804dacdcd8e31d8386e65d17eff1ee9fe836a2d5f`.
The APM sidecar was not deployed by this task. Its requested server-side
`http_metrics.rs` integration is included and verified live.

Source is based on `cb618d6` (server code from `574f5ce`) plus the workspace
files bound by `source.json`.
`build.json` records the binary hashes and CPU flags. The Linux release-fast build
uses Rust 1.91 with `-C target-cpu=skylake-avx512`. A first native-CPU build
crashed with SIGILL on the older router CPU; it never served production and was
replaced before qualification. The second fixture attempt exposed a missing empty
journal block in the harness; correcting the harness required no server changes.

## Initial qualification passed

- 86 server library tests passed; 8 HTTP regression tests passed, 3 large capacity
  scenarios remained explicitly ignored; 2 pool-expansion tooling tests passed.
- The extracted-path integration passed locally and on the physical router. It
  checks fresh encrypted answers, replica selection, one retry only on explicit
  rejection, no ambiguous replay, a three-worker pool publication without extra
  packing copies, controller heartbeat expiry and explicit revocation.
- Physical trial: 600,000 fixture records, eight publications, four concurrent
  clients and 256 checked publication-round answers. Five distinct retained
  objects plus publication construction overlap peaked at **6.172 GiB**, below
  the 7-GiB process cap on the actual 8-GiB/4-vCPU router. Swap and all memory
  pressure/OOM counters stayed zero. Twenty disconnected slow uploads released
  admission. A physical process restart started unready and rebuilt exact-session
  state before queries resumed. The complete trial passed in 430 seconds.
- The restart deliberately changes process identity. The original sampler reports
  35 identity errors after that event; these are retained as evidence, not removed.
  Separate post-restart peak and event files accompany the successful test log.
- The production wallet client verified 28 canonical records, routing refresh and
  a cover round. Oracle generation checked the anchor hash against the local
  canonical journal and independently queried zakurad. No wallet protocol changed.
- Pooled production placement has two workers and replication factor two. The
  coordinator reports remote packing enabled and zero resident packing objects.
  Both worker URLs appear in the new router's evaluation accounting. Production
  publication advanced after cutover.
- Offline rollback rehearsal preserved existing publication and recovery fields
  and rejected a placement that could not map back to complete legacy pairs.

The short two-client public run returned 638/638 correct answers, no errors,
10.62 correct QPS, p50 157.31 ms, p95 185.60 ms and p99 222.46 ms. The
four-client run returned 936/936 correct answers, no errors, 15.59 correct QPS
and p99 290.56 ms. The preceding legacy baseline returned 1,096/1,096 correct answers, 18.26 QPS and
p99 193.28 ms. These short runs show lower throughput on the new four-core router;
they do not establish a capacity increase. Full reports are retained alongside
this file.

## Scope and remaining qualification

The physical pre-cutover fixture used evaluation workers and ingress inside the
test process on the coordinator; only the packing router occupied its final host.
Post-cutover sampling covers the actual coordinator, ingress, router and both
8-GiB workers. Sustained production qualification is recorded separately when its
elapsed-time gate completes; the seven-minute fixture run does not satisfy six
hours of fleet qualification.

The measured initial assignment is one growing query domain with five retained
sessions and four admitted router requests. The configured six-object ceiling is
an admission guard, not proof of six-object multi-domain qualification. Physical
multi-router assignment moves, extra sealed-domain capacity, and automatic fleet
expansion are not authorized by these measurements. The APM sidecar remains a
separate deployment.

See `physical-summary.json`, `physical-router-http.log`,
`physical-router-samples.jsonl`, `public-wallet.log`, and the captured load reports.
Operational details and safe rollback are in
[the SSH runbook](../../ops/deploy/architecture2-ssh.md).

## Sustained campaign in progress

The six-hour production load started at **16:22:47 UTC**, September 24, with
four generators and an offered 8 QPS. Completion is expected shortly after
22:22:47 UTC (02:22:47 Dubai time on September 25). The service is
`architecture2-production-qualification.service` on the coordinator; artifacts
are under `/root/architecture2-deploy`. All five serving processes are sampled
every two seconds. `assess-serving.py` evaluates duration, all offered arrivals,
answer correctness, the 0.1% error-rate and one-second p99 bounds, sampling
continuity, process identity, memory/swap events and publication progress.
No six-hour pass is claimed before that assessment completes.

The progress push merges upstream `ipir-sp` rc.3 and passes
`cargo check --locked -p enhance-pir-server --all-targets` on the merged tree.
The deployed/qualified binary remains the frozen artifact in `build.json`; the
dependency merge does not silently replace it. `collect-serving.py --wait` is
running locally to collect all five sample streams and assess the campaign once
its service finishes. It records a failure if completion evidence is absent.
