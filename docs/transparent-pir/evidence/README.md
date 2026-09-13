# Transparent PIR evidence

Updated 2026-09-13. Evidence supports the [deployment decision](../deployment.md); it is not itself an operating instruction. [Remaining work](../remaining-work.md) owns unmeasured gates.

## Current productionization evidence

Use [current status](../status.md) and [remaining work](../remaining-work.md) for
milestone decisions. Rows below identify the evidence's scope; dated failures
remain failures even when a later correction passes.

| Evidence | Established result | Limit |
|---|---|---|
| [M0 baseline](productionize-m0-2026-09-09/README.md) | Reproducible wallet/deployment baseline and findings | Later milestones own acceptance |
| [M1 six-hour acceptance](productionize-m1-six-hour-acceptance-2026-09-13/README.md) | Matching canary and rollout, six hours and 300 blocks on every worker | Later freshness incident remains M5; original 24-hour command failed |
| [M2 recovery application](productionize-m2-2026-09-09/README.md) | Native isolated recovery-only macOS application | Ad-hoc signed, not distributed; M3 precedes authoritative opt-in PIR |
| [M3 fixture correctness](productionize-m3-2026-09-10/README.md) | Independent reduction, history catalogue, forks/reorgs and interruption matrix | Controlled fixtures do not replace release-app public-path checks |
| [M3 repaired public regression](productionize-m3-suite-fix-2026-09-13/README.md) | Frozen binary: eleven cases/68 checkpoints passed; complete checks and per-attempt logs retained | Bounded-retry correctness, not uninterrupted availability or wallet-client adoption |
| [M3 native private-wallet validation](productionize-m3-private-wallet-2026-09-13/README.md) | Recovery and phase-based kill/resume both equal independent reduction | Application screens, directly observed in-flight kill and previously committed live progress not established |
| [M3 original deployed regression](productionize-m3-live-2026-09-13/README.md) | Six passes and five transport failures across eleven cases | Failed attempt retained; repaired frozen-binary acceptance is recorded above |
| [Paired block/PIR comparison](block-comparison-2026-09-09/README.md) | 80 exact recoveries and derived additional-transparent-payload savings | Not whole-wallet incremental latency or sustained capacity |

## Historical evidence ledger

These rows describe **their own capture dates**, not present blockers or current
fleet configuration. In particular, references to an open M1 or a 24-hour gate
below are historical; the current requirement and decision are recorded above.
Raw measurements and dated correction histories remain at their original paths.

| Evidence | Scope | Valid use | Limit at capture |
|---|---|---|---|
| [M1 durable routing audit](productionize-m1-routing-audit-2026-09-10/README.md) | Confirmed outage missed between polls, invalidated run, durable counter and observer correction | Prevents recovered withdrawals from escaping the gate; full checks pass | At capture: Source-only; worker delay unresolved; no active acceptance run |
| [M1 reorg-related withdrawal](productionize-m1-postreorg-2026-09-10/README.md) | Failed 65.7-minute run, local probe window, controller rejection and fresh full gate | Separates reorg-related withdrawal from preceding worker stalls | At capture: Failed run is not accepted; fresh six-hour/300-block gate and fleet observation remain open |
| [M1 collection and forwarding deployment](productionize-m1-collection-forward-canary-2026-09-10/README.md) | Qualified worker and control transport deployed; fresh canary start and preceding diagnostic failure preserved | Establishes exact deployed components and active observation at capture | At capture: Full canary, fleet rollout and 24-hour observation remain open; old local pause cause not fully established |
| [M1 forwarded-status diagnosis](productionize-m1-forwarded-status-2026-09-10/README.md) | Local status remains fast during a remote delay; two bounded forwarding trials | Distinguishes a delay outside the Unix status handler | At capture: Pre-deployment diagnosis; later deployment recorded above; shared pauses need further diagnosis |
| [M1 collection correction qualification](productionize-m1-collection-qualification-2026-09-10/README.md) | Compatible Linux build and three Amsterdam repetitions | All unchanged combined screens pass for `d8f5217` | At capture: Qualification only; later deployment recorded above; not loaded acceptance |
| [M1 collection-lock regression](productionize-m1-collection-lock-2026-09-10/README.md) | Real cache writer lock blocks collection and status; source correction and cancellation regression | Establishes and fixes a reproducible serving-lock stall | At capture: Does not prove all live pauses have this cause; later deployment recorded above |
| [M1 persistent-session status stall](productionize-m1-status-stall-2026-09-10/README.md) | Failed 96.8-minute canary and simultaneous local/remote diagnostic probes | Records renewed status timeout despite persistent connection | At capture: Diagnosis in progress; no acceptance or rollout |
| [M1 status-channel investigation](productionize-m1-status-channel-2026-09-10/README.md) | Failed canary, SSH timing comparison and owned-session lifecycle tests | Separates connection setup from worker status; tests cancellation and reconnection | At capture: Diagnostic trials do not satisfy loaded acceptance |
| [M3 wallet correctness, fixture scope](productionize-m3-2026-09-10/README.md) | Wallet branch from `0ce6d158f`: block model and independent reducer, catalogue of every required history, forks and reorgs, a 22-row interruption matrix, process kills including one through the real bridge, shadow comparison, real-chain sample, independent review | Fixture-local correctness and lifecycle established with exact comparison; imported scripts, an explicit unsupported-script count and `interrupted` on reopen landed | At capture: Three gates remain unexecuted (M1 now accepted) — the deployed accepted-anchor regression suite, the public-path comparison and the kill through the release library against the public services; M3 stays open |
| [M2 recovery application](productionize-m2-2026-09-09/README.md) | Wallet branch from `b6aa1f97f`: store append correction, recovery-only native build, explicit modes, honest coverage, macOS release build, live lifecycle | Close M0-F1/F2/F3; recovery-only custody boundary; requirement-by-requirement record | At capture: No authoritative PIR balance before M3; one fresh synthetic live wallet; release unsigned, not distributed |
| [M1 public 503 correction](productionize-m1-publication-503-2026-09-10/README.md) | Failed canary, routing/control logs, transport regression and operations-only deployment | Isolated control connections and bounded status retry preserve a verified warm quorum after transient transport failure | At capture: Original SSH failure cause unproven; fresh full loaded acceptance remains open |
| [M1 bounded query wait](productionize-m1-query-wait-2026-09-09/README.md) | Three two-slot runs, complete persistence and external clients | All generator screens pass at 8.856–9.721 s, 233 exact queries and two retries | At capture: Fresh actual-hardware canary required; M1 remains open |
| [M1 asynchronous snapshot cache](productionize-m1-async-cache-2026-09-09/README.md) | Three one/two-slot pairs; persistence drained and memory sampled | All one-slot runs pass; two-slot timings improve | At capture: Two-slot availability screen still fails; not deployed |
| [M1 target CPU diagnosis](productionize-m1-target-cpu-2026-09-09/README.md) | Portable backend diagnostic and actual worker CPU sampling | Generator and target hardware differ; portable selection alone does not reproduce slowdown | At capture: Diagnostic, not live acceptance |
| [M1 query-tail qualification](productionize-m1-query-tails-2026-09-09/README.md) | Batched snapshot reads, query phase traces, six Amsterdam repetitions and canary handoff | Generator screen passes; recent-01 warm upgrade verified; live canary failure preserved | At capture: Target CPU/backend differs; canary failed public freshness; no fleet promotion |
| [M1 memory phases](productionize-m1-memory-phases-2026-09-09/README.md) | Construction-to-snapshot handoff, Linux file-cache advice and drained external clients; six Amsterdam repetitions | Lower memory pressure, 10.765–11.566 s two-slot visibility and 5.6–7.5% retries | At capture: Two of three two-slot query-gap checks fail; no live promotion |
| [M1 construction optimization](productionize-m1-construction-2026-09-09/README.md) | Public-mask reuse, parallel collapse, snapshot batching; full-residency Amsterdam repetitions | Packing about 38% faster; two-slot timing screen passes but query availability fails | At capture: No candidate promotion or live canary; memory admission remains open |
| [M1 admission diagnosis](productionize-m1-admission-2026-09-09/README.md) | Separate clients, stage traces, FIFO admission and initial warm-readiness gate | Fixed incomplete startup; corrected two-slot repeats do not consistently meet provisional latency screen | At capture: Isolated frozen fixture; no canary promotion |
| [M1 full recent residency](productionize-m1-full-residency-2026-09-09/README.md) | Frozen real 14-shard assignment, four processes, production memory limits and disk caching | 864 exact queries; modeled memory check passed; two-slot update latency failed | At capture: Co-located clients; complete initial warm readiness was not checked; superseded by admission investigation |
| [M1 isolated burst comparison](productionize-m1-burst-2026-09-09/README.md) | Four processes, real recent-8k geometry, two exact HTTP clients on the Amsterdam generator | Worker preparation comparison: one versus two build slots; 592 exact queries | At capture: Single shard; excludes fleet transport/routing and full residency; no production setting changed |
| [Productionization M1 investigation](productionize-m1-2026-09-09/README.md) | Retrieved failed canary, controller/worker timing and current six-worker readiness | Establish failed acceptance and serial burst-publication delay | At capture: No correction deployed, replacement soak or fleet promotion |
| [Productionization M0 baseline](productionize-m0-2026-09-09/README.md) | Captured wallet working source, adapter/UI tests, checkpoint-write probe and public anchor | Reproduce integration baseline; identify M1/M2 gaps | At capture: Private fleet/soak status unverified; no real-user balance or release-build claim |
| [Amsterdam PIR versus compact blocks](block-comparison-2026-09-09/README.md) | Full chain through 3,473,686; mixed and fresh suites, 20 concurrent wallets per method | 80 exact recoveries, per-wallet traffic/timing and observed server APM | At capture: One pair per suite; synthetic profiles, shared heterogeneous PIR fleet, controlled HTTP baseline |
| [Managed preparation and gated rollout](managed-preparation-2026-09-09/README.md) | Persistent preparation ownership, active reorg validity, prewarm retries and coordinated maintenance | Exact source/tests, failed attempts, corrected recent-01 deployment and dated loaded-canary snapshot | At capture: Six-hour/300-block gate, whole-fleet batch and 24-hour observation still pending |
| [Worker hardening and wallet adapter](hardening-2026-09-08/README.md) | Recent canary, memory/cache corrections, wallet store/API/app changes | Dated source/tests, live query smoke and failed attempts | At capture: Six-hour/300-block gate and wider rollout pending; no user balance proof |
| [Continuous publication rollout](continuous-publication-2026-09-08/README.md) | Mainnet RPC ingestion, suffix publication, warm activation, public routing and wallet adapter | Dated rollout, reorg withdrawal, memory corrections and final acceptance record | At capture: See the run result; no user balance, mobile latency or saturation claim |
| [Local deployment runtime validation](deployment-runtime-local-2026-09-08/README.md) | Synthetic rows at both deployed geometries; offline rollout harness | Cache correctness, restore feasibility and activation/rollback checks | At capture: No live deployment-time, cold-disk or fleet-capacity claim |
| [Deployed regression 2026-09-08](regression-deployed-2026-09-08/README.md) | Full-chain publication, 11 required profiles; 33 completed syncs | Five profiles passed exact recovery checks | At capture: Run failed during sixth profile with HTTP 502; remaining profiles blocked, release gate remains open |
| [Regression preflight 2026-09-08](regression-2026-09-08/README.md) | Frozen 11-wallet, 68-sync accepted-anchor suite; public map preflight | Confirms explicit refusal of the old pilot publication | At capture: No deployed wallet queries or capacity measurements; full-chain run pending |
| [Dataset inventory 2026-09-08](inventory-2026-09-08/README.md) | Journal identity through anchor 3,473,686; cutoff derivation; 11-block independent spot-check; coordinator, pilot worker and public-origin state | The dataset every later gate uses; the live pilot state at that instant | At capture: One instant; the pilot's three-shard set, not the full-chain publication |
| [c-8 measurements](../../transparent-pir-evaluation/shard-utilisation/droplet-c8-measurements.txt) | One ams3 dedicated 8-core Xeon 8280, source a24964b; four-second scaling points and six runtime builds | Approximately 26 GiB/s scan bandwidth, one-thread evaluation saturation, measured runtime RSS and cold builds | At capture: Not proposed host SKUs, independent-host scaling or sustained end-to-end sync |

## Additional operational and measurement records

These dated records retain the detailed path from early deployment to M1. Their
launch-time next steps are historical; use the current milestone checklist.

- [Mixed-tier census, 2026-09-08](census-2026-09-08/README.md)
- [Deployment runtime follow-up — 2026-09-08](deployment-runtime-followup-2026-09-08/README.md)
- [Deployment-runtime live validation — 2026-09-08](deployment-runtime-live-2026-09-08/README.md)
- [Parent-filter evaluation, 2026-09-08](parent-filters-2026-09-08/README.md)
- [Production parent-filter deployment, 2026-09-08](parent-filters-production-2026-09-08/README.md)
- [M1 background cache-advice candidate](productionize-m1-cache-advice-2026-09-11/README.md)
- [M1 invalidated-preparation cancellation canary](productionize-m1-cancel-prepare-2026-09-11/README.md)
- [M1 collection and preparation diagnostics — 2026-09-11](productionize-m1-collection-timing-2026-09-11/README.md)
- [Control-path failure — 2026-09-11](productionize-m1-control-path-failure-2026-09-11/README.md)
- [M1 deferred live cache collection](productionize-m1-deferred-collection-2026-09-11/README.md)
- [Persistent headless worker configuration — 2026-09-11](productionize-m1-headless-2026-09-11/README.md)
- [M1 public HTTP disconnect qualification](productionize-m1-http-retry-2026-09-12/README.md)
- [Incremental runtime-cache writeback qualification](productionize-m1-incremental-writeback-2026-09-11/README.md)
- [M1 publication loading diagnostics](productionize-m1-loading-diagnostics-2026-09-11/README.md)
- [Publication I/O investigation — 2026-09-10](productionize-m1-publication-io-2026-09-10/README.md)
- [Publication I/O canary — 2026-09-11](productionize-m1-publication-io-canary-2026-09-11/README.md)
- [M1 single-CRT NTT reduction candidate](productionize-m1-shoup-reduction-2026-09-11/README.md)
- [M1 publication-stage timing investigation](productionize-m1-stage-timing-2026-09-11/README.md)
- [M1 managed storage-policy canary](productionize-m1-storage-policy-2026-09-11/README.md)
- [Unpublished reorg correction](productionize-m1-unpublished-reorg-2026-09-11/README.md)
- [Load-test workload sample, 2026-09-08](workload-sample-2026-09-08/README.md)

## Comparable measured and projected quantities

| Uniform geometry | Shards | Plaintext, decimal GB | Projected fleet RSS from c-8 per-shard measurements |
|---|---:|---:|---:|
| recent-8k | 1091 | 64.1 | about 287.1 GiB |
| archive-32k | 314 | 73.8 | about 141.5 GiB |
| archive-wide | 162 | 57.1 | about 93.2 GiB |

The RSS column is multiplication of measured isolated-runtime costs, not an observed all-resident fleet under load. In the c-8 report, directory/page RSS is 134.72/134.72 MiB for recent-8k and 230.69/358.75 MiB for archive-wide. Archive-wide build time sums to 14.07 seconds per cold shard. Reservations are 256 and 576 MiB respectively; budgets must also cover process/allocator/HTTP/build/revision overhead.

The old full-chain report's 273/137/91.1 GiB figures use reservation arithmetic; later decision prose rounds or projects differently. Use labeled arithmetic rather than mixing these values. A 5% runtime measurement correction is not a safe total process-headroom policy.

The reported p50 values 0.26 MB (recent-8k) and 0.52 MB (archive-wide) follow two directory query uploads for a median chain script. They are not a complete wallet sync bill. At archive-wide, fewer shard matches improve reported p99 query-upload cost, but maximum tails can worsen. Include filters, setup, responses, transport and multiple owned/unused scripts in real wallet accounting.

The superseded full-chain narrative incorrectly ruled out narrow directories under wider pages before correcting itself. Such shapes are legal when script-based sealing closes earlier; the additional boundaries must be measured. Its claim that wide-profile residency/scaling had never run was superseded by the linked c-8 measurements. Old narratives have been deleted; raw runs remain and version control retains the former prose.

## Required run metadata

Every new run directory must include a machine-readable manifest and short results note:

- Run id, UTC time, source/tool commit, exact command/flags and dependency/build profile.
- Backend/crates, schema, named geometry and row dimensions, seal policy, packing/key-reuse mode.
- Network/genesis, journal identity, inclusive height range, anchor hash/time, cutoff height/algorithm, event and script counts.
- Actual host SKU, region, CPU model/sharing, RAM, storage, OS; client device and network.
- Workload definition, script-to-wallet grouping, concurrency and duration, repetitions, filter/setup/runtime cache state and revision churn.
- Raw outputs, failures/timeouts/incomplete counts, query and completed-sync rates, latency distribution, memory and disk peaks.
- Separate upload, response, setup, filters and transport bytes; decimal GB versus binary GiB; measured versus projected values and formulas.
- Comparison baseline with identical coverage/workload, limitations and acceptance result.

Older explanatory Markdown notes were deleted with the superseded plans. Keep raw logs/JSON immutable after publication; append a correction note and a new run when necessary. Do not overwrite historical measurements with projections. Do not compute a mixed-set percentile by adding two tier percentiles. Do not call script percentiles wallet percentiles or infer daily-active-user capacity from a microbenchmark.
