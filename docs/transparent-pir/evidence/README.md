# Transparent PIR evidence

Updated 2026-09-09. Evidence supports the [deployment decision](../deployment.md); it is not itself an operating instruction. [Remaining work](../remaining-work.md) owns unmeasured gates.

## Evidence ledger

| Evidence | Scope | Valid use | Limit |
|---|---|---|---|
| [M2 recovery application](productionize-m2-2026-09-09/README.md) | Wallet branch from `b6aa1f97f`: store append correction, recovery-only native build, explicit modes, honest coverage, macOS release build, live lifecycle | Close M0-F1/F2/F3; recovery-only custody boundary; requirement-by-requirement record | No authoritative PIR balance before M3; one fresh synthetic live wallet; release unsigned, not distributed |
| [M1 public 503 correction](productionize-m1-publication-503-2026-09-10/README.md) | Failed canary, routing/control logs, transport regression and operations-only deployment | Isolated control connections and bounded status retry preserve a verified warm quorum after transient transport failure | Original SSH failure cause unproven; fresh full loaded acceptance remains open |
| [M1 bounded query wait](productionize-m1-query-wait-2026-09-09/README.md) | Three two-slot runs, complete persistence and external clients | All generator screens pass at 8.856–9.721 s, 233 exact queries and two retries | Fresh actual-hardware canary required; M1 remains open |
| [M1 asynchronous snapshot cache](productionize-m1-async-cache-2026-09-09/README.md) | Three one/two-slot pairs; persistence drained and memory sampled | All one-slot runs pass; two-slot timings improve | Two-slot availability screen still fails; not deployed |
| [M1 target CPU diagnosis](productionize-m1-target-cpu-2026-09-09/README.md) | Portable backend diagnostic and actual worker CPU sampling | Generator and target hardware differ; portable selection alone does not reproduce slowdown | Diagnostic, not live acceptance |
| [M1 query-tail qualification](productionize-m1-query-tails-2026-09-09/README.md) | Batched snapshot reads, query phase traces, six Amsterdam repetitions and canary handoff | Generator screen passes; recent-01 warm upgrade verified; live canary failure preserved | Target CPU/backend differs; canary failed public freshness; no fleet promotion |
| [M1 memory phases](productionize-m1-memory-phases-2026-09-09/README.md) | Construction-to-snapshot handoff, Linux file-cache advice and drained external clients; six Amsterdam repetitions | Lower memory pressure, 10.765–11.566 s two-slot visibility and 5.6–7.5% retries | Two of three two-slot query-gap checks fail; no live promotion |
| [M1 construction optimization](productionize-m1-construction-2026-09-09/README.md) | Public-mask reuse, parallel collapse, snapshot batching; full-residency Amsterdam repetitions | Packing about 38% faster; two-slot timing screen passes but query availability fails | No candidate promotion or live canary; memory admission remains open |
| [M1 admission diagnosis](productionize-m1-admission-2026-09-09/README.md) | Separate clients, stage traces, FIFO admission and initial warm-readiness gate | Fixed incomplete startup; corrected two-slot repeats do not consistently meet provisional latency screen | Isolated frozen fixture; no canary promotion |
| [M1 full recent residency](productionize-m1-full-residency-2026-09-09/README.md) | Frozen real 14-shard assignment, four processes, production memory limits and disk caching | 864 exact queries; modeled memory check passed; two-slot update latency failed | Co-located clients; complete initial warm readiness was not checked; superseded by admission investigation |
| [M1 isolated burst comparison](productionize-m1-burst-2026-09-09/README.md) | Four processes, real recent-8k geometry, two exact HTTP clients on the Amsterdam generator | Worker preparation comparison: one versus two build slots; 592 exact queries | Single shard; excludes fleet transport/routing and full residency; no production setting changed |
| [Productionization M1 investigation](productionize-m1-2026-09-09/README.md) | Retrieved failed canary, controller/worker timing and current six-worker readiness | Establish failed acceptance and serial burst-publication delay | No correction deployed, replacement soak or fleet promotion |
| [Productionization M0 baseline](productionize-m0-2026-09-09/README.md) | Captured wallet working source, adapter/UI tests, checkpoint-write probe and public anchor | Reproduce integration baseline; identify M1/M2 gaps | Private fleet/soak status unverified; no real-user balance or release-build claim |
| [Amsterdam PIR versus compact blocks](block-comparison-2026-09-09/README.md) | Full chain through 3,473,686; mixed and fresh suites, 20 concurrent wallets per method | 80 exact recoveries, per-wallet traffic/timing and observed server APM | One pair per suite; synthetic profiles, shared heterogeneous PIR fleet, controlled HTTP baseline |
| [Managed preparation and gated rollout](managed-preparation-2026-09-09/README.md) | Persistent preparation ownership, active reorg validity, prewarm retries and coordinated maintenance | Exact source/tests, failed attempts, corrected recent-01 deployment and dated loaded-canary snapshot | Six-hour/300-block gate, whole-fleet batch and 24-hour observation still pending |
| [Worker hardening and wallet adapter](hardening-2026-09-08/README.md) | Recent canary, memory/cache corrections, wallet store/API/app changes | Dated source/tests, live query smoke and failed attempts | Six-hour/300-block gate and wider rollout pending; no user balance proof |
| [Continuous publication rollout](continuous-publication-2026-09-08/README.md) | Mainnet RPC ingestion, suffix publication, warm activation, public routing and wallet adapter | Dated rollout, reorg withdrawal, memory corrections and final acceptance record | See the run result; no user balance, mobile latency or saturation claim |
| [Local deployment runtime validation](deployment-runtime-local-2026-09-08/README.md) | Synthetic rows at both deployed geometries; offline rollout harness | Cache correctness, restore feasibility and activation/rollback checks | No live deployment-time, cold-disk or fleet-capacity claim |
| Full-chain raw runs: [recent-8k](../../transparent-pir-evaluation/shard-utilisation/fullchain-8192.txt), [archive-32k](../../transparent-pir-evaluation/shard-utilisation/fullchain-archive-32k.txt), [archive-wide](../../transparent-pir-evaluation/shard-utilisation/fullchain-archive-wide.txt) | Heights 0–3,473,686; 352,873,356 events; 9,264,547 indexable scripts; uniform geometry runs | Relative shard counts, plaintext allocation, chain-script matches and projected query upload | Not a mixed-tier publication, wallet-population sample, mobile latency or HTTP throughput |
| [Deployed regression 2026-09-08](regression-deployed-2026-09-08/README.md) | Full-chain publication, 11 required profiles; 33 completed syncs | Five profiles passed exact recovery checks | Run failed during sixth profile with HTTP 502; remaining profiles blocked, release gate remains open |
| [Regression preflight 2026-09-08](regression-2026-09-08/README.md) | Frozen 11-wallet, 68-sync accepted-anchor suite; public map preflight | Confirms explicit refusal of the old pilot publication | No deployed wallet queries or capacity measurements; full-chain run pending |
| [Dataset inventory 2026-09-08](inventory-2026-09-08/README.md) | Journal identity through anchor 3,473,686; cutoff derivation; 11-block independent spot-check; coordinator, pilot worker and public-origin state | The dataset every later gate uses; the live pilot state at that instant | One instant; the pilot's three-shard set, not the full-chain publication |
| [c-8 measurements](../../transparent-pir-evaluation/shard-utilisation/droplet-c8-measurements.txt) | One ams3 dedicated 8-core Xeon 8280, source a24964b; four-second scaling points and six runtime builds | Approximately 26 GiB/s scan bandwidth, one-thread evaluation saturation, measured runtime RSS and cold builds | Not proposed host SKUs, independent-host scaling or sustained end-to-end sync |
| Earlier shard utilisation raw files in `docs/transparent-pir-evaluation/shard-utilisation/` | Bounded Ironwood/genesis subsets and several schemas | Reproduce packing/placement development with recorded coverage | Never substitute a partial journal for whole-chain data |
| Historical retrieval experiments (version control history) | Python/transparent-history HTTP, navigation and key-reuse backends | Investigate historical behavior on matching backend/workload | Not active transparent-shard wire parameters or deployment capacity |

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
