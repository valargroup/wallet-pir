# Transparent PIR evidence

Updated 2026-09-07. Evidence supports the [deployment decision](../deployment.md); it is not itself an operating instruction. [Remaining work](../remaining-work.md) owns unmeasured gates.

## Evidence ledger

| Evidence | Scope | Valid use | Limit |
|---|---|---|---|
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
