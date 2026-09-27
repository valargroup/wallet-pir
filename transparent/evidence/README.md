# Transparent PIR evidence

Curated September 14, 2026 from existing records; no live verification was performed.
[Status](../docs/status.md) owns acceptance decisions and
[remaining work](../docs/remaining-work.md) owns open gates.
The following records establish only their stated revision, workload and coverage.

| Evidence | Scope and limit |
|---|---|
| [M0 baseline](productionize-m0-2026-09-09/README.md) | Source/application inventory; later milestones own acceptance |
| [M1 accepted six-hour window](productionize-m1-six-hour-acceptance-2026-09-13/README.md) | Complete observation includes the later failed 24-hour command; M5 follow-up stays open |
| [M1 matching canary and fleet rollout](productionize-m1-deferred-collection-2026-09-11/README.md) | Includes failed upgrade and recovery, exact source/build and corrected rollout |
| [Router retry qualification](productionize-m1-http-retry-2026-09-12/README.md) | Includes failed observation used by the operations audit regression |
| [M2 native recovery application](productionize-m2-2026-09-09/README.md) | Ad-hoc signed; not distribution acceptance |
| [M3 fixture correctness](productionize-m3-2026-09-10/README.md) | Independent reducer, histories, forks/reorgs and interruption matrix |
| [Original M3 public regression](productionize-m3-live-2026-09-13/README.md) | Six passes and five transport failures; remains a failed attempt |
| [Repaired M3 public regression](productionize-m3-suite-fix-2026-09-13/README.md) | Eleven cases/68 checkpoints; bounded retries do not prove availability |
| [Native private-wallet comparison](productionize-m3-private-wallet-2026-09-13/README.md) | Exact recovery/resume; app screens and stronger live interruption remain open |
| [Paired block/PIR comparison](block-comparison-2026-09-09/README.md) | 80 exact recoveries; derived additional-payload savings, not whole-wallet speedup |
| [Dataset inventory](inventory-2026-09-08/README.md) | Journal coverage and source provenance |
| [Full-chain census](census-2026-09-08/README.md) | Sizing inputs, not measured wallet capacity |
| [Publication verification](publication-2026-09-08/README.md) | Independent spot checks and publication provenance |
| [Frozen workload sample](workload-sample-2026-09-08/README.md) | Still used by the load workflow |
| [Single-lookup directory census](single-lookup-census-2026-09-27/README.md) | Offline choice-table sizes, 4K placement limit and projected bytes; no wallet measurement |
| [Single-lookup directory measurement](single-lookup-measure-2026-09-27/README.md) | Paired loopback syncs with and without choice tables on one host; 939/939 exact; not WAN, mobile or fleet |
| [Single-lookup directory fleet benchmark](single-lookup-fleet-2026-09-27/README.md) | Temporary 4-worker bench fleet, not production; steps 8/32 all exact; 128 unstable in both variants from router health checks |
| [Range-filter precision sweep](filter-precision-sweep-2026-09-28/README.md) | Exact recent filter sizes for P=8–19; derived false-match cost model; no profile implemented |
| [Directory placement at 4,096 rows](directory-placement-4k-2026-09-28/README.md) | Model of the two-choice placer; no overflow up to 96% load; corrects the census reading |
| [Sync latency under emulated delay](single-lookup-latency-2026-09-28/README.md) | Loopback netem 50/100 ms RTT; tables −18% and prefetch −31% on restore-6m p50; not mobile |
| [Bulk-history isolation census](bulk-isolation-census-2026-09-28/README.md) | Offline boundaries with long histories in bulk storage; restore-6m matched shards −28%, projected bytes −11%; no bulk table implemented |
| [Initial full-chain publication](continuous-publication-2026-09-08/README.md) | Historical topology and rollout; M1 is the later operational acceptance |
| [Parent-filter evaluation](parent-filters-2026-09-08/README.md) | Offline sweep and incomplete paired HTTP performance evaluation |
| [Parent-filter artifact rollout](parent-filters-production-2026-09-08/README.md) | Bounded recovery canary; not heavy-wallet performance acceptance |
| [Routing withdrawal audit](productionize-m1-routing-audit-2026-09-10/README.md) | Unique invalidation and durable-counter rationale |
| [Background advice qualification](productionize-m1-cache-advice-2026-09-11/README.md) | Supporting qualification for the matching accepted worker |
| [Reduction and disk-stall qualification](productionize-m1-shoup-reduction-2026-09-11/README.md) | Arithmetic argument, source, rejected candidates and qualified correction |
| [Storage policy](productionize-m1-storage-policy-2026-09-11/README.md) | Supporting fleet preflight and storage provenance |

## Deployment baselines

The [three fleet-series repetitions](runs/README.md) support the current deployment
comparison; incomplete/failed work remains in each run. [Filter and geometry
baselines](baselines/README.md) preserve format inputs and isolated sizing evidence.
They do not establish an accepted sustained beta operating envelope.

Follow the [shared metadata and retention rules](../../evidence/README.md). Superseded diagnostic
runs are listed in the [cleanup ledger](../../docs/cleanup-2026-09-14.md); their old
status descriptions must not be read as present blockers or current fleet settings.
