# Transparent PIR evidence

Curated September 14, 2026 from existing records; no live verification was performed.
[Status](../docs/status.md) owns acceptance decisions and
[remaining work](../docs/remaining-work.md) owns open gates.
The following records establish only their stated revision, workload and coverage.

| Evidence | Scope and limit |
|---|---|
| [Tail publication profile and compute fix](publication-freshness-2026-10-03/README.md) | Local release-fast benchmark on a synthetic recent-8k tail derived from the retained mainnet day: native publication −48% to −61% (9.67 s → 3.78 s at 97% fill) with byte-identical output; partly filled page-hint reduction; not deployed and not a freshness qualification |
| [Recent replicas on the shared native crate](shared-native-rollout-2026-09-30/README.md) | Rolling upgrade of three recent replicas (one elastic added first) to `6360f0d8` without maintenance, then a 10-minute 20 QPS gate: 12,528/12,528 exact, p99 48 ms; archive owner not upgraded, not a soak |
| [Archive tier from two owners to one](archive-consolidation-2026-09-29/README.md) | Cutover to a single archive owner and the same combined 20 QPS measurement (mixed and archive-only) before and after, recent tier held at two; the archive-only client is built from an uncommitted change |
| [Elastic recent tier validation](elastic-recent-validation-2026-09-29/README.md) | Real droplets in production: operator scale-out/in, scaler-driven scale-out, make-before-break replacement and scale-in under a temporary low-capacity policy; not a capacity measurement |
| [Recent tier from four to two](recent-floor-2026-09-29/README.md) | Inventory rollout and two 20 QPS gates on two recent replicas (before and after moving runtime builds off the query pool); retirement of recent-03/04 |
| [Replica membership fix](replica-membership-2026-09-29/README.md) | Before/after routing of recent replicas across one production deploy over SSH (no CI or soak); every member managed, recent replicas rolled without maintenance; not capacity, and the 24-hour routing gate is open |
| [Continuous 5 QPS query load](continuous-5qps-2026-09-29/README.md) | Enabled ongoing public-path query load; fresh keys, exact row hashes, 80% recent / 20% archive across 85 sealed shards; frozen initial observations and persistent health gates; not whole-wallet throughput or completed capacity acceptance |
| [Schema v10 production cutover](v10-cutover-2026-09-28/README.md) | Same-history 86-shard publication verified and deployed over SSH; public regression 11/11 cases and 68/68 checkpoints; 124/124 exact smoke syncs; four-client load 46 exact of 47 attempts, one query-budget incomplete; one transient init probe 503; no sustained-capacity acceptance |
| [Compact schema v10 layout](compact-layout-2026-09-28/README.md) | Actual sample builder; [full-journal census](compact-layout-2026-09-28/census.md): 38.79% fewer allocated table bytes / 63.36% more capacity for the same history; not throughput or deployment acceptance |
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
| [Cluster qualification baseline](cluster-qualification-2026-09-28/README.md) | Live regression 11/11 cases passed; pinned-anchor load runs, all completed syncs exact; v7 cluster before v8 |
| [Native correctness on real archive tables](archive-native-certificate-2026-09-28/README.md) | Real v8 archive-wide segments certify 149–170 bits (pages) and 197–211 (directories); 83-bit worst-case exception accepted |
| [Native two-mask correctness screen](native-certificate-2026-09-28/README.md) | Analytic certificates for v8 shapes; worst-case query term passes 128 bits to 32,768 rows, 83 bits at 65,536; no served snapshot certified |
| [Schema v9 cutover](v9-cutover-2026-09-28/README.md) | Production v7 to v9: 139-shard set verified, fleet rollout (one automatic rollback), publisher activated; 60-minute metadata window; regression 11/11 cases, 68/68 checkpoints; [pinned load run](v9-cutover-2026-09-28/load/README.md) 45/45 exact with 48–75% fewer bytes in most classes; no capacity series |
| [Version-2 event journal](journal-v2-conversion-2026-09-28/README.md) | Production v1 journal re-encoded into a new directory and extended to 3,499,198; 5 × 5,000-block ranges byte-identical to fresh v9 ingests; spot check 11/11; nothing published |
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

- [Native txid display qualification](txid-display-2026-10-01/README.md): frozen confirmed vectors, private HTTP retrieval and explicit measurement limits.
