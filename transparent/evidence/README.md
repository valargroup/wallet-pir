# Transparent PIR evidence

Curated September 14, 2026 from existing records; no live verification was performed.
[Status](../docs/status.md) owns acceptance decisions and
[remaining work](../docs/remaining-work.md) owns open gates.
The following records establish only their stated revision, workload and coverage.

| Evidence | Scope and limit |
|---|---|
| [Fleet redeploy to `a317455e`](fleet-redeploy-2026-10-09/README.md) | Production roll of recent-01/02, the publisher and archive-03 to the full-CI release of `a317455e`, under the runbook's gates: recent tier no outage, metadata at most 3.3 s, archive 339 s from the disk cache (164 hits, 0 misses); afterwards 30 minutes of 5 QPS exact load with 0 errors, byte-identical maps, and a 20 QPS gate at 20.51 QPS with all 12,290 queries exact, p50 17 ms, p99 59 ms; not a soak, and the wallet regression fixture is still v10 |
| [Quality monitoring moved to v11](quality-monitoring-v11-2026-10-09/README.md) | Independent canary rebuilt from `854f5677` with the v11 load fixture (anchor 3,488,499) after failing `oracle_invalid` since the cutover; APM moved to the v11 roster and load status; failure controls, passing samples and rollback backups; alert modes unchanged |
| [Sealed tier boundary on the live journal](tier-boundary-sealed-2026-10-08/README.md) | `shard-cutoff` at `569f68e6` run read-only against the live v11 journal: at anchor 3,500,738 the six-month height is 3,289,805 and the boundary moves back to 3,231,753 with 81 archive shards, matching the live map's sealed shards 0–80; plus a tip anchor and recent replica memory with a two-more-shards sizing estimate; nothing published |
| [Txid display v2 native demo](txid-display-v2-2026-10-08/README.md) | `make transparent-txid-demo` at `9557d843`: fixed 113-byte entries with first source and two outputs, one table per bucket; 14 entries match the Python oracle byte for byte through private lookups; every lookup two 40,200 B queries and two 5,648 B replies, absent identical; corrupted oracle refused; local loopback, not a deployment or capacity run |
| [Txid display v2x input-listing census](txid-display-v2x-input-census-2026-10-08/README.md) | Same genesis records with every transparent input listed (experimental v2x, unpublished): mean record 931 B against 330 B, inline at 128 B 47.9% against 92.5%, reserved memory about 1.7-2x; measurement only, no build, RSS or deployment |
| [Txid display genesis journal census and layout](txid-display-genesis-census-2026-10-07/README.md) | Coordinator census of a genesis-to-3,508,673 display journal (17,024,724 records, 426 archives; inline 92.5% at 128 B), then derived layout pricing: today's `txid-2k` needs 899 page segments, so 1,325 runtimes and 51.8 GiB built, not 850 and 33.2 GiB; only 80,000-record pre-NU6 archives fit 64 GB; anonymity classes from the census models; no build, RSS or production measurement |
| [Tiered txid display on production](txid-display-tiered-2026-10-07/README.md) | Deploy journals beside history on 2026-10-06, the live controller timeline and a 20 QPS load and bandwidth run on 2026-10-07: live block to serving p50 16.0 s, max 97.2 s, 22 of 1,054 cycles over 20 s; 20 lookups/s 12,004/12,004 exact, p99 187 ms; pages-4 and up over 300 KB cold; anonymity, `verify` and growth not measured; not an acceptance result |
| [Shipped txid display runtimes, pre-deploy bench](txid-display-shipped-runtimes-bench-2026-10-07/README.md) | Local roman-dev-2 replay with `--ship-runtimes`: 40.05 MiB per runtime file, 80.09 MiB per block; controller prebuild p50 4.36 s at 2 threads; worker CPU per block 1.15 against 5.80 CPU-s; local rsync of one block's delta 0.33 s; 0 fallbacks; cycle p50 4.2 to 6.4 s on equal CPUs; not deployed, network copy not measured |
| [Shipped txid display runtimes, production deploy](txid-display-shipped-runtimes-2026-10-08/README.md) | `--ship-runtimes` on the tiered proof of concept 10:49–13:21 UTC, 121 cycles: prebuild p50 2.8 s, worker prepare 1.4 s and 1.5 CPU-s per block, 0 fallbacks of 242 loads, lookup p99 during loads 1.37× quiet (was 2.5×); block to serving p50 6.7 s but p95 29.9 s because the 96 MiB copy took p95 10.2 s; ended by the v2 cutover |
| [Txid display block-to-serving on production](txid-display-freshness-2026-10-07/README.md) | recent-01 before and after the batched `txid-2k` hint and two build threads: block to serving p50 16.1 to 8.5 s, p95 18.9 to 12.4 s over 40 cycles; history prewarm +3% p50; short window, not an acceptance result |
| [Txid display backfill sizing](txid-display-backfill-sizing-2026-10-07/README.md) | Display-eligible txids per height range, bracketed from four published history maps and calibrated against the live display map (ratio 0.996); archives, memory, disk, ingest and map bytes per candidate start; planning arithmetic only, no production measurement |
| [Tiered txid display proof of concept](txid-display-tiered-2026-10-05/README.md) | Local release-fast runs on synthetic journals: seals and window drops reproduced, all lookups exact, metered bytes equal computed, recent-01-shaped rebuild bench and bucket ablation; includes a failed first run and a harness gap; not deployed, not real-chain packing |
| [Changed-native activity candidate `c3c66b9b`](activity-metadata-2026-10-04/README.md) | Supplemental fat-LTO build manifest for the 13 tools absent from exact-head CI, plus focused and mutation checks of the candidate's preparation and qualification guards; source and fixture evidence only, nothing staged, gated or deployed |
| [Prewarm hint assurance](prewarm-hint-assurance-2026-10-04/README.md) | Source-level follow-up to the prewarm hint: recent-only dispatch, derivation and provenance, independent known-answer vectors, forced reference fallback and mutation results; no timing re-measured, not deployed |
| [Recent-worker prewarm hint](prewarm-hint-2026-10-03/README.md) | Local release-fast comparison on the retained synthetic recent-8k tails: two-table runtime build at two threads −30% to −37% (4.81 s → 3.05 s at 97% fill), identical hints, masks and answers, unchanged peak memory; not deployed and not a freshness qualification |
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
