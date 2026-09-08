# Transparent PIR deployment

Accepted target: 2026-09-07. Implement and validate through [remaining work](remaining-work.md). Live state is recorded only in [status](status.md). This supersedes the nine-small-archive-worker plan and the single-global-geometry proposals.

## Target configuration

| Parameter | Recent | Archive |
|---|---|---|
| Initial range | Last six calendar months of pinned anchor time | Genesis through block before recent range |
| Initial profile | `recent-8k` | `archive-wide` |
| Directory/page rows | 8192 / 8192 | 32768 / 65536 |
| Row bytes; inline events | 3584; 2 | 3584; 2 |
| Seal target:capacity (scripts, pages) | Derive with `SealPolicy::for_geometry` | `393216:458752,63488:65536`, verify against derivation |
| Workers | 4 full recent replicas | 2 disjoint archive assignments |
| Host target | 4 vCPU / 8 GiB | 8 vCPU / 64 GiB, memory optimized |
| Runtime cache | 5 GiB = 5368709120 bytes | 48 GiB = 51539607552 bytes |
| Process MemoryMax | 7 GiB | 56 GiB |
| Swap | Disabled for service | Disabled for service |
| Build/query slots initially | 1 / 2 | 1 / 2 |
| Revision retention | 3 superseded plus current, bounded disk policy | Same where revisions apply |
| Local SSD target | Included disk (160 GiB price baseline) | 200 GiB each |
| Warm state | All assigned tables and advertised tail; budget retained tail runtimes | Entire current assignment before fleet readiness |

These are proposed operating budgets within the accepted architecture, not target-host RSS measurements. Cache reservations omit overhead. Verify full-process and cgroup memory during cold builds, queued HTTP traffic, revision overlap and restart. Do not raise limits automatically to fit a failed census.

Use one 2 vCPU / 4 GiB routing host initially. This is a single point of failure. The existing chain node/indexer/publisher remains separate from retrieval capacity budgeting. Keep archive restores off recent workers. Serve immutable public filters and setup from an object/CDN origin, with a refreshable map; verify cross-origin map consistency and retain an independent wallet anchor source.

## Geometry optimization, not a launch dependency

Add optional `recent-4k-8k` (4096 directory / 8192 page rows), seal policy `49152:57344,7936:8192`. Do not change the meaning of `recent-4k`, which already means 4096/4096. Promote only on same-range census, real placement, and total wallet-byte/latency evidence. Smaller directory upload alone does not establish a smaller sync. Initial full-chain deployment proceeds with `recent-8k`.

Do not publish `archive-32k` as the selected target: uniform-chain evidence favors `archive-wide`. Preserve the tested registry entry for compatibility/research. Geometry fallback is an explicit decision with re-census, storage, client and capacity review; never silently rewrite an already published profile.

## Sizing and availability

Uniform full-chain `archive-wide` evidence is 162 shards and 57.1 decimal GB plaintext. Applying measured c-8 per-shard RSS gives approximately 93.2 GiB prepared residency, or 81 shards / 46.6 GiB per half. This is a sizing proxy, not the mixed-tier census. Two 48 GiB cache reservations can hold the approximate halves at 576 MiB reserved per shard, but actual RSS, retained revisions and assignment imbalance must pass validation.

Retain space for the current assignment, candidate publication and rollback artifacts plus at least 20% disk headroom. The publisher needs independent peak-RSS and temporary-disk measurements; census RSS is not publisher RSS. Do not duplicate immutable sealed bytes per tail revision unnecessarily.

Losing an archive host makes its range unavailable until recovery. Recent replication does not make archive or router highly available. The initial target accepts that explicit interruption. If archive availability requirements change, evaluate two 128 GiB hosts with complete archive copies, or replicated assignments, before claiming failover. Do not apply the old nine-host 219 restores/s estimate to this fleet.

## Budget baseline

Public DigitalOcean list prices checked 2026-09-07: four regular Basic 8 GiB/4 vCPU hosts at $48, two regular memory-optimized 64 GiB/8 vCPU hosts at $336, one Basic 4 GiB router at $24: **$888/month compute**. [Provider pricing](https://www.digitalocean.com/pricing/droplets).

This excludes existing node/indexer/publisher, CDN/object storage, backups, taxes and transfer overages. Basic shared workers are a cost baseline, not guaranteed sustained bandwidth. Recheck SKU/region availability and benchmark both shared and dedicated alternatives before provisioning. Additional vCPUs do not imply independent memory bandwidth. Two full-copy 128 GiB archive hosts alone would cost $1344/month at the same listed family.

## Publication and rollout procedure

1. Verify live state and journal identity. Pin source SHA, schema, anchor hash/time, explicit cutoff height and cutoff derivation algorithm in candidate metadata.
2. Use a new publication directory; never overwrite the rollback set or switch an existing schema in place. Publisher CLI supports both tiers, but expose and validate those inputs in CI before relying on the workflow.
3. Validate complete coverage, manifests, tables, filters, true directory placement and exact replay before touching running services. Stage verified bytes at assigned owners.
4. Run retrieval deployment `preflight` with the tested full main SHA and explicit shard directory. Current workflow lives at `.github/workflows/deploy-transparent-shard.yml`; inspect its inputs at execution time.
5. Prepare required runtimes, then activate consistent map/routing/revision state. A loaded-only readiness response is insufficient for the warm fleet.
6. Canary real wallet recovery, tail republishing and outage handling, then expand only after workload gates pass. Record actual deployment state and measurement artifacts.
7. Roll back binary, publication and routing together. Preserve the previously valid revision/coverage relationship; a wallet must explicitly recover from an incompatible or lower anchor rather than silently accept it.

The filter service currently ships through the Enhance deployment workflow. A filter binary/map schema mismatch can fail that coordinated rollout. Verify the filter service unit's shard directory and preserve the Enhance legacy rollback path. Do not copy a transparent Caddy configuration onto the Enhance coordinator without integration validation. Decoupling filter deployment is a planned work item.

## Ingest, infrastructure and aging

The journal's start height is persistent identity, not a resume cursor. Read metadata before starting ingestion; a genesis journal requires `start_height=0`. A mismatched start can set the old journal aside. Keep the workflow mismatch guard and resume from committed next height. Publication uses the read-only journal-opening path.

Ingest reads the chain from the node's own RocksDB (`--state-dir`, the
workflow's `state_dir` input, `/root/.cache/zakura` on the coordinator) as a
read-only secondary instance; previous outputs resolve through the node's
transaction index. Omitting `state_dir` selects the JSON-RPC path, which is
roughly a hundred times slower and exists only so an old dispatch keeps its
meaning. Resume a genesis journal with `start_height=0` and `state_dir` set.

Historical reports mention Terraform state drift, unintended transparent-spend provisioning, SSH firewall drift, volume lookup mismatch and stale SSH host keys. Re-verify these as preflight findings rather than blindly following archived repair commands. Inspect the saved plan and protect the shared node/volume and Enhance services.

Freeze the initial cutoff for the pilot. Derive six calendar months from the pinned UTC anchor timestamp with a documented month-end rule and a deterministic height selection; record the chosen height instead of recalculating it each publish. Enforce a forced shard boundary there. Do not use approximate block counts as calendar time.

Old recent shards keep their geometry forever within the publication lineage. They can move to archive ownership after verified copying and routing handoff. Budget their actual growth; the previous 6.5 GiB/year figure is an extrapolation, not a retention guarantee. Re-cutting into wider shards is deferred until epoch identity and wallet replay semantics are specified and tested.
