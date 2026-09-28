# Full-journal compact-layout census, 2026-09-28

**For identical coverage, v10 needs 38.79% fewer allocated plaintext table bytes,
or supports 63.36% more of this history at the same table-storage budget.**
The read-only replay covers all 353,795,624 events from genesis through height
3,499,341. Every recorded v9 shard terminal hash agrees with the journal.

| Same mainnet history | v9 baseline | v10 census |
|---|---:|---:|
| Archive shards | 126 | 77 |
| Recent shards | 13 | 9 |
| Total shards | 139 | 86 |
| Allocated directory + page tables | 51,388,612,608 B | 31,457,280,000 B |
| Binary GiB | 47.859375 | 29.296875 |

Every v10 shard places into one directory segment and one page segment. The
census uses the actual byte-weighted directory placer and the builder's exact
page-demand rule. It re-seals the history under v10, retaining the v9 tier cutoff
at 3,262,749; it does not reinterpret or resize the old shards in place.

Capacity gain is `v9_bytes / v10_bytes - 1`; storage reduction is
`1 - v10_bytes / v9_bytes`. The calculation includes allocated fixed-size tables,
including unused rows. It excludes filters, choice tables, manifests, runtime
caches, replication and retained revisions. This is storage capacity for this
observed event mix, not wallet throughput or a universal workload guarantee.

The replay took 3,917.77 seconds with 493,540 KiB peak process RSS on the existing
8-vCPU coordinator, restricted to low CPU/I/O scheduling priority and a 4 GiB
cgroup. Production v9 continued serving. The cgroup includes file cache and is
not the process RSS. This is not a controlled publisher timing comparison.

The actual bounded-sample builder and decode checks are in [the source
qualification](README.md). This full-chain census calculates demand and
placement; emitted production tables, native certificates, client recovery and
load acceptance require their own publication/deployment evidence.

[Manifest](census-manifest.json), [per-shard results](census.jsonl),
[resource report](census-time.txt). The original source-qualification manifest
remains an immutable record of its initial files.
