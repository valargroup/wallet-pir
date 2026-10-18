# Sealed tier boundary on the live journal, 2026-10-08

`shard-cutoff` from wallet-pir `569f68e6` ("seal the last archive shard by fill at the tier
boundary", record schema `transparent-cutoff-v2`) was run on the coordinator against the live v11
event journal. It ran once at v11's own anchor, 3,500,738, and once at the journal's tip. The
first result was compared with the live v11 shard map. Both recent replicas' memory was read to
size the recent tier under the new boundary.

This was a read-only check. Nothing was published, deployed, restarted or reconfigured. The
production lock was not taken. v11 keeps serving with `recent_from` 3,289,805.

## Result

At anchor 3,500,738 the rule behaved as expected:

| | Record | Live v11 map |
|---|---:|---:|
| Calendar height (anchor time minus six months) | 3,289,805 | `recent_from` 3,289,805 |
| Recorded boundary (`cutoff.height`) | **3,231,753** | shard 81 starts at 3,231,753 |
| Archive shards before the boundary | 81 | shards 0–80, all sealed `archive-wide`, contiguous 0–3,231,752 |
| Block before the boundary | 3,231,752 `0000000001129e28…` | shard 80 terminal hash and shard 81 parent hash, same |

Each of the live shards 0–80 reached the `archive-wide` page-row threshold (63,488). Shard 81,
which v11 force-sealed at the old boundary, holds 14,981 page rows (23.6% of the threshold) and
96,629 scripts. Under the new rule those 58,052 blocks move to the recent tier. At this anchor the
recent tier would then start at block time 2026-02-07 06:49:47 UTC rather than 2026-03-29, about
seven and a half months before the anchor instead of six.

The rule is only a check here. It changes nothing until a new full publication is made under it.

At the journal tip, 3,511,195 (2026-10-09 00:33:46 UTC), the six-month height is 3,301,394 but
the boundary is still **3,231,753**, again with 81 archive shards. Shard 81 has not filled by
3,301,394, so a full publication made now would start the recent tier at the same height as
above, covering 3,231,753–3,511,195. The boundary moves in whole archive shards, not block by
block: it stays put until a later anchor's six-month height passes the point where shard 81 would
fill.

| Run | Anchor | Calendar height | Boundary | Archive shards | Wall time | Peak RSS |
|---|---:|---:|---:|---:|---:|---:|
| v11 anchor | 3,500,738 | 3,289,805 | 3,231,753 | 81 | 19 min 51 s | 687 MiB |
| Journal tip | 3,511,195 | 3,301,394 | 3,231,753 | 81 | 24 min 3 s | 672 MiB |

Most of each run is the replay of about 3.3 million blocks: roughly 15 minutes, reading 42.3 GB
and 30.8 GB from disk (`time -v` file system inputs; the second run found part of the journal
cached), sampled once at 88 MB/s. The node RPC phase (header times for the search and the
2,000-block window) took about 5 minutes in the first run and about 9 in the second. The process peaked under 700 MiB. The run's cgroup reached its
12 GiB cap only because the page cache filled with journal reads; there was no OOM kill, and both
runs exited 0.

## Recent replica sizing

These are measured values from 2026-10-09 00:17 UTC (`raw/recent-replicas-20261009T0017Z.txt`),
with the arithmetic below them.

| | recent-01 | recent-02 |
|---|---:|---:|
| Host memory, total / available | 8.33 GB / 5.94 GB (71%) | 8.33 GB / 6.44 GB (77%) |
| Shard server cgroup memory / `MemoryMax` | 1.26 GB / 7 GiB (`MemoryHigh` 5.5 GiB) | 1.28 GB / 7 GiB |
| Runtime cache charged / budget | 1,846,447,200 B / 5 GiB (34%) | same |
| Assigned recent shards; warm runtimes | 9; 18 | 9; 18 |
| Txid display worker | 418 MB (`MemoryMax` 1.5 GiB) | none |

The cache charges each recent shard its full reservation (two `recent-4k-8k` tables, from
`reserved_bytes` in `transparent-shard-server/src/runtime.rs`):

- directory 4,096 rows: 83,933,752 B; pages 8,192 rows: 100,710,968 B;
- **184,644,720 B (176.1 MiB) per shard.** The charged total above is exactly 10 shards: the 9
  assigned plus one retained tail revision.

A built runtime holds less, 117,535,856 B (112.1 MiB) per shard. This matches the on-disk runtime
files of 50,379,384 and 67,156,600 B, 16 of each per replica.

Moving 3,231,753–3,289,804 into the recent tier adds 14,981 page rows. The current recent tier
(shards 82–90) holds 64,847, so the total would be 79,828. At the recent seal threshold of 7,936
rows that is 10 sealed shards and a tail, about **two more recent shards** (11 instead of 9). Page
rows do not add exactly across re-cut boundaries, so the count could be one higher or lower.

With 11 shards:

- **Cache.** The assignment planner refuses a replica whose recent shards exceed 95% of its cache
  (`--headroom 0.05`). That allows 27 shards. Eleven are charged 2,031,091,920 B, 38% of the
  5 GiB cache, or 2.22 GB (41%) with one retained tail revision.
- **Process memory.** Actual memory grows by about 235 MB (2 × 117.5 MB). The shard server
  cgroup would be about 1.5 GB of its 7 GiB `MemoryMax`.
- **Host memory.** Available memory would fall to about 68% on recent-01 and 75% on recent-02,
  far above the 20% floor.
- **During a republish.** All recent shards change, so for a while each replica could hold the
  old and the new tier together (10 + 12 charged shards, 4.06 GB, 76% of the cache). Built, that
  is about 2.6 GB, and recent-01 would still have about half its memory available.
- **Archive owner.** archive-03 gives up one `archive-wide` shard, 536,966,256 B (512.1 MiB)
  charged.

Confidence: the bytes per shard are high confidence, because the formula reproduces the live cache
counter exactly. The shard count is medium confidence, ±1. Neither changes the conclusion: both
replicas have room for several more recent shards.

## Files

- `cutoff-3500738.json`: the `transparent-cutoff-v2` record at v11's anchor.
- `cutoff-tip.json`: the record at the journal tip.
- `manifest.json`: run metadata (source, build, binary digest, host, commands, timings, limits).
- `raw/run-run.sh`, `raw/run-sample.sh`: the wrapper each run used and the 5-second cgroup
  memory sampler.
- `raw/run-<run>.stderr`, `raw/run-<run>.time`, `raw/run-mem-<run>.log`: tool output with
  `/usr/bin/time -v`, start and end times, and memory samples (current, peak, `/` use).
- `raw/build-569f68e6.txt`: the build script and the end of its log.
- `raw/live-v11-shards-3511112.json`: the active v11 publication's `shards.json`, copied
  read-only when the publication was at 3,511,112.
- `raw/live-v11-active-3511112.txt`: that publication's `active.json` and the map file's SHA-256.
- `raw/recent-replicas-20261009T0017Z.txt`: free memory, unit limits, selected metrics and
  runtime file sizes from both recent replicas.
- `SHA256SUMS`: digests of the files above.

Neither record contains credentials. The tool read the RPC cookie by path and records only
`"source": "rpc"`.

## Limits

- The cross-check compares the boundary, shard count and boundary hashes. Per-shard ranges of
  the replayed shards 0–80 are not printed by the tool. They are implied because the sealer is
  deterministic and its count and end both match, but they were not compared one by one.
  (A two-tier `shard-census` was declined.)
- The recent shard count under the new boundary is arithmetic, not a replay.
- Replica memory is one snapshot, not a series. Republish behaviour is projected.
- Times run past midnight UTC into 2026-10-09; the directory keeps the date the change was made.
