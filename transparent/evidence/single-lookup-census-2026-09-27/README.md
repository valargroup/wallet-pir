# Single-lookup directory census, 2026-09-27

Phase A (offline) evaluation of the single-lookup directory proposed in §2 of the
2026-09-27 architecture review (Git history at `b693cfde`,
`transparent/docs/architecture_update.md`; the implemented result is in
[architecture](../../docs/architecture.md#directory-placement-and-the-choice-table)).
The evaluated variant differs from the proposal's MPHF. It publishes a per-shard
**choice table**, an xor-retrieval static function that stores about 1.23 bits per
placed script. The table tells the wallet which of its two existing two-choice
candidate rows holds the script. Placement, directory rows and the server are
unchanged. The wallet sends one directory query per matched script and shard
instead of two.

This run is a census and an arithmetic projection. It does not measure wallets.
No schema, publisher, server or wallet change is implemented or deployed.

| Field | Value |
|---|---|
| Source | `a843ce24` plus the uncommitted working tree in `source.patch` (sha256 `fa35e77d…48c2`) |
| Tool | `shard-census`, release profile, built on the host; `PROTOC`, `LIBCLANG_PATH` and `BINDGEN_EXTRA_CLANG_ARGS` set only to satisfy build dependencies |
| Host | `roman-ipir-bench-8vcpu`: 8 vCPU Xeon Platinum 8358, 31 GiB RAM, Ubuntu 24.04.4, rustc 1.89.0 (`host.txt`). Not a production host. |
| Journal | A byte slice of the coordinator's `/srv/zakura/transparent-event-data`, read-only, covering heights 3,262,749–3,473,686: `events.bin` bytes 42,853,870,036–43,367,545,915 and 210,938 block records, with offsets rebased and `meta.json` `start_height` set to 3,262,749. `journal-slice-SHA256SUMS`. Anchor `0000000000755137…0be1d`, the same as the [September census](../census-2026-09-08/README.md). |
| Commands | `shard-census --data-dir <slice> --geometry {recent-8k,recent-4k-8k} --first-shard-id 160 --end-height 3473686 --single-lookup --per-shard` |
| Seal policy | The first reported policy for each geometry: `98304:114688, 7936:8192` (recent-8k) and `49152:57344, 7936:8192` (recent-4k-8k). Later policies in the raw output are the census's default sweep and are not evaluated here. |
| Workload | [Frozen workload sample](../workload-sample-2026-09-08/README.md), 1,080 wallets, run through `pairs.py` |
| Baseline | [Fleet series r3](../runs/fleet-series-2026-09-08-r3/README.md), concurrency 32 |
| Elapsed | 32 s and 35 s wall; peak RSS 44 MB and 36 MB (`census-*-time.txt`) |

## Results

**Boundaries reproduce the September census.** Both geometries seal the same 13
recent shards on page rows (ids 160–172), plus tail 173. The recent-8k fullest row is
8 of 14 in shard 172, which matches `census-2026-09-08/census.txt`. That confirms the
slice and `--first-shard-id` reproduce the published placement salt.

**Choice tables** (measured, from the real placement):

| Quantity | recent-8k | recent-4k-8k |
|---|---:|---:|
| Tables built / failed | 14 / 0 | 14 / 0 |
| Seed retries | 0 | 0 |
| Bytes per table, p50 / max | 5,323 / 7,006 | 5,323 / 7,006 |
| Bits per script | 1.233–1.256 | 1.233–1.256 |
| Total, 14 tables | 75,807 B | 75,807 B |
| Slowest build | 133 ms | 135 ms |

**Two-choice placement at 4,096 directory rows is at its limit.** Under
recent-4k-8k, the fullest row is **14 of 14** in shard 172, with p50 11 and p95 14.
Every shard still fits one segment. But a slightly larger shard would take a
second segment, and that doubles every query against it. So recent-4k-8k is not
safe with two-choice placement at this seal target, whether or not the choice
table is used.

**Matched pairs** (`pairs.tsv`): recent (script, shard) activity pairs per sampled
wallet, excluding filter false positives.

| Class | Sealed pairs | Tail pairs | Tail share | Matched shards |
|---|---:|---:|---:|---:|
| restore-6m | 5.892 | 0.042 | 0.7% | 4.925 |
| multi-script (40) | 52.525 | 0.600 | 1.1% | 13.258 |
| reused-tail | 6.408 | 0.517 | 7.5% | 6.925 |
| catch-up-30d | 2.225 | 0.108 | 4.6% | 1.808 |
| catch-up-7d | 1.925 | 0.225 | 10.5% | 1.217 |
| catch-up-1d | 1.350 | 1.367 | 50.3% | 1.808 |

Cross-check: restore-6m gives 2 × 5.934 = 11.87 directory queries per wallet;
r3 measured 11.821 (1,454 over 123 syncs). Multi-script gives 106.25; r3
measured 105.98.

## Projection (derived arithmetic, not a measurement)

```text
new_total = r3_total - r3_directory + (sealed_pairs + k * tail_pairs) * query + matched_shards * 5,415
```

- `query` is 133,144 B at 8,192 rows or 111,640 B at 4,096 rows: packing keys plus
  selection plus binding, and a 5,136 B response (`tests/geometry_costs.rs`).
- `k` is 2 when the tail keeps two-choice and 1 when it doesn't.
- 5,415 B is the mean table size, 75,807 / 14. The r3 per-sync means are taken over
  all 123 attempted syncs.

| Workload | r3 per sync | 8K, sealed only | 8K, tail too | 4K-8K, sealed only |
|---|---:|---:|---:|---:|
| restore-6m | 3,146,413 B | 2,394,836 B (−23.9%) | 2,389,244 B (−24.1%) | 2,266,328 B (−28.0%) |
| multi-script | 19,761,564 B | 12,875,416 B (−34.8%) | 12,795,530 B (−35.3%) | 11,720,114 B (−40.7%) |

The choice-table downloads are 26.7 KB (1.1% of the new restore-6m total) and
71.8 KB (0.6% of the new multi-script total). The projection holds pages, filters,
setup and manifests fixed. It ignores the extra table's request overhead, and it
does not model latency. It assumes one fewer sequential directory round trip per
pair; that saving is not measured here.

## Gate A outcome

- Passed for **choice table at recent-8k, sealed shards**:
  - construction succeeded on 14 of 14 shards with no retries;
  - the tables are about 1% of per-sync bytes;
  - projected restore-6m saving is about 24%, against a 15% threshold.
- **Tail:** leaving it on two-choice costs about 0.2 points for six-month wallets.
  It matters for short absences, where half of catch-up-1d pairs land on the tail.
  Tail support is a follow-up after sealed shards, not a blocker.
- **recent-4k-8k is not selected.** Two-choice placement reaches the slot limit. It
  would need a denser single-lookup placement (an MPHF or k-perfect hash), and that
  adds about 4 more points on restore-6m. Revisit only if that placement is built
  and replayed.
- Not established: wallet latency, client CPU on mobile, server throughput, false
  positives and HTTP overhead. Phase C owns those.
