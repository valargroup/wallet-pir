# V10 full-chain qualification

Runtime source `8e69ea75b1e0071e3b978b0c78cc0487377f9e82`; comparison operations
`4905d3a30f63b096e514ab6851a95d4b9b0e80e0`. These are qualification results;
public deployment and client validation are recorded in the parent bundle.

The frozen input is a reflink snapshot of the unchanged version-2 production
journal. The controller/reconciler pause while taking the snapshot lasted
0.0714769 s. The snapshot covers 3,499,486; the comparative publication is pinned
to height 3,499,341, hash
`0000000000941f2c717bea13c62af6ef787e4a5b9e3fd90fd304d746b10d63de`,
with the same recent cutoff 3,262,749 as v9.

| Identical input: 353,795,624 events | V9 | V10 |
|---|---:|---:|
| Archive / recent shards | 126 / 13 | 77 / 9 |
| Total shards | 139 | 86 |
| Allocated plaintext directory and page bytes | 51,388,612,608 | 31,457,280,000 |
| GiB | 47.859375 | 29.296875 |

The emitted v10 files match every shard of the [census](../../compact-layout-2026-09-28/census.md).
This is **38.79% less allocated table space**, or **63.36% more history capacity**
at equal table storage for this history. It excludes public filters, manifests,
prepared native caches, replicas, journal and rollback copies. It is not a
throughput or retention guarantee.

Publication took 4,823.020 s (80 min 23 s); user CPU 4,576.31 s, system CPU
100.46 s, maximum resident set 4,665,892 KiB (4.45 GiB). The coordinator was an
8-vCPU/64-GiB memory-optimized Intel host with other production services running.
The previous v9 timing is not a controlled paired CPU benchmark.

`shard-verify` checked all 86 manifests, tables and filters, contiguous coverage,
tiers and anchor, then rebuilt eight selected shards from the snapshot with
byte-identical filters, directories and pages. This spot replay is not an
independent full-chain event-extraction audit. The regression export independently
reduced the selected public-script fixture histories; cases and all checkpoint
expectations are unchanged.

All **172** actual table-segment native certificates pass the configured policy:
128 correctness bits for directories/recent pages and the previously accepted
83-bit archive-page floor. Seven archive-page reports are below 128: shard 14
(97), 22 (112), 23 (96), 30 (117), 31 (102), 32 (103), 39 (122). The minimum is
96. These analytic decryption-failure bounds are conditional on the exported
noise/sampler model and are not lattice security levels. Raw reports, public
setup hashes and policy decisions are in [certificates.tgz](certificates.tgz).
The [accepted archive exception](../../archive-native-certificate-2026-09-28/README.md)
was not lowered for this rollout.

The first fixture comparison failed because its default contract requires an
advancing anchor. This is a same-anchor format replacement. The explicit
`--same-anchor` mode additionally requires equal anchors, cutoff, case definitions,
all expectations and map identity; shared accepted headers must still agree.
The corrected comparison has no blocking or review differences. Both reports,
the original failed qualification log and the resumed success are preserved.

The 31,487,623,852-byte publication including metadata was copied to
`/srv/transparent-data-v10/publications/initial` on the coordinator root disk.
Every file was SHA-256 compared and fsynced before removing only the duplicate
candidate. The journal volume and root retained more than 20% free space. The
logical candidate path remains `/srv/zakura/transparent-shards-v10-full`.
The live publication root must resolve to the same physical parent so sandboxed
hard links preserve sealed data instead of duplicating it each revision.

`bin/SHA256SUMS` identifies exact production executables. Release bundle SHA-256:
`24a86054f6ea74e0b6fbc55e669dc8185f962a11171ab4bae2d10edf82a5c7a7`.
Source bundle SHA-256:
`3b9484f3a043f35cc1ef075ce0992d920e2205226a6f4611f8d1fd79757a784c`.
Linux build flags and toolchain are in `bin/build.json`; all failed and successful
build attempts are retained under `../build/`. Long-running CI was deliberately
bypassed under the user's direct-SSH deployment instruction.
