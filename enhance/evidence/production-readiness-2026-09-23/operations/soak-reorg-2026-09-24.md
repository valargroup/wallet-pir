# Public soak: transient canonical-anchor change

Status: **freshness evidence incomplete pending reorg review**. The public
six-hour load was still running when this event was investigated. This record
does not mark the full freshness or correctness gate passed.

The coordinator freshness observer sampled a temporary publication block at
01:55:30, 01:55:40, and 01:55:50 UTC on 2026-09-24. It reported no ingestion
failure. The [bounded coordinator log](reorg-2026-09-24-coordinator.log)
(SHA-256 `f8cf31b6f27c27e62aafc908bb582115c940f2cac034ef0b4d3e72d0908795c1`)
records `canonical anchor changed during preparation` at 01:55:21.973 UTC
and a rewind of no-longer-canonical height 3,494,062 at 01:55:31.987 UTC.
This is the server's pre-commit canonical-hash guard, followed by its reorg
recovery path; the observation alone does not prove all resulting answers.

The [bounded node log](reorg-2026-09-24-node.log) (SHA-256
`76b832c11ca56189f984c73ae87c2ccdbb7374a97b482850d92289576b0849c9`)
shows an initial committed block at height 3,494,062 with hash
`000000000051bd3223915176b0ee7957e332a7ae27505161a3b17598584f6612`,
then a different verified block at the same height with hash
`000000000034b1c2e2c9b9f0b94fc6ad2403733516fdb63e6003590ea3af2abd`.
Zakura logged a best-chain switch and chain-fork mempool re-verification at
01:55:21.885 UTC, then advertised the second block as committed at
01:55:27.878 UTC. These logs establish a same-height fork, rather than a
height-only ingestion stall. A post-load canonical RPC hash check is still
needed to verify which branch remains canonical.

| UTC sample | Node tip | Published anchor | Generation | Blocked |
|---|---:|---:|---:|---|
| 01:55:10 | 3,494,060 | 3,494,060 | 380 | no |
| 01:55:20 | 3,494,062 | 3,494,060 | 380 | no |
| 01:55:30 | 3,494,062 | 3,494,060 | 380 | yes |
| 01:55:40 | 3,494,062 | 3,494,060 | 380 | yes |
| 01:55:50 | 3,494,063 | 3,494,060 | 380 | yes |
| 01:56:00 | 3,494,063 | 3,494,062 | 381 | no |
| 01:56:10 | 3,494,063 | 3,494,063 | 382 | no |

The [3½-hour assessment](../soak-first3h30m-freshness.json) uses a copied
observer trace with SHA-256
`bc0692124963c2ed5c48036d9a7cd730d0303d61b2d2bb906caddd3862015758`.
Its conservative assessment bounded all 156 observed tip advances within
70.001 seconds but returned `evidence_incomplete` because publication was
reported blocked. The height-only trace cannot identify the orphan and
replacement block hashes when the reorg does not lower the sampled height.
After the measured load, verify the canonical hash from node RPC, the final
load correctness report, and exact public
queries from an independently extracted chain oracle. Keep this event visible
in the full-window assessment; do not suppress the assessor's finding merely
because the block cleared quickly.
