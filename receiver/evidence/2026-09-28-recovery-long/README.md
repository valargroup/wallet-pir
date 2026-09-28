# Long-history 25-key recovery benchmark

September 28, 2026. All six runs passed. Both paths recovered the same five notes,
identified the one spent note and produced valid database witnesses for all four
unspent notes. Each unspent note is worth 100000 zatoshis.

## Timing

Medians of three alternating trials per mode on an Apple M3 Ultra:

| Phase | Ordinary scan | Private recovery |
| --- | ---: | ---: |
| Total including wallet setup | 32.309 s | 24.003 s |
| Wallet/cache setup | 0.280 s | 0.280 s |
| Compact scanning | 31.791 s | 23.015 s |
| Receiver lookups | 0.000 s | 0.321 s |
| Enhance fetches | 0.227 s | 0.230 s |
| Authentication and private import | 0.005 s | 0.137 s |

Private recovery took **25.7% less time** in this local synthetic workload.
This is not a measured mainnet seed-restore speedup. Block/tree-state downloads,
WAN/Tor latency, UI enrichment, outgoing recovery and transaction proving are excluded.
The ordinary path also used Enhance PIR to authenticate each recovered memo.
No compiler or other benchmark was running during the timed long-history trials.

## Workload

The retained public index covers heights 3428143 through 3497109, with 68967 blocks,
618014 Ironwood Actions and 20747 indexed payment positions. There are 25051 empty
blocks. The synthetic chain preserves each block's Action count, inserts five test
payments and adds one spending block, yielding **68968 blocks and 618015 Actions**.
The resulting directory has 20752 records in 8192 rows. Its common witness file is
3997558 bytes. Twenty of the 25 wallet receivers have no payment.

The benchmark uses fake compact blocks with real note encryption and wallet code.
Indexed noise notes share one receiver with contiguous directory pages. This
preserves indexed positions and table occupancy but not real receiver frequency or
transaction grouping. Other-pool mainnet history is absent. Each nonempty fixture
block groups its Actions into one transaction. No existing wallet was read.

Each trial creates a fresh file-backed wallet and clients, scans in 1000-block
batches, performs discovery/enhancement, and validates final state. Correct preceding
tree frontiers are supplied from fixture preparation. Fixture construction, cached
tree-state preparation and server startup are outside timing. Post-run witness
checks are outside timing for both modes. Trial order is scan/PIR, PIR/scan, scan/PIR.
Both modes use the same persisted compact history. The production 50-address window
is unchanged.

## HTTP body bytes

Private recovery made 25 receiver queries, with one manifest, parameter and common
witness download. It also made five Enhance queries and its setup downloads:

| Service | Uploaded | Downloaded |
| --- | ---: | ---: |
| receiver | 3,380,500 | 4,142,031 |
| enhance | 1,142,340 | 291,017 |

The baseline has no receiver traffic and the same Enhance body totals. Counts
exclude HTTP/TLS overhead and compact-block/tree-state downloads. Requests use real
encrypted protocols over loopback. Server ingestion is not timed. The fixture helper
seeds a disposable journal while preserving block boundaries for shard sealing.

## Correctness issue found

The initial multi-batch check exposed an interaction between private recovery and
an existing scan optimization. Private mode disabled nullifier pruning, but a large
contiguous batch still skipped inserting older nullifiers. A privately discovered
old note then stayed queued as `AwaitingSpendHistory`. The wallet refused to credit
it rather than treating incomplete spend evidence as absence of a spend.

The library now uses one policy for insertion and pruning. When late discovery is
enabled, every block's spend evidence is retained. Ordinary wallets keep the scan
optimization. Regression coverage checks an early spend and an unspent note, with
opt-in enabled and disabled, over a 201-block batch. All 55 receiving/recovery tests
pass. The corrected 2101-block fixture passes all six comparisons. Vizor's updated
pin passes locked compilation and its three swap policy tests. Earlier missing
history is not fabricated by this change. Existing gaps require rescanning.

A separate fixture bug initially supplied empty tree state at later batch boundaries.
The harness now supplies each batch's actual preceding frontier. Neither failed run
is included in these timings. Their failures were not treated as completed recovery.

## Reproduction

See [harness instructions](../../tools/recovery-benchmark/README.md). Decompress
`public-shape.json.gz` and supply it as the optional shape input. The generated
`compact.bin` can be reused across later runs. `versions.json` records source and
binary hashes, build profiles and input digests. `fixture.json` records expected
notes and compact-chain digest. `results.json` contains all trials and HTTP counts.
`summary.json` contains medians. All six `complete` values require exact note state
and valid witnesses. No production service or installed app was changed.
