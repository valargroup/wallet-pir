# Filter and geometry baselines

- [BIP 158 results](bip158/results.json) and [independent mainnet cross-check](bip158/mainnet-crosscheck.json).
- [Mainnet study](mainnet-study/): the 1,152-block input, protobuf/traffic accounting and recorded comparisons. Supporting old layout results are retained with the original checksum manifest.
- [Shard utilisation](shard-utilisation/): full-chain censuses, packing/geometry projections and isolated c-8 measurements referenced by current sizing and source comments.
- [Protobuf schema](compact_formats.proto) and [initial sample](mainnet-3470396-3470523.jsonl.gz): recorded format inputs.

These measurements use their original hardware and source. The current
[filter tools](../../../tools/filters/README.md) retain format-specific utility;
removed retrieval tools can be recovered from the source revision in the
[cleanup ledger](../../../docs/cleanup-2026-09-14.md).

## Measured and projected quantities

| Uniform geometry | Shards | Plaintext, decimal GB | Projected fleet RSS from c-8 per-shard measurements |
|---|---:|---:|---:|
| recent-8k | 1091 | 64.1 | about 287.1 GiB |
| archive-32k | 314 | 73.8 | about 141.5 GiB |
| archive-wide | 162 | 57.1 | about 93.2 GiB |

The RSS column is multiplication of measured isolated-runtime costs, not an observed all-resident fleet under load. In the c-8 report, directory/page RSS is 134.72/134.72 MiB for recent-8k and 230.69/358.75 MiB for archive-wide. Archive-wide build time sums to 14.07 seconds per cold shard. Reservations are 256 and 576 MiB respectively; budgets must also cover process/allocator/HTTP/build/revision overhead.

The old full-chain report's 273/137/91.1 GiB figures use reservation arithmetic; later decision prose rounds or projects differently. Use labeled arithmetic rather than mixing these values. A 5% runtime measurement correction is not a safe total process-headroom policy.

The reported p50 values 0.26 MB (recent-8k) and 0.52 MB (archive-wide) follow two directory query uploads for a median chain script. They are not a complete wallet sync bill. At archive-wide, fewer shard matches improve reported p99 query-upload cost, but maximum tails can worsen. Include filters, setup, responses, transport and multiple owned/unused scripts in real wallet accounting.

The superseded full-chain narrative incorrectly ruled out narrow directories under wider pages before correcting itself. Such shapes are legal when script-based sealing closes earlier; the additional boundaries must be measured. Its claim that wide-profile residency/scaling had never run was superseded by the linked c-8 measurements. Old narratives have been deleted; raw runs remain and version control retains the former prose.
