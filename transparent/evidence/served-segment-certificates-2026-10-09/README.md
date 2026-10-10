# Served-segment certificates at both query widths, 2026-10-09

Every table segment served at 21:54 UTC by the v11 history map and the txid display
v2 map was certified at 49-bit nearest and 44-bit dithered query rounding before
`06a972db` (which advertises the dithered schemes) was deployed to either service.
This closes the per-snapshot gate in
[remaining work](../../docs/remaining-work.md#dithered-44-bit-queries-deployed-2026-10-09);
the [shape screen](../dithered-query-2026-10-09/README.md) only covered synthetic tables.

All 608 segments meet their floors at both widths. [results.json](results.json) has one
row per segment; `summarize.py` fails if any segment lacks a width, a report binds
other bytes than the manifest's segment hash, a width is wrong, a floor is missed,
the dithered certificate's 49-bit nearest screen disagrees with the separate nearest
certificate, or a table has more than one setup.

| Product | Geometry / table | Segments | Floor | Lowest 49 nearest | Lowest 44 dithered |
|---|---|---:|---:|---:|---:|
| history | archive-wide directory (32,768 rows) | 82 | 128 | 172 | 183 |
| history | archive-wide pages (65,536 rows) | 82 | 83 | 95 | 120 |
| history | recent-4k-8k directory | 9 | 128 | 285 | 280 |
| history | recent-4k-8k pages | 9 | 128 | 273 | 265 |
| txid | txid-2k txdirectory | 426 | 128 | 284 | 280 |

On the tightest table, archive-wide pages, dithering raises every segment's bound: the
five lowest are 95–105 bits at 49 nearest and 120–133 at 44 dithered. The checker's
default `--require-bits 128` makes it exit nonzero for 14 of these reports (12 nearest,
2 dithered, all 95–127 bits); [certify-failures.txt](certify-failures.txt) lists them.
The floor for this table is 83, which `summarize.py` applies.

## What ran

- Snapshot: history `active.json` at height 3,512,212 (`inputs/history-active.json`,
  91 shards), txid display tip 3,512,212, cycle 1,528 (`inputs/txid-active.json`,
  426 shards). The history `map_sha256` (`9ec3faeb…`) is not the SHA-256 of
  `shards.json` (`64f892c1…`); for txid display they are equal. Both are recorded.
- [run.sh](run.sh) on `roman-ipir-bench-8vcpu` (8 vCPU Xeon Platinum 8358, 31 GiB,
  Ubuntu 24.04, ams3) streamed each segment from the coordinator under
  `ionice -c3 nice -n19`, checked the manifest digest and the segment's SHA-256, then ran
  `native_certificate segment --query-rounding nearest|dithered`. 21:54:48–22:58:55 UTC.
  The unsealed tips went first; [native.log](native.log) has per-report time and RSS
  (at most 7.8 s).
- History shard 89 (sealed, revision 901, `5a6a8eac…`) failed to fetch in that run
  ([failures.tsv](failures.tsv)): its candidate directory had been pruned before the
  sealed pass. [rerun-89.sh](rerun-89.sh) fetched the same manifest digest from the
  then-active publication (height 3,512,274, `inputs/history-active-rerun-89.json`) and
  certified both of its segments.
- Tool: `native_certificate` example of `transparent-shard-server` at `06a972db`,
  release-fast, Rust 1.97.1, `target-cpu=native`, SHA-256 `e20e5eb6…`
  ([manifest](manifest.json)). Checker: ipir-sp `d76e61a`
  `reinspiring/tools/security/certify_native.py` (`9a519715…`) with
  `native_gaussian_cdf.txt` (`4c6e3f51…`).
- Reports and certificates (2,432 files) are in `reports.tar.xz`.

## Limits

- One snapshot. The unsealed tips (history shard 90, txid shard 425) change every
  block, and nothing certifies later revisions as they are published. Their certified
  revisions have 290–304 bits; the shape screen covers later revisions only by shape.
- Reports were run without `--public-sha256`, so they bind each certificate to the
  segment bytes, not to the masks a server publishes for them.
- The `native-certificates` gate of the schema-candidate path still pins the older
  checker and runs nearest only; that is a change for the next schema candidate.
