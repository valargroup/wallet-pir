# Dithered 44-bit query screen for Transparent tables, 2026-10-09

Correctness reports for the native two-mask m29 profile at both query widths
the servers now accept. These are 49 bits rounded to nearest (today's query)
and 44 bits with dithered rounding (`reinspiring-two-mask-m29-dq44-v1`). The
reports cover every table a published shard may name, at schema
`transparent-shard-v11`. **None is a per-snapshot certificate of a served
segment.** They screen shapes; deployment still certifies each snapshot.

## Method

- **What dithering changes.** The client rounds each selection coefficient up
  with probability equal to the fraction it drops, using fresh coins. The
  rounding errors are then independent and zero mean. The checker budgets them
  as a variance term from each block's `query_l2_squared`, the largest
  per-column sum of squared entries. Nearest rounding at 49 bits instead
  reserves its worst case, 16 times the column L1. The setup, masks and
  packing weights are the same at both widths.
- **Report tool.** The `native_certificate` example in
  `transparent/services/transparent-shard-server/examples/` has two additions:
  - `--query-rounding dithered` emits `native-noise-two-mask-rounded-dithered-v1`
    with `query_rounding`, `query_bits: 44` and `query_l2_squared`.
  - `synthetic --geometry <g> --table <t>` uses that table's own setup and masks.

  Nearest output is byte-for-byte what the tool produced before. That was
  checked against `2d1b9abc` on two synthetic shapes and on a segment with
  measured and with worst-case query terms. The dithered report of the same
  input differs only in `format`, `query_bits`, `query_rounding` and
  `query_l2_squared`.
- **Checker.** `certify_native.py` from ipir-sp `d76e61a` (PR #30), SHA-256
  `9a519715cfdccebdb12766ae7bed6a3b14863664c9f5847ebbc55374095bd6d7`, run with
  the default `--require-bits 128`. Its certificate is conditional on the
  exported weights and idealised sampler draws. It is not a lattice-security
  estimate.
- **Inputs.**
  - Synthetic: each table at its own setup, filled either with a splitmix64
    full-range database (`random`) or with all 0xffff (`max`), using the
    worst-case query term for any u16 column of that height.
  - Segment: the same `random.Random(1)` archive-wide page table as the
    [2026-09-28 screen](../native-certificate-2026-09-28/README.md), with the
    query term measured from its columns.
- **Command.** `run.sh <certify_native.py>`, `release-fast` profile, on an
  Apple M4 Max with 128 GiB under macOS. It took 114 s, build included.
  `manifest.json` has the provenance and `results.json` one row per report.
  `SHA256SUMS` covers the retained files.

## Results

Certified failure bits (`actual_profile.certified_failure_bits`). Floors:
83 for archive-wide pages, 128 for every other table.

| Table | Rows | Fill | 49 nearest | 44 dithered |
|---|---:|---|---:|---:|
| txid-2k txdirectory | 2,048 | random / max | 291 / 293 | 285 / 286 |
| txid-4k txdirectory | 4,096 | random / max | 282 / 287 | 270 / 275 |
| recent-4k directory | 4,096 | random / max | 287 / 278 | 275 / 267 |
| recent-4k pages | 4,096 | random / max | 286 / 287 | 274 / 275 |
| recent-4k-8k directory | 4,096 | random / max | 281 / 277 | 270 / 266 |
| recent-4k-8k pages | 8,192 | random / max | 264 / 267 | 246 / 248 |
| recent-8k directory | 8,192 | random / max | 264 / 272 | 246 / 252 |
| recent-8k pages | 8,192 | random / max | 261 / 270 | 243 / 251 |
| archive-32k directory | 32,768 | random / max | 171 / 171 | 177 / 177 |
| archive-32k pages | 32,768 | random / max | 167 / 171 | 175 / 177 |
| archive-wide directory | 32,768 | random / max | 170 / 168 | 177 / 175 |
| archive-wide pages | 65,536 | random / max | **85 / 85** | **113 / 112** |
| archive-wide pages, measured (segment) | 65,536 | seed 1 | 165 | 199 |

Every table meets its floor at 44 dithered bits. Each dithered certificate
also screens 49-bit nearest on the same weights. That screen equals the
separate nearest report's result in all 25 pairs.

Bytes per query fall by `rows * 5 / 8`: 1,280 B at 2,048 rows, 5,120 B at
8,192, 20,480 B at 32,768 and 40,960 B at 65,536. The 27,648-byte key and the
response are unchanged.

## Reading the results

- **Large tables gain margin.** On 65,536-row archive-wide pages the
  data-independent bound rises from 85 to 112 bits. It is still under 128, so
  the documented 83-bit exception for that table stays in force. At 32,768
  rows the bound rises by 6–8 bits.
- **Small tables lose a little.** At up to 8,192 rows each coefficient's
  rounding error grows from at most 2^4 to under 2^10. That outweighs the
  cancellation the variance term credits, so these tables lose 6–20 bits.
  They stay at or above 243.
- **Packing weights hardly move.** They change little with fill or table, as
  in the earlier screen. The query term decides the margin at large tables.

## Limits and deployment

- The servers accept both widths from one snapshot. Wallets and txid clients
  built from this source switch to 44 bits as soon as a service advertises the
  dithered schemes. Deploying these servers therefore needs a per-snapshot
  certificate at **both** widths for every served segment: the 49-bit nearest
  one as today, and a 44-bit dithered one
  (`native_certificate segment ... --query-rounding dithered`).
- The deployed `native-certificates` gate pins the earlier checker. It must
  move to the `d76e61a` script above, whose SHA-256 is recorded here, because
  older checkers refuse the dithered format. This is
  [remaining work](../../docs/remaining-work.md); nothing here changes the
  candidate pins.
- Synthetic packing weights are representative, not bounds over every
  database. Nothing here counts decryption failures.
