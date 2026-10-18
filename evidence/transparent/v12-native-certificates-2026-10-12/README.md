# v12 native correctness screen, 2026-10-12

Correctness reports for the native two-mask m29 profile at both query widths, for
every table a `transparent-shard-v12` shard may name: the recent tier's
2,048-row directory and 8,192-row page shards (`decoupled-2k-8k`), and the
archive tier's 32,768-row directory and page shards (`decoupled-32k-32k`).
Setup seeds are derived from the schema, geometry and table kind, so these are
new parameter sets and need their own screen. **None is a per-snapshot
certificate of a served segment.** They screen shapes; deployment still
certifies each snapshot.

## Method

- **Report tool.** `native_certificate` example in
  `src/transparent/transparent-shard-server/examples/`, synthetic mode with
  `--geometry` and `--table` (the table's own setup and masks), the worst-case
  query term for any u16 column, and a splitmix64 full-range database
  (`random`) or an all-0xffff one (`max`), at `--query-rounding nearest` (49
  bits) and `dithered` (44 bits).
- **Checker.** `certify_native.py` from ipir-sp `d76e61a`, SHA-256
  `9a519715cfdccebdb12766ae7bed6a3b14863664c9f5847ebbc55374095bd6d7`, default
  `--require-bits 128`. Its certificate is conditional on the exported weights
  and idealised sampler draws; it is not a lattice-security estimate.
- **Command.** `run.sh <certify_native.py>`, `release-fast`, on the
  development hub (x86_64, `target-cpu=native`), from branch
  `v12/decoupled-pages` at the commit adding this directory.
  `results.json` has one row per report; `SHA256SUMS` covers the files.

## Results

Certified failure bits (`actual_profile.certified_failure_bits`), random / max fill.
The floor is 128 for every table.

| Table | Rows | 49 nearest | 44 dithered |
|---|---:|---:|---:|
| decoupled-2k-8k directory | 2,048 | 290 / 296 | 284 / 290 |
| decoupled-2k-8k pages | 8,192 | 265 / 268 | 246 / 249 |
| decoupled-32k-32k directory | 32,768 | 170 / 163 | 176 / 172 |
| decoupled-32k-32k pages | 32,768 | 171 / 172 | 177 / 177 |

Every table meets 128 bits at both widths; the lowest is 163 (archive directory,
all-0xffff, 49-bit nearest). The figures sit beside the
[2026-10-09 dithered screen](https://github.com/valargroup/wallet-pir/blob/c00c2101e85fb9518ef8509f4478ed097ce49814/evidence/transparent/dithered-query-2026-10-09/README.md) for the same
row counts under v11 names (archive-32k 171/171 nearest, 177/177 dithered;
recent-8k pages 261/270 and 243/251), as expected: the shapes are the same and
only the seeds moved.
