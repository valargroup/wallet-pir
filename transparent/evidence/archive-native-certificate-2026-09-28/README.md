# Native correctness on real archive tables, 2026-09-28

Per-segment correctness certificates for native ReinspiRING (two-mask m29,
schema `transparent-shard-v8`) on **real** `archive-wide` tables built from the
mainnet journal. This follows the [shape screen](../native-certificate-2026-09-28/README.md),
where the data-independent bound for 65,536-row tables was 83 bits against a
128-bit policy.

| Field | Value |
|---|---|
| Source | `main` at `5dfa0a5b`+ (v8), `shard-publish` and the `native_certificate` example, built on `roman-ipir-bench-8vcpu` |
| Checker | `reinspiring/tools/security/certify_native.py`, ipir-sp `v0.1.0-rc.6` (1f2aec6), default 128-bit requirement |
| Input | Three journal slices streamed read-only from the coordinator: archive shards 84 (median events, heights 388,103–402,087), 94 (most scripts, 578,596–596,441) and 157 (most events, 2,997,091–3,133,539), from the [September census](../census-2026-09-08/README.md) |
| Publication | Each slice published as a v8 `archive-wide` set (32,768 directory rows, 65,536 page rows), range profile v2, directory choice on (`*.publish.log`). Page demand under 4,096-byte rows is 55,804–56,742 rows, one segment each |
| Command | `run.sh`: `native_certificate segment --geometry archive-wide --table {pages,directory} --rows-bin <segment>`, then `certify_native.py`; column statistics with numpy |

## Results

Certified failure bits (measured query term; 128 required):

| Shard | Pages (65,536 rows) | Deterministic error | Directory (32,768 rows) |
|---|---:|---:|---:|
| 84, median | 159 | 41.0e9 | 211 |
| 94, spam era | **149** | 44.9e9 | 199 |
| 157, densest | 170 | 37.3e9 | 197 |

The decryption radius is 2^37 = 137.4e9. The data-independent worst case at
65,536 rows is 71.1e9 of deterministic error, or 83 bits.

## Why the worst case does not apply, and what sets the real margin

The failing term is the 49-bit query rounding (±16 per query coefficient)
multiplied by the stored 16-bit values, summed down one column. Its bound
assumes a column of `0xFFFF` in every row.

In real page tables:
- the heaviest columns reach 51–62% of that maximum (`column-statistics.jsonl`);
- the average column reaches 18–22%;
- 42–50% of the 16-bit values are non-zero.

In shard 94 the heaviest columns sit at byte 72 and every 96 bytes after it.
That is the low 16 bits of each event's block height (entry header ends at
byte 68, `height` is at event offset 4). They average about 46,800 in the 87%
of occupied rows, because the shard's heights run from 0xD424 upwards in
their low half.

So the margin follows chain structure, not attacker-chosen bytes. A shard whose
heights' low 16 bits cluster near 0xFFFF, with full pages, would be the least
favourable real case, estimated at 100–110 bits.

`archive-wide` covers only history before the recent cutoff. That history is
sealed and fixed, and ageing never re-cuts shards into it. Each segment can
therefore be certified once, exactly, when the v8 set is published.

## Decision (2026-09-28)

Keep `archive-wide`. Accept the 65,536-row page tables under a documented
exception to the 128-bit data-independent policy:
- the absolute floor is 83 bits per query for any data;
- every published archive page segment is certified with its measured query
  term during the v8 publication, and the result is recorded;
- a segment below 128 bits is reported explicitly, not silently accepted.

Directory tables (32,768 rows) and all recent tables meet 128 bits even on the
data-independent bound.

## Limits

- Three of about 160 archive shards were measured. The v8 publication must
  certify all of them.
- The certificate is analytic, conditional on the exported weights and
  idealised sampler draws; it does not count decryption failures.
- Storing heights relative to the shard's start would cap the heaviest column
  further. That is an optional layout change, not made here.
