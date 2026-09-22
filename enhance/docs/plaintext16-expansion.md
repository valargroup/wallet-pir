# 16-bit plaintext expansion

Schema 9 uses all 16 bits in each existing `u16` database coefficient. The
profile is named `simplepir-p16-q46-v1` and is bound into the public generation,
derived parameter ID and persisted artifact metadata. Clients regenerate the
profile parameters and compare the complete parameter set; a 14-bit generation
or a 45-bit query profile is rejected.

## Exact benefit for 737-byte records

Six instances contain `6 × 2,048 × 16 = 196,608` plaintext bits. Thirty-three
737-byte records use 194,568 bits, leaving 2,040 bits (255 bytes), for 98.96%
payload utilization. Thirty-four records need 200,464 bits and therefore a
seventh instance.

| Property | Schema 8 | Schema 9 | Change |
|---|---:|---:|---:|
| Plaintext profile | 14-bit / 41-bit query | 16-bit / 46-bit query | versioned migration |
| Records per row | 29 | 33 | +4, or +13.793% |
| Row bytes | 21,373 | 24,321 | +2,948 |
| Records per 8,192-row shard | 237,568 | 270,336 | +32,768 |
| Records per three-shard group | 712,704 | 811,008 | +98,304 |
| Database columns / instances | 12,288 / 6 | 12,288 / 6 | unchanged |
| Encoded database allocation | 192 MiB | 192 MiB | unchanged |
| Response coefficient payload | 30 KiB | 30 KiB | unchanged |
| Query coefficient payload at 8,192 rows | 41 KiB | 46 KiB | +5 KiB |

The win is capacity, not a smaller fixed-shape allocation. At any record count
that needs the same number of shards, worker database and CRS allocations remain
the same. Memory falls by one complete shard only when the denser rows cross a
shard-count boundary. For example, 270,336 records require two schema-8 shards
and exactly one schema-9 shard.

Dense 14-bit storage would reduce the encoded database from 192 to 168 MiB but
would keep 29 records per row. Benchmarks found that unpacking slowed the full
server path by 91.9% on an M4 Max and 8.2% on an AVX-512 Xeon. The 16-bit `u16`
scan retained essentially the existing server time, so schema 9 chooses capacity
and predictable SIMD access over the 24 MiB database-allocation saving.

## Why the query is 46 bits

Changing `p` does not weaken RLWE query privacy by itself; it reduces the
correctness noise margin. The formula-derived 45-bit query passed empirical
fixtures, but the conservative sufficient certificate exhausted its
deterministic allowance on an all-maximum 8,192-row fixture. One extra query bit
made the allowance positive. Across six fixed public schedules, the weakest
ideal independent-sampler union bound was `2^-143` per query. The implemented
system claim additionally depends on the ChaCha20 PRG replacement advantage and
ordinary OS randomness assumptions.

That result is scoped to the evaluated public databases and setup schedules. It
is not a universal bound over arbitrary snapshots, larger dimensions, other
samplers or changed response precision. Production activation therefore
requires a certificate bound to the actual snapshot and setup, plus independent
review of the adapted bound. Empirical trials support feasibility: 768 fresh
16-bit/46-bit six-instance queries across maximum, mixed and pseudorandom
fixtures had zero failures, with the largest true error at 9.04% of the decoding
threshold. Trial success alone does not establish a rare-failure probability.

## Migration contract

The transition increments the schema, protocol revision and artifact version.
It also binds the profile string into public and persisted metadata. Deploy the
schema-8 and schema-9 services in parallel with separate storage roots and
origins during wallet adoption. This preserves rollback and prevents one binary
from guessing a profile from an untrusted query. Rebuild every schema-9 worker
artifact; a schema-8 artifact cannot be reused even though both databases store
`u16` coefficients and have six instances.
